# Databricks notebook source
# RL TRAIN (healthcare, REAL-JOURNEYS-ONLY) -- the offline-RL training as ONE serverless process (pure-numpy CQL, torch-
# free). The offline SIMULATOR is RETIRED: we train only on REAL reconstructed journeys (nba_ml_build_journey_set ->
# journey_transitions), i.e. the hand-crafted-playbook history the live pipeline recorded. BC = RECREATE the playbook;
# CQL = recreate + OPTIMIZE for milestone value. Exports the numpy Q-net keyed to the LIVE catalog arms (action x channel)
# to the UC volume the scorer auto-reloads, + the rl_card. This is "learn the marketer's playbook, then beat it."

# COMMAND ----------

# MAGIC %run ./nba_journey_env

# COMMAND ----------

# MAGIC %run ./nba_ml_cql_numpy

# COMMAND ----------

# MAGIC %run ./nba_ml_catalog

# COMMAND ----------

import json, numpy as np, pandas as pd
dbutils.widgets.text("ml_catalog", "ml_worspace"); dbutils.widgets.text("ml_schema", "core")
dbutils.widgets.text("cql_steps", "40000"); dbutils.widgets.text("alpha", "0.2"); dbutils.widgets.text("out_name", "rl_qnet.json")
dbutils.widgets.text("max_rows", "600000")   # driver-memory cap on real transitions (full set OOMs the serverless driver)
ML_CAT = dbutils.widgets.get("ml_catalog"); ML_SCH = dbutils.widgets.get("ml_schema")
CQL_STEPS = int(dbutils.widgets.get("cql_steps")); ALPHA = float(dbutils.widgets.get("alpha"))
spark.sql(f"CREATE VOLUME IF NOT EXISTS {ML_CAT}.{ML_SCH}.ckpt"); VOL = f"/Volumes/{ML_CAT}/{ML_SCH}/ckpt"

# the LIVE catalog (dim_definitions) -> arms; ACTION_DIM = #(action,channel) + NOOP (computed at runtime, no import-timing)
ARMS_HC = load_catalog(spark); ACTION_DIM = len(ARMS_HC) + 1; NOOP = len(ARMS_HC)
print(f"catalog: {len(ARMS_HC)} (action,channel) arms + NOOP; STATE_DIM={STATE_DIM}, ACTION_DIM={ACTION_DIM}")

# REAL journeys -- the hand-crafted-playbook history is the ONLY training data (sim retired)
jt = spark.table(f"{ML_CAT}.{ML_SCH}.journey_transitions"); n_real = jt.count()
if not n_real:
    dbutils.notebook.exit(json.dumps({"status": "no journeys yet -- let the playbook accrue, then retrain"}))
# DRIVER-MEMORY CAP: toPandas() over the FULL set (1.17M) + per-row materialization OOMs the serverless driver. A
# representative random sample preserves the journey + decision-fact distribution for offline RL; seed is fixed so the
# train is reproducible. (OOM fix 2026-06-19)
MAX_ROWS = int(dbutils.widgets.get("max_rows") or "0")
if MAX_ROWS and n_real > MAX_ROWS:
    jt = jt.sample(False, min(1.0, float(MAX_ROWS) / n_real), seed=42)
    print(f"sampled {n_real} -> ~{MAX_ROWS} transitions (driver-memory cap)")
rp = jt.toPandas()
# VECTORIZED stack (NOT a 1.17M-row Python list comprehension, which balloons driver memory + wall time -> OOM)
O = np.stack(rp["obs"].to_numpy()).astype(np.float32); S2 = np.stack(rp["next_obs"].to_numpy()).astype(np.float32)
A = rp["action"].to_numpy(np.int64); R = rp["reward"].to_numpy(np.float32); D = rp["done"].to_numpy(np.float32)
A = np.clip(A, 0, NOOP)
print(f"real transitions: {len(O)}; action hist[:10]={np.bincount(A, minlength=ACTION_DIM)[:10].tolist()} ... NOOP={int((A==NOOP).sum())}")

# REWARD SHAPING (potential-based, optimality-preserving): potential = milestone progress already in the obs (the
# milestone block, weighted by milestone value) -> a dense gradient toward STARS between sparse fact-flips. Scale by 0.1.
_MS0 = NF + NDISP_TOTAL
def _phi(OB): return (OB[:, _MS0:_MS0 + len(MS_VALUES)] * MS_VALUES).sum(1) * 0.5
R = (R + 0.97 * _phi(S2) - _phi(O)) * 0.1
# OBS NORMALIZATION (THE critical fix -- without it the net can't see the decisive 0/1 fact flags). Stats ride in the
# qnet so the live scorer normalizes IDENTICALLY.
OBS_MEAN = O.mean(0); OBS_STD = O.std(0) + 1e-6
On = (O - OBS_MEAN) / OBS_STD; S2n = (S2 - OBS_MEAN) / OBS_STD

bc = train_bc(On, A, STATE_DIM, ACTION_DIM)                                            # RECREATE: clone the playbook
Q = train_cql(On, A, R, S2n, D, STATE_DIM, ACTION_DIM, steps=CQL_STEPS, batch=512, gamma=0.97, alpha=ALPHA, lr=1e-4)  # + OPTIMIZE

# off-policy eval on the REAL data: recreate = agreement with the logged (playbook) action; STARS reach = the goal signal
_STARS = NF + NDISP_TOTAL + MILESTONE_IDS.index("stars")
bc_match = float((greedy(bc, On) == A).mean()); cql_match = float((greedy(Q, On) == A).mean())
stars = float((S2[:, _STARS] >= 1).mean())
print(f"RECREATE: BC matches playbook {bc_match:.2f}, CQL matches {cql_match:.2f}  |  journeys at STARS {stars:.2f}")

# OPTIMIZE eval -- does the policy ROUTE to the channel the member ENGAGED on? Build a baseline state, set exactly ONE
# channel's positive-engagement disposition, and check the greedy arm's channel. A model that LEARNED per-member channel
# affinity (from the dispositions in its state) reroutes to the engaged channel; the fixed-channel playbook cannot.
_ENGAGE = {"email": "link_clicked", "sms": "link_clicked", "push": "opened", "voice": "answered"}
_route = {}; _hits = 0; _tot = 0
for _ch in ["email", "sms", "push", "voice"]:
    if (_ch, _ENGAGE[_ch]) not in DISP_AT: continue
    _s = np.zeros((1, STATE_DIM), np.float32)
    _s[0, NF + DISP_AT[(_ch, _ENGAGE[_ch])]] = 1.0                 # member engaged on THIS channel
    _sn = (_s - OBS_MEAN) / OBS_STD if OBS_MEAN is not None else _s
    _a = int(greedy(Q, _sn)[0])
    _chosen = ARMS_HC[_a]["channel"] if _a < len(ARMS_HC) else "hold"
    _route[_ch] = _chosen; _tot += 1; _hits += int(_chosen == _ch)
route_acc = _hits / max(_tot, 1)
print(f"OPTIMIZE: engaged-channel -> policy-chosen-channel {_route}  |  routes to engaged channel {_hits}/{_tot} ({route_acc:.2f})")

# FULL PERSONALIZATION eval -- distinct member PROFILES should yield distinct preferred ACTIONS (not just channels). Build
# assessed-stage states with different clinical/engagement profiles and read the policy's preferred action (max-Q over
# that action's channels). This is the headline proof: the model picks the action that fits WHO THE MEMBER IS.
_FC = {c: i for i, c in enumerate(FEATURE_COLS)}
def _best_action(facts):
    s = np.zeros((1, STATE_DIM), np.float32)
    # CLINICAL DECISION POINT: the member has climbed to PCP-selected (engaged), where the clinical actions
    # (med/care/a1c/wellness/mammogram/colonoscopy/awv) are the relevant choices. Set the full funnel-up-to-pcp +
    # reached/registered/assessed/engaged milestones so the eval reads the personalized CLINICAL pick, not the early
    # funnel. (A non-pcp state makes every clinical action ineligible -> the model trivially picks portal/pcp.) (2026-06-19)
    for _f in ["respondedToOutreach", "registeredForPortal", "loggedIn", "viewedBenefits", "hraCompleted", "pcpSelected"]:
        if _f in _FC: s[0, _FC[_f]] = 1.0
    for _m in range(4): s[0, NF + NDISP_TOTAL + _m] = 1.0                        # reached, registered, assessed, engaged
    for k, v in facts.items():
        if k in _FC: s[0, _FC[k]] = v
    sn = (s - OBS_MEAN) / OBS_STD if OBS_MEAN is not None else s
    qv = _forward(Q, sn)[0][0]
    best = {}
    for i, a in enumerate(ARMS_HC):
        if a["actionId"] not in best or qv[i] > best[a["actionId"]]: best[a["actionId"]] = float(qv[i])
    return max(best, key=best.get)
_profiles = {
    "diabetic_low_adherence":  {"diabetic": 1.0, "rxAdherencePDC": 0.4, "riskScore": 0.5},
    "high_risk_complex_sdoh":  {"riskScore": 0.9, "comorbidityCount": 4.0, "erVisits12mo": 4.0, "sdohBarrier": 1.0},
    "low_risk_young_digital":  {"riskScore": 0.15, "age": 55.0, "portalLogins30d": 20.0, "pagesViewed30d": 60.0},
    "lapsed_member":           {"daysSinceLogin": 55.0, "riskScore": 0.4},
}
_pers = {name: _best_action(f) for name, f in _profiles.items()}
print("FULL PERSONALIZATION (profile -> preferred action):")
for name, act in _pers.items(): print(f"  {name:24s} -> {act}")

qnet = {"layers": qnet_layers(Q), "obs_mean": OBS_MEAN.tolist(), "obs_std": OBS_STD.tolist(),
        "feature_cols": FEATURE_COLS, "state_dim": STATE_DIM, "action_dim": ACTION_DIM, "max_steps": MAX_STEPS,
        "arms": [{"actionId": a["actionId"], "channel": a["channel"]} for a in ARMS_HC],
        "noop_index": NOOP, "milestones": [{"id": m} for m in MILESTONE_IDS]}
with open(f"{VOL}/{dbutils.widgets.get('out_name')}", "w") as fh: json.dump(qnet, fh)
card = {"n_real_transitions": int(len(O)), "bc_playbook_match": bc_match, "cql_playbook_match": cql_match,
        "stars_reach": stars, "n_arms": len(ARMS_HC),
        "milestones": [{"id": m["id"], "name": m["name"], "value": m["value"]} for m in MILESTONES],
        "engine": "pure-numpy CQL on REAL hand-crafted journeys", "alpha": ALPHA}
spark.createDataFrame([(json.dumps(card),)], "card string").write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ML_CAT}.{ML_SCH}.rl_card")
print(f"exported {dbutils.widgets.get('out_name')} ({len(ARMS_HC)} arms) -> {VOL} + rl_card")
dbutils.notebook.exit(json.dumps({"n_real": int(len(O)), "bc_match": bc_match, "cql_match": cql_match,
                                  "stars_reach": stars, "route_to_engaged": route_acc, "routing": _route,
                                  "personalization": _pers}))
