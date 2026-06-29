# Databricks notebook source
# BUILD JOURNEY SET — reconstruct REAL member journeys from the lake into offline-RL transitions for the CQL.
#   sends     = silver_activations (op=DISPATCH)              -> the arm sent, by member, in time order
#   features  = silver_snapshots.factsJson (point-in-time)    -> the state AT each send (no leakage)
#   final     = gold_member_snapshot.factsJson               -> the member's current features (terminal next-state)
#   reward    = milestone value gained between consecutive sends - SEND_COST   (computed from the feature transition)
# Output: {ml_ns}.journey_transitions (obs, action, reward, next_obs, done, source) — the real reconstructed journeys
# UNION'd with single-step transitions from the DYNAMIC sim (sim_examples), so the CQL gets all-channel coverage (sms +
# mail) instead of going cold on under-sent channels. The obs layout is the env's (_obs_batch/_milestones_batch), so real
# and sim rows are byte-identical. Degrades gracefully: no real sends -> sim-only; no sim_examples -> real-only. The
# real-journey reconstruction is PROVEN exact (see nba_ml_journey_reconstruct_local.py).

# COMMAND ----------

# MAGIC %run ./nba_ml_common

# COMMAND ----------

# MAGIC %run ./nba_journey_env

# COMMAND ----------

# MAGIC %run ./nba_ml_journey

# COMMAND ----------

# MAGIC %run ./nba_ml_catalog

# COMMAND ----------

import json, numpy as np, pandas as pd, time as _time
ML_NS, SRC_NS = ml_widgets()
dbutils.widgets.text("window_hours", "0")   # 0 = reconstruct ALL history; >0 = only DISPATCHes in the last N hours.
WINDOW_HOURS = float(dbutils.widgets.get("window_hours") or "0")
dbutils.widgets.text("hold_mult", "1.0")    # cap HOLD (NOOP) rows to this multiple of real sends (0 = keep all)
# RECENCY SCOPE: the lake holds ~1.8M historical (happy-path) sends; once the activation layer runs in REALISTIC mode the
# new journeys carry real-shaped outcomes (partial funnels, opt-outs, sparse converts). Grounding the policy on the union
# of both would relearn the happy-path fiction. window_hours scopes reconstruction to the realistic-mode window so the
# flywheel trains on the REAL outcomes the live system actually produced — not stale optimism. eventTs is epoch MILLIS
# (same convention as nba_ml_build_training_set), so compare against a millis cutoff — NOT a TIMESTAMP interval.
_CUTOFF = int(_time.time() * 1000) - int(WINDOW_HOURS * 3600000) if WINDOW_HOURS > 0 else None
_W = f" AND eventTs >= {_CUTOFF}" if _CUTOFF else ""
if WINDOW_HOURS > 0: print(f"recency scope: last {WINDOW_HOURS}h of sends (eventTs >= {_CUTOFF}) — realistic-mode window")

# PER-ACTION arms from the live catalog (dim_definitions). Computed at runtime -> {(actionId, channel): arm}; NOOP = n.
ARMS_HC = load_catalog(spark)
ARM_IDX = arm_index_map(ARMS_HC); NOOP_HC = len(ARMS_HC)
print(f"catalog arms: {NOOP_HC} (action,channel) + NOOP")

# SEND steps: a CREATE (the router's chosen action for the member -- the lifecycle records CREATE->SOFT_COMPLETE, not a
# separate DISPATCH op) + the point-in-time snapshot for its correlationId. Carries actionId so we key the REAL arm.
dec = (spark.table(f"{SRC_NS}.silver_activations").where("op = 'CREATE'" + _W)
       .select("entityId", "actionId", "channel", "correlationId", F.col("eventTs").alias("decisionTs")))
snap = (spark.table(f"{SRC_NS}.silver_snapshots")
        .select("entityId", "correlationId", F.col("updatedTs").alias("snapTs"), parse_features_udf("factsJson").alias("f")))
send = (dec.join(snap.select("correlationId", "f"), "correlationId", "left")
        .select("entityId", "decisionTs", "actionId", "channel", *[F.col(f"f.{c}").alias(c) for c in FEATURE_COLS]))

# HOLD steps (PACING): evaluations that had eligible actions but whose correlationId NEVER dispatched = the system
# held (chose not to send). These are the real no-op decision points the model needs to learn spacing. The snapshot
# carries the state at the hold; reconstruct maps channel='__hold' -> NOOP (no send cost).
dispatched = dec.select("correlationId").distinct()
hold_corr = (spark.table(f"{SRC_NS}.silver_eval_eligible").select("correlationId").distinct()
             .join(dispatched, "correlationId", "left_anti"))
hold = (snap.join(hold_corr, "correlationId")
        .select("entityId", F.col("snapTs").alias("decisionTs"), F.lit("").alias("actionId"), F.lit(HOLD_CHANNEL).alias("channel"),
                *[F.col(f"f.{c}").alias(c) for c in FEATURE_COLS]))
if _CUTOFF:                               # scope holds to the same realistic-mode window as the sends (snapTs is epoch millis)
    hold = hold.where(f"decisionTs >= {_CUTOFF}")

# BALANCE: the throttle turns ~85% of decision points into HOLDs; training on that imbalance collapses the policy to
# always-NOOP (route_to_engaged 0.0, profiles all pick the same early action). Cap holds to HOLD_MULT x the real sends
# so the policy learns from the actual action outcomes while keeping pacing signal. (2026-06-19)
HOLD_MULT = float(dbutils.widgets.get("hold_mult") or "0")
if HOLD_MULT > 0:
    _ns = send.count(); _nh = hold.count(); _cap = int(HOLD_MULT * _ns)
    if _nh > _cap and _cap > 0:
        hold = hold.sample(False, min(1.0, _cap / _nh), seed=42)
        print(f"balanced holds: {_nh} -> ~{_cap} ({HOLD_MULT}x {_ns} sends)")

allrows = send.unionByName(hold).where("daysSinceLogin IS NOT NULL AND entityId IS NOT NULL")
pdf = allrows.toPandas()
n_hold = int((pdf["channel"] == HOLD_CHANNEL).sum()) if len(pdf) else 0
print(f"decision points: {len(pdf)} ({len(pdf) - n_hold} sends + {n_hold} holds) across {pdf['entityId'].nunique() if len(pdf) else 0} members")

# POINT-IN-TIME DISPOSITION STATE: for each decision point, set each channel's disposition to the LATEST nba.disposition
# on that channel STRICTLY BEFORE the decision time (no leakage), mapped raw-status -> env disposition via RAW_TO_DISP.
# This is the real-data analogue of the simulator's per-channel disposition — so a reconstructed obs carries the same
# action-state the scorer serves and the env trains on. merge_asof(by=entityId, backward, no exact match) = the as-of join.
for _dc in DISP_COLS: pdf[_dc] = 0.0
if len(pdf):
    try:
        disp_hist = (spark.table(f"{SRC_NS}.silver_fact_history").where("factClass = 'disposition'")
                     .select("entityId", "channel", F.col("value").alias("raw"), F.col("eventTs").alias("dTs"))).toPandas()
        if len(disp_hist):
            base = pdf[["entityId", "decisionTs"]].reset_index().rename(columns={"index": "_row"}).sort_values("decisionTs")
            for _ch in CH_DISPS:
                h = disp_hist[disp_hist["channel"].astype(str) == _ch].copy()
                if not len(h): continue
                h["disp"] = h["raw"].astype(str).map(RAW_TO_DISP.get(_ch, {}))
                h = h.dropna(subset=["disp"]).rename(columns={"dTs": "decisionTs"}).sort_values("decisionTs")
                if not len(h): continue
                m = pd.merge_asof(base, h[["entityId", "decisionTs", "disp"]], on="decisionTs", by="entityId",
                                  direction="backward", allow_exact_matches=False).dropna(subset=["disp"])
                for _disp in m["disp"].unique():
                    pdf.loc[m.loc[m["disp"] == _disp, "_row"].values, f"{_disp}_{_ch}"] = 1.0
            print(f"joined point-in-time dispositions: {int((pdf[DISP_COLS].to_numpy() >= 1).sum())} disposition flags set")
    except Exception as e:
        print(f"  disposition join skipped ({type(e).__name__}: {e}) -> dispositions default to 0")

# DECISION FACTS (member attributes: profile/claims/engagement) live in GOLD, not the lean point-in-time snapshot (which
# carries only rule/progress facts). Overlay each member's CURRENT gold value onto every decision point, so the model
# trains on the SAME profile the scorer reads from gold at serve time -- without this the model never sees them (=0).
DECISION_COLS = ["riskScore", "diabetic", "age", "planDSNP", "tenureMonths", "sdohBarrier", "comorbidityCount",
                 "erVisits12mo", "rxAdherencePDC", "openCareGaps", "portalLogins30d", "pagesViewed30d",
                 "avgTimeOnPageSec", "benefitsPageViews"]
if len(pdf):
    try:
        gw = (spark.table(f"{SRC_NS}.gold_member_snapshot").where("key LIKE 'operator.%'")
              .groupBy("entityId").pivot("key").agg(F.first("value")).toPandas())
        gmap = {}
        for _, grow in gw.iterrows():
            dd = {}
            for keyname, col, kind in FEATURE_KEYS:
                if col in DECISION_COLS and keyname in gw.columns and grow.get(keyname) is not None:
                    dd[col] = _coerce(grow[keyname], kind)
            gmap[grow["entityId"]] = dd
        for col in DECISION_COLS:
            pdf[col] = [gmap.get(eid, {}).get(col, 0.0) for eid in pdf["entityId"]]
        print(f"overlaid {len(DECISION_COLS)} gold decision facts onto {len(pdf)} decision points ({len(gmap)} members)")
    except Exception as e:
        print(f"  gold decision overlay skipped ({type(e).__name__}: {e})")

# the member's CURRENT features = the terminal next-state after their last send
finals = {}
try:
    gold = (spark.table(f"{SRC_NS}.gold_member_snapshot")
            .select("entityId", parse_features_udf("factsJson").alias("f")))
    gpd = gold.select("entityId", *[F.col(f"f.{c}").alias(c) for c in FEATURE_COLS]).toPandas()
    finals = {r["entityId"]: np.array([float(r[c]) for c in FEATURE_COLS], np.float32) for _, r in gpd.iterrows()}
except Exception as e:
    print(f"  gold_member_snapshot unavailable ({type(e).__name__}) -> terminal = last send's features")

O, A, R, S2, D = (reconstruct_pandas(pdf, finals, ARM_IDX, NOOP_HC) if len(pdf) else
                  (np.zeros((0, STATE_DIM), np.float32), np.zeros(0, np.int64), np.zeros(0, np.float32),
                   np.zeros((0, STATE_DIM), np.float32), np.zeros(0, np.float32)))
print(f"reconstructed journey transitions: {len(O)} real")

# ── DYNAMIC-SIM COVERAGE (the all-channel unlock) ───────────────────────────────────────────────────────────────────
# The REAL journeys are dominated by the channels the live system actually sent (mostly email/push); the CQL is then
# COLD / out-of-distribution on the rarely-sent channels (sms, mail). We close that gap by turning the DYNAMIC sim
# (nba_ml_simulate -> sim_examples; covers ALL 5 channels with the realistic action_fit × channel_propensity × fatigue
# response) into RL transitions and UNION'ing them in, so the CQL trains on real ∪ sim. ADDITIVE — the real-journey logic
# above is untouched; obs is byte-compatible (the env's _obs_batch/_milestones_batch over FEATURE_COLS, dispositions=0).
#
# CRITICAL — NON-TERMINAL converts: a sim CONVERT (label=1) FLIPS the action's DRIVEN milestone fact (from the catalog),
# so the member PROGRESSES, and the transition is done=0. That makes the CQL credit the convert with the SAME downstream
# journey value the REAL multi-step transitions get (bootstrapped from the progressed next_obs). Without this, sms/mail
# (sim-only) cap at a flat one-step reward while email/push/voice (real multi-step) earn the discounted future, so
# sms/mail ALWAYS lose the argmax — the exact bug we saw (sms/mail Q maxed at 0). Reward = milestone VALUE gained by the
# flip (0 if it doesn't complete a milestone; the rest is bootstrapped). Non-converts (label=0) make no progress -> done=1.
sim_O = sim_A = sim_R = sim_S2 = sim_D = None; n_sim = 0; sim_ch_hist = {}
try:
    se = spark.table(f"{ML_NS}.sim_examples")
    spdf = se.select("actionId", "channel", "label", *[F.col(c) for c in FEATURE_COLS]).toPandas() if se.take(1) else None
    if spdf is not None and len(spdf):
        # key each (actionId, channel) to its CATALOG arm; drop rows the catalog doesn't model (cleaner than NOOP-mapping)
        arms = np.array([ARM_IDX.get((str(a), str(c)), -1) for a, c in zip(spdf["actionId"], spdf["channel"])], np.int64)
        keep = arms >= 0
        dropped = int((~keep).sum())
        spdf = spdf.loc[keep].reset_index(drop=True); arms = arms[keep]
        if len(spdf):
            # full feature vector = FEATURE_COLS block then the per-channel disposition block (dispositions default 0)
            Ff = np.zeros((len(spdf), NF + NDISP_TOTAL), np.float32)
            for j, c in enumerate(FEATURE_COLS): Ff[:, j] = pd.to_numeric(spdf[c], errors="coerce").fillna(0.0).to_numpy()
            lab = pd.to_numeric(spdf["label"], errors="coerce").fillna(0.0).to_numpy(np.float64); conv = lab >= 1
            # NON-TERMINAL converts: flip the action's driven milestone fact in the next state -> member advances
            drives_col = np.array([FCOL.get(ARMS_HC[a].get("drives"), -1) for a in arms], np.int64)  # base-feature col, -1 if unmapped
            S2f = Ff.copy(); fi = np.where(conv & (drives_col >= 0))[0]
            if len(fi): S2f[fi, drives_col[fi]] = 1.0
            ms = _milestones_batch(Ff); ms2 = _milestones_batch(S2f)
            sim_O = _obs_batch(Ff, ms, 0); sim_S2 = _obs_batch(S2f, ms2, 0)
            sim_A = arms
            gained = np.clip(ms2.astype(np.int16) - ms.astype(np.int16), 0, 1).astype(np.float64)    # (n,5) newly-true milestones
            sim_R = (gained @ MS_VALUES - SEND_COST).astype(np.float32)        # immediate milestone gain; rest bootstrapped
            sim_D = (1.0 - conv.astype(np.float32)).astype(np.float32)         # converts continue (done=0), else terminal
            n_sim = len(spdf)
            # CAP sim ~ the real count (floor 8k) so the real journeys' pacing/HOLD signal isn't drowned; the sim is
            # ~uniform per channel so uniform sampling keeps sms/mail covered.
            _cap = max(len(O), 8000)
            if n_sim > _cap:
                _si = np.random.default_rng(42).choice(n_sim, _cap, replace=False)
                sim_O, sim_S2, sim_A, sim_R, sim_D = sim_O[_si], sim_S2[_si], sim_A[_si], sim_R[_si], sim_D[_si]
                n_sim = _cap
            sim_ch_hist = pd.Series([ARMS_HC[a]["channel"] for a in sim_A]).value_counts().to_dict()
            print(f"sim transitions: {n_sim} (from {len(spdf)}, dropped {dropped}); non-terminal converts={int((sim_D==0).sum())}; per-channel {sim_ch_hist}")
    else:
        print("sim_examples empty/missing -> real-only (no sim coverage added)")
except Exception as e:
    print(f"sim_examples join skipped ({type(e).__name__}: {e}) -> real-only")

# ── UNION real ∪ sim and write ──────────────────────────────────────────────────────────────────────────────────────
schema = "obs array<float>, action int, reward float, next_obs array<float>, done float, source string"
frames = []
if len(O):
    frames.append(pd.DataFrame({"obs": [list(map(float, x)) for x in O], "action": A.astype(int),
                                "reward": R.astype(float), "next_obs": [list(map(float, x)) for x in S2],
                                "done": D.astype(float), "source": "real"}))
if n_sim:
    frames.append(pd.DataFrame({"obs": [list(map(float, x)) for x in sim_O], "action": sim_A.astype(int),
                                "reward": sim_R.astype(float), "next_obs": [list(map(float, x)) for x in sim_S2],
                                "done": sim_D.astype(float), "source": "sim"}))
if frames:
    sdf = spark.createDataFrame(pd.concat(frames, ignore_index=True))
else:
    sdf = spark.createDataFrame([], schema)
sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ML_NS}.journey_transitions")
_STARS_IDX = NF + NDISP_TOTAL + MILESTONE_IDS.index("stars")    # obs = [base | dispositions | milestones | step]
up = int((S2[:, _STARS_IDX] >= 1).sum()) if len(S2) else 0
print(f"wrote {ML_NS}.journey_transitions: {len(O)} real ({up} reach STARS) + {n_sim} sim = {len(O) + n_sim} total")
dbutils.notebook.exit(json.dumps({"transitions": int(len(O) + n_sim), "real": int(len(O)), "sim": int(n_sim),
                                  "sim_channel_dist": sim_ch_hist,
                                  "members": int(pdf['entityId'].nunique()) if len(pdf) else 0}))
