# Databricks notebook source
# SIMULATE — the digital twin + bulk DATA-GENERATION engine (vectorized). Resamples REAL gold members, draws each a
# RANDOM recent-contact fatigue state, samples a uniform logging policy over eligible arms, and labels with the DYNAMIC
# ground-truth response model (nba_ml_common: action_fit × channel_propensity × fatigue) — the SAME world nba_source_sim
# produces real outcomes from. The synthetic CONTEXT_FEATS span the full fatigue range so the model LEARNS the cost of
# repetition (hit a channel/ask too often, too soon → it converts poorly), not just who-fits-what. This is what makes
# the model deliver a VARIED, paced mix instead of converging on one channel. Output: {ml_ns}.sim_examples.

# COMMAND ----------

# MAGIC %run ./nba_ml_common

# COMMAND ----------

import json, numpy as np, pandas as pd
dbutils.widgets.text("n_members", "200000")
dbutils.widgets.text("seed_from_gold", "true")
ML_NS, SRC_NS = ml_widgets()
N = int(dbutils.widgets.get("n_members"))
rng = np.random.default_rng(7)

# ---------- 1) member population: resample the REAL gold feature distribution if available, else parametric ----------
def gold_pop():
    try:
        g = spark.table(f"{SRC_NS}.gold_member_snapshot").where("key LIKE 'operator.%'")
        wide = g.groupBy("nbaId").pivot("key").agg(F.first("value")).toPandas()
        if len(wide) < 200: return None
        rows = [features_from_factmap({k: wide.iloc[i].get(k) for k in wide.columns if k != "nbaId"}) for i in range(len(wide))]
        return pd.DataFrame(rows)
    except Exception:
        return None

real = gold_pop() if dbutils.widgets.get("seed_from_gold") == "true" else None
if real is not None and len(real):
    M = real.sample(N, replace=True, random_state=7).reset_index(drop=True)
    print(f"member population: resampled {N} from {len(real)} real gold members")
else:                                                          # parametric fallback over the HEALTHCARE feature contract
    M = pd.DataFrame({
        "daysSinceLogin": rng.integers(0, 60, N).astype(float), "riskScore": np.clip(rng.normal(1.5, 1.0, N), 0, 5),
        "diabetic": (rng.random(N) < 0.30).astype(float), "isDNC": (rng.random(N) < 0.05).astype(float),
        "smsConsent": (rng.random(N) < 0.65).astype(float), "age": np.clip(rng.normal(68, 12, N), 18, 95),
        "sdohBarrier": (rng.random(N) < 0.25).astype(float), "comorbidityCount": rng.poisson(1.8, N).astype(float),
        "erVisits12mo": rng.poisson(0.4, N).astype(float), "rxAdherencePDC": np.clip(rng.normal(0.7, 0.2, N), 0, 1),
        "openCareGaps": rng.poisson(1.5, N).astype(float), "portalLogins30d": rng.poisson(2.0, N).astype(float),
        "pagesViewed30d": rng.poisson(3.0, N).astype(float)})
    print(f"member population: parametric synthetic ({N})")

def col(k): return M[k].to_numpy() if k in M.columns else np.zeros(len(M))

# ---------- 2) DYNAMIC ground-truth (vectorized mirror of nba_ml_common): action_fit × channel_propensity × fatigue ----
isDNC = col("isDNC"); smsConsent = col("smsConsent")
def af_vec(action):                                                        # action_fit over the population
    spec = FSTAR.get(action, {})
    z = np.full(N, float(spec.get("b", 0.0)))
    for k, w in spec.items():
        if k != "b": z = z + float(w) * col(k)
    return 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))
af_by_action = np.column_stack([af_vec(a) for a in HEALTHCARE_ACTIONS])    # N×15

digital = np.clip((col("portalLogins30d") / 15.0 + col("pagesViewed30d") / 40.0) / 2.0, 0, 1)
old = np.clip((col("age") - 55.0) / 30.0, 0, 1)
comp = np.clip(col("comorbidityCount") / 5.0, 0, 1)
sdoh = (col("sdohBarrier") >= 1).astype(float)
# SMS has the STEEPEST digital slope (0.72) -> it is the BEST channel for the high-digital ("text-first") slice, a real
# learnable archetype, so the policy routes them to SMS instead of holding. MUST mirror nba_source_sim.channel_propensity.
cp_base = {"email": 0.35 + 0.55 * digital, "push": 0.18 + 0.55 * digital - 0.12 * old,
           "sms": 0.38 + 0.72 * digital - 0.10 * old, "voice": 0.20 + 0.45 * old + 0.45 * comp,
           "mail": 0.24 + 0.42 * old * (1.0 - digital) + 0.30 * sdoh - 0.22 * comp}
tilt = np.exp(rng.normal(0.0, 0.45, (N, len(CHANNELS))))                   # per-member idiosyncratic taste (irreducible noise)
chan_prop = np.clip(np.column_stack([cp_base[c] for c in CHANNELS]), 0.03, 1.0) * tilt   # N×5
tol = np.clip(rng.normal(1.0, 0.4, N), 0.4, 2.2)

# RANDOM recent-contact fatigue state per member, spanning the full range so the model learns the decay.
tot_n = rng.poisson(rng.uniform(0.0, 6.0, N)).astype(int)
this_ch_n = rng.binomial(np.maximum(tot_n, 0), 0.40)                       # this channel's recent count (concentration)
this_act_n = rng.binomial(np.maximum(this_ch_n, 0), 0.55)                  # this exact ASK's recent count (subset)
days_since = np.clip(rng.exponential(3.0, N), 0.0, 14.0)
def fatigue_vec(ch_n, act_n, tt, ds, t):
    return (np.exp(-0.40 / t * ch_n) * np.exp(-0.55 / t * act_n) * np.exp(-0.10 / t * tt)
            * (0.4 + 0.6 * (1.0 - np.exp(-np.maximum(0.0, ds) / 2.5))))

# ---------- 3) logging policy = uniform over each member's eligible (action, channel); isDNC -> no eligible arms ----------
elig_ch = np.column_stack([((isDNC < 1) & ~(np.full(N, c == "sms") & (smsConsent < 1))) for c in CHANNELS])  # N×5
nelig = elig_ch.sum(1); keep = nelig > 0
ch_idx = (rng.random((N, len(CHANNELS))) * elig_ch).argmax(1)              # uniform eligible channel
ac_idx = rng.integers(0, len(HEALTHCARE_ACTIONS), N)                       # uniform action
prop = 1.0 / (len(HEALTHCARE_ACTIONS) * np.maximum(nelig, 1))

ar = np.arange(N)
convert = np.clip(af_by_action[ar, ac_idx] * chan_prop[ar, ch_idx]
                  * fatigue_vec(this_ch_n, this_act_n, tot_n, days_since, tol), 0.0, 0.98)
label = (rng.random(N) < convert).astype(int)

out = M[keep].copy()
out["actionId"] = [HEALTHCARE_ACTIONS[i] for i in ac_idx[keep]]
out["channel"]  = [CHANNELS[i] for i in ch_idx[keep]]
out["thisChannelRecentN"] = this_ch_n[keep].astype(float)
out["thisActionRecentN"]  = this_act_n[keep].astype(float)
out["daysSinceLastContact"] = days_since[keep].round(2)
out["propensity"] = prop[keep].round(5)
out["label"] = label[keep]
out["source"] = "sim"
for c in FEATURE_COLS:
    if c not in out.columns: out[c] = 0.0
out = out[FEATURE_COLS + ["actionId", "channel", "propensity", "label", "source"]]
sim = spark.createDataFrame(out).withColumn("ts", F.current_timestamp())
sim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ML_NS}.sim_examples")

n = sim.count(); pos = int(out["label"].sum())
# DIAGNOSTICS: (a) FRESH best-channel mix (argmax channel_propensity, no fatigue) = the per-member channel variation;
# (b) the FATIGUE effect = convert rate on a fresh channel (0 recent) vs a hammered one (>=3) — should drop sharply.
fresh_best = pd.Series([CHANNELS[i] for i in chan_prop.argmax(1)[keep]]).value_counts(normalize=True).round(3).to_dict()
lk = label[keep]; chk = this_ch_n[keep]
fresh_cr = round(float(lk[chk == 0].mean()) if (chk == 0).any() else 0.0, 3)
tired_cr = round(float(lk[chk >= 3].mean()) if (chk >= 3).any() else 0.0, 3)
print(f"sim_examples: {n} rows  positives={pos}  base_rate={pos/max(n,1):.3f}")
print(f"  fresh best-channel mix (per-member archetype winner): {fresh_best}")
print(f"  FATIGUE effect — convert rate: fresh channel(0)={fresh_cr}  hammered(>=3)={tired_cr}")
dbutils.notebook.exit(json.dumps({"sim_rows": n, "positives": pos, "fresh_best_channel_mix": fresh_best,
                                  "fatigue_fresh_cr": fresh_cr, "fatigue_tired_cr": tired_cr}))
