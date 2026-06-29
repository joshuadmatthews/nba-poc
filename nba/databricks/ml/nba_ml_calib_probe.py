# Databricks notebook source
# CALIBRATION PROBE (throwaway) — load the REAL gold member population and print the per-member best-channel mix for
# the SOURCE-SIM's channel_match (the actual ground-truth per-member channel propensity), current vs a widened-mail
# variant. argmax channel_match = the channel the model would learn as best (channel_match multiplies the action's
# convert prob uniformly across channels, so the best channel is the argmax of the multiplier). Read-only.

# COMMAND ----------

# MAGIC %run ./nba_ml_common

# COMMAND ----------

import json, numpy as np, pandas as pd
ML_NS, SRC_NS = ml_widgets()
g = spark.table(f"{SRC_NS}.gold_member_snapshot").where("key LIKE 'operator.%'")
wide = g.groupBy("nbaId").pivot("key").agg(F.first("value")).toPandas()
rows = [features_from_factmap({k: wide.iloc[i].get(k) for k in wide.columns if k != "nbaId"}) for i in range(len(wide))]
M = pd.DataFrame(rows); N = len(M)
feats = ["age", "portalLogins30d", "pagesViewed30d", "comorbidityCount", "sdohBarrier", "erVisits12mo", "riskScore", "smsConsent", "isDNC"]
def col(k): return M[k].to_numpy() if k in M.columns else np.zeros(N)

def cm_scores(variant):
    digital = np.clip((col("portalLogins30d") / 15.0 + col("pagesViewed30d") / 40.0) / 2.0, 0, 1)
    old = np.clip((col("age") - 55.0) / 30.0, 0, 1)
    comp = np.clip(col("comorbidityCount") / 5.0, 0, 1)
    sdoh = (col("sdohBarrier") >= 1).astype(float)
    s = {"email": 0.30 + 0.65 * digital, "push": 0.25 + 0.60 * digital, "sms": 0.45 + 0.20 * digital}
    if variant == "current":                                   # today's source-sim channel_match (mail = flat 0.40)
        s["voice"] = 0.35 + 0.55 * old * (1 - digital)
        s["mail"] = np.full(N, 0.40)
    else:                                                      # WIDEN MAIL: voice=clinical complexity, mail=traditional
        s["voice"] = 0.25 + 0.45 * old * (1 - digital) + 0.35 * comp
        s["mail"] = 0.38 + 0.40 * old * (1 - digital) + 0.22 * sdoh - 0.30 * comp
    return s

def mix(variant):
    s = cm_scores(variant); chans = ["email", "push", "sms", "voice", "mail"]
    Z = np.column_stack([s[c] for c in chans])
    Z[col("smsConsent") < 1, chans.index("sms")] = -1e9        # consent gate
    Z[col("isDNC") >= 1, :] = -1e9                             # compliance gate
    best = Z.argmax(1)
    return pd.Series([chans[b] for b in best]).value_counts(normalize=True).round(3).to_dict()

result = {
    "N": N,
    "feat_median": {f: round(float(M[f].median()), 2) for f in feats},
    "channel_match_mix": {v: mix(v) for v in ["current", "widen_mail"]},
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
