# Databricks notebook source
# BANDIT — the online / RL layer. NBA is a CONTEXTUAL BANDIT: context = member features, arms = eligible
# (action, channel), reward = conversion. "Learns as data comes in" = frequent incremental policy updates +
# principled exploration so we gather counterfactual signal on arms the current policy avoids.
#
# Exploration lives entirely in the ML LAYER (nba_ml_score): it softmax-samples a winner per slot and LOGS the
# propensity to predictions_log — so the action-router stays a pure deterministic argmax (NO router change). This
# notebook CONSUMES the logged (context, arm, propensity, reward) tuples and TUNES the scorer's exploration
# temperature (written to {ml_ns}.bandit_policy; nba_ml_score reads it).
#
# What it does:
#   1) reads predictions_log (scores + logged propensity) joined to realized outcomes (terminal state)
#   2) updates a per-arm Beta–Bernoulli posterior on conversion (LinUCB over features is the next increment)
#   3) computes off-policy value (SNIPS) on the logged data with the REAL propensities
#   4) recommends a temperature (anneal down as confidence grows) and writes {ml_ns}.bandit_policy
# Full online RL (streaming Structured Streaming update + Thompson sampling at score time) is the next increment.

# COMMAND ----------

# MAGIC %run ./nba_ml_common

# COMMAND ----------

import json, numpy as np, pandas as pd
ML_NS, SRC_NS = ml_widgets()
dbutils.widgets.text("temperature", "0.15")   # softmax temp for exploration; 0 -> pure exploit (argmax)
dbutils.widgets.text("min_per_arm", "50")

# ---------- 1) logged bandit tuples from training_examples (PRESENTED-gated label + the scorer's logged
# propensity — already joined by nba_ml_build_training_set). reward = label (1 converted / 0 not). ----------
try:
    logged = (spark.table(f"{ML_NS}.training_examples")
              .select("actionId", "channel", "propensity", F.col("label").alias("reward"))).toPandas()
except Exception:
    dbutils.notebook.exit(json.dumps({"note": "no training_examples yet — run nba_ml_build_training_set first"}))

# ---------- 2) per-arm Beta–Bernoulli posterior (the simplest honest bandit on conversion) ----------
arms = {}
for (a, c), grp in logged.dropna(subset=["reward"]).groupby(["actionId", "channel"]):
    s = int(grp["reward"].sum()); n = int(grp["reward"].count())
    arms[f"{a}:{c}"] = {"alpha": 1 + s, "beta": 1 + (n - s), "n": n, "mean": (1 + s) / (2 + n)}

# ---------- 3) off-policy value (SNIPS) of champion vs an exploring policy ----------
TEMP = float(dbutils.widgets.get("temperature"))
val = None
if "propensity" in logged.columns and logged["propensity"].notna().any() and logged["reward"].notna().any():
    d = logged.dropna(subset=["reward", "propensity"]).copy()
    w = (1.0 / d["propensity"].clip(0.05, 1.0))
    val = {"logged_snips": float((w * d["reward"]).sum() / w.sum()), "n": int(len(d))}

# ---------- 4) publish the exploration policy nba_ml_score reads (anneal temp as data accrues) ----------
total_n = sum(a["n"] for a in arms.values()) if arms else 0
rec_temp = float(np.clip(0.30 * np.exp(-total_n / 20000.0), 0.03, 0.30))   # explore more when data is thin
policy = {"temperature": round(rec_temp, 3), "arms": arms, "off_policy": val,
          "note": "nba_ml_score reads this temperature to set softmax exploration; the router stays pure argmax"}
spark.createDataFrame([(json.dumps(policy), )], "policy string") \
    .withColumn("updatedAt", F.current_timestamp()) \
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ML_NS}.bandit_policy")
print("bandit_policy:", json.dumps(policy)[:400])
dbutils.notebook.exit(json.dumps({"arms": len(arms), "off_policy": val}))
