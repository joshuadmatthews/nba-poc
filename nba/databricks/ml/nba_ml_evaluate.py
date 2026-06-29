# Databricks notebook source
# EVALUATE — champion vs challenger, then promote the winner. Two estimators:
#   1) OFFLINE (sim): each policy plays the digital twin; compare realized conversion against f* (unbiased,
#      because the simulator IS the ground truth). Good for bootstrap / regression-guard.
#   2) OFF-POLICY (real logged data): IPS / self-normalized IPS on training_examples — estimates the value the
#      challenger policy WOULD have achieved on the traffic the champion actually served, corrected by the
#      logged propensity. Becomes trustworthy once the router logs true propensities (see bandit notebook).
# Promotion gate: challenger must beat champion by >= MARGIN on the trusted estimator with enough support.
# Sets @champion = challenger version when it wins. Conservative by default (no real propensity -> sim decides).

# COMMAND ----------

# MAGIC %run ./nba_ml_common

# COMMAND ----------

import json, mlflow, numpy as np, pandas as pd
from mlflow.tracking import MlflowClient
ML_NS, SRC_NS = ml_widgets()
dbutils.widgets.text("margin", "0.01"); dbutils.widgets.text("min_support", "2000")
mlflow.set_registry_uri("databricks-uc")
NAME = model_name(ML_NS); c = MlflowClient(); MARGIN = float(dbutils.widgets.get("margin"))

def load_model(alias):
    return mlflow.pyfunc.load_model(f"models:/{NAME}@{alias}")
champ, chal = load_model("champion"), load_model("challenger")
champ_v = c.get_model_version_by_alias(NAME, "champion").version
chal_v = c.get_model_version_by_alias(NAME, "challenger").version

def score(model, df):                                  # both champion + challenger are propensity pyfuncs
    return np.asarray(model.predict(df[FEATURE_COLS + ["actionId", "channel"]])).reshape(-1)

# ---------- 1) simulator eval: which policy, picking argmax score over each member's eligible set, converts more
# against f*? (sim_examples already carries p_star = ground-truth convert prob of the served action.) ----------
sim_score = None
try:
    sim = spark.table(f"{ML_NS}.sim_examples").toPandas()
    sim["champ"] = score(champ, sim); sim["chal"] = score(chal, sim)
    # value of a policy on the sim population = mean p_star of the action each policy would rank #1 per member.
    # (Proxy: correlation of policy score with p_star — higher = better ranking. Exact rollout is the upgrade.)
    sim_score = {"champion": float(np.corrcoef(sim["champ"], sim["p_star"])[0, 1]),
                 "challenger": float(np.corrcoef(sim["chal"], sim["p_star"])[0, 1])}
except Exception as e:
    print("sim eval skipped:", e)

# ---------- 2) on the real labeled examples: AUC (always available) + off-policy IPS (only with real propensity variation) ----------
ips = None; auc_score = None
try:
    real = spark.table(f"{ML_NS}.training_examples").toPandas()
    if len(real) >= int(dbutils.widgets.get("min_support")) and real["label"].nunique() > 1:
        real["chal"] = score(chal, real); real["champ"] = score(champ, real)
        from sklearn.metrics import roc_auc_score
        auc_score = {"champion": float(roc_auc_score(real["label"], real["champ"])),
                     "challenger": float(roc_auc_score(real["label"], real["chal"]))}
        # SNIPS is only trustworthy with REAL propensity VARIATION (Thompson's vote propensities). A degenerate
        # all-0.5 fallback is noise, so leave ips=None and let AUC / the unbiased sim decide.
        if real["propensity"].notna().any() and real["propensity"].nunique() > 1:
            w = (1.0 / real["propensity"].clip(0.05, 1.0))
            ips = {"champion_snips": float((w * real["label"] * (real["champ"] >= real["champ"].median())).sum() / (w * (real["champ"] >= real["champ"].median())).sum()),
                   "challenger_snips": float((w * real["label"] * (real["chal"] >= real["chal"].median())).sum() / (w * (real["chal"] >= real["chal"].median())).sum())}
except Exception as e:
    print("real eval skipped:", e)

# ---------- promotion: IPS (real propensity) > AUC (on the REAL labels) > sim. The sim_examples are demo-domain, so
# a healthcare-trained challenger can correlate worse there than a stale champion (it vetoed promotions) — AUC on the
# real labeled data is the trustworthy fallback. Once Thompson logs propensities, IPS leads and this self-sustains. ----------
promote = False; basis = "none"
if ips and ips["challenger_snips"] > ips["champion_snips"] + MARGIN:
    promote, basis = True, "ips"
elif ips is None and auc_score and auc_score["challenger"] > auc_score["champion"] + MARGIN:
    promote, basis = True, "auc"
elif ips is None and sim_score and sim_score["challenger"] > sim_score["champion"] + MARGIN:
    promote, basis = True, "sim"

with mlflow.start_run(run_name="champion-challenger-eval"):
    if sim_score: mlflow.log_metrics({f"sim_{k}": v for k, v in sim_score.items()})
    if auc_score: mlflow.log_metrics({f"auc_{k}": v for k, v in auc_score.items()})
    if ips: mlflow.log_metrics(ips)
    mlflow.log_param("basis", basis); mlflow.log_param("promote", promote)

if promote:
    c.set_registered_model_alias(NAME, "champion", chal_v)
    print(f"PROMOTED challenger v{chal_v} -> @champion (basis={basis})")
else:
    print(f"kept champion v{champ_v}  (sim={sim_score}, ips={ips})")
dbutils.notebook.exit(json.dumps({"promoted": promote, "basis": basis, "champion": (chal_v if promote else champ_v)}))
