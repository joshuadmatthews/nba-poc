# Databricks notebook source
# TRAIN INITIAL — the safe cutover. Registers the CURRENT heuristic AS @champion (zero behavior change when the
# pipeline switches to model-served scores) and the FIRST learned model as @challenger. The challenger only
# takes over if nba_ml_evaluate proves it wins. Output: UC model {ml_ns}.nba_propensity @champion + @challenger.

# COMMAND ----------

# MAGIC %run ./nba_ml_common

# COMMAND ----------

import json, mlflow, numpy as np, pandas as pd
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

ML_NS, SRC_NS = ml_widgets()
mlflow.set_registry_uri("databricks-uc")
NAME = model_name(ML_NS)

def load(t):
    try: return spark.table(f"{ML_NS}.{t}").toPandas()
    except Exception: return pd.DataFrame()

real, sim = load("training_examples"), load("sim_examples")
cols = FEATURE_COLS + ["actionId", "channel", "label"]
parts = [d[cols] for d in (real, sim) if len(d) and set(cols).issubset(d.columns)]
data = pd.concat(parts, ignore_index=True) if parts else None
if data is None or data["label"].nunique() < 2:
    raise SystemExit("need labeled data with both classes — run nba_ml_simulate first")

CH = {c: i for i, c in enumerate(CHANNELS)}
acts = sorted(data["actionId"].astype(str).unique()); AC = {a: i for i, a in enumerate(acts)}
data["channel_idx"] = data["channel"].map(CH).fillna(-1)
data["action_idx"] = data["actionId"].astype(str).map(AC).fillna(-1)
X, y = data[TRAIN_FEATS].astype(float), data["label"].astype(int)
sig_in = (real if len(real) else sim)[FEATURE_COLS + ["actionId", "channel"]]

# @champion = the heuristic (what we do today)  — UC: read the version from the log_model result
with mlflow.start_run(run_name="initial-champion-heuristic"):
    _ci = mlflow.pyfunc.log_model("model", python_model=HeuristicModel(),
                                  registered_model_name=NAME, signature=infer_signature(sig_in, np.array([0.5])))
champ_v = _ci.registered_model_version

# @challenger = the first learned model
with mlflow.start_run(run_name="initial-challenger-model"):
    clf = HistGradientBoostingClassifier(max_iter=300, max_depth=6, learning_rate=0.08, validation_fraction=0.1)
    clf.fit(X, y)
    auc = float(roc_auc_score(y, clf.predict_proba(X)[:, 1]))
    # ORACLE: the best AUC anyone could get against f* is roc_auc(label, p_star). If model AUC ~ oracle, the
    # model has learned essentially all the LEARNABLE signal (the rest is irreducible noise in f*).
    oracle = (float(roc_auc_score(sim["label"], sim["p_star"]))
              if ("p_star" in sim.columns and len(sim) and sim["label"].nunique() > 1) else None)
    mlflow.log_metric("train_auc", auc)
    if oracle is not None: mlflow.log_metric("oracle_auc", oracle)
    mlflow.log_dict({"channels": CH, "actions": AC}, "encoders.json")
    _xi = mlflow.pyfunc.log_model("model", python_model=PropensityModel(clf, CH, AC),
                                  registered_model_name=NAME, signature=infer_signature(sig_in, np.array([0.5])))
chal_v = _xi.registered_model_version

c = MlflowClient()
c.set_registered_model_alias(NAME, "champion", champ_v)
c.set_registered_model_alias(NAME, "challenger", chal_v)
capture = f"{auc/oracle*100:.0f}% of achievable" if oracle else "n/a"
print(f"{NAME}: @champion=v{champ_v} (heuristic)  @challenger=v{chal_v} (model)  "
      f"train_auc={auc:.3f}  oracle_auc={oracle}  ({capture})")
dbutils.notebook.exit(json.dumps({"champion": champ_v, "challenger": chal_v, "train_auc": auc, "oracle_auc": oracle}))
