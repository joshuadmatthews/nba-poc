# Databricks notebook source
# TRAIN — nightly retrain on the latest real (point-in-time) + simulator examples. Logs a new model version and
# points @challenger at it. Promotion is nba_ml_evaluate's job — this never touches @champion. Real outcomes are
# upweighted over sim so the model converges to true propensity as data accrues.

# COMMAND ----------

# MAGIC %run ./nba_ml_common

# COMMAND ----------

import json, mlflow, numpy as np, pandas as pd
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

ML_NS, SRC_NS = ml_widgets()
dbutils.widgets.text("sim_weight", "0.3")
dbutils.widgets.text("n_bootstrap", "8")   # K bootstrap resamples -> PropensityEnsemble for Thompson exploration in the scorer (1 = single model)
mlflow.set_registry_uri("databricks-uc")
NAME = model_name(ML_NS)

def load(t):
    try: return spark.table(f"{ML_NS}.{t}").toPandas()
    except Exception: return pd.DataFrame()

real, sim = load("training_examples"), load("sim_examples")
cols = FEATURE_COLS + ["actionId", "channel", "label"]
real = real[cols].assign(w=1.0) if len(real) and set(cols).issubset(real.columns) else pd.DataFrame()
sim = sim[cols].assign(w=float(dbutils.widgets.get("sim_weight"))) if len(sim) and set(cols).issubset(sim.columns) else pd.DataFrame()
data = pd.concat([d for d in (real, sim) if len(d)], ignore_index=True)
if not len(data) or data["label"].nunique() < 2:
    raise SystemExit("insufficient labeled data for retrain")

CH = {c: i for i, c in enumerate(CHANNELS)}
acts = sorted(data["actionId"].astype(str).unique()); AC = {a: i for i, a in enumerate(acts)}
data["channel_idx"] = data["channel"].map(CH).fillna(-1)
data["action_idx"] = data["actionId"].astype(str).map(AC).fillna(-1)
X, y, w = data[TRAIN_FEATS].astype(float), data["label"].astype(int), data["w"]

K = int(dbutils.widgets.get("n_bootstrap") or "1")
def _fit(Xd, yd, wd):
    c = HistGradientBoostingClassifier(max_iter=400, max_depth=6, learning_rate=0.06, validation_fraction=0.1)
    c.fit(Xd, yd, sample_weight=wd); return c
with mlflow.start_run(run_name="nightly-retrain"):
    if K > 1:                                   # THOMPSON: K bootstrap resamples; the scorer samples per decision
        brng = np.random.default_rng(42); ests = []     # from the ensemble spread (cold arm -> wide spread -> explored,
        for _k in range(K):                              # converges to the mean as data lands). train_auc on the mean.
            bi = brng.integers(0, len(data), len(data)); db = data.iloc[bi]
            ests.append(_fit(db[TRAIN_FEATS].astype(float), db["label"].astype(int), db["w"]))
        proba = np.mean([e.predict_proba(X)[:, 1] for e in ests], axis=0)
        pymodel = PropensityEnsemble(ests, CH, AC)
    else:
        clf = _fit(X, y, w); proba = clf.predict_proba(X)[:, 1]; pymodel = PropensityModel(clf, CH, AC)
    auc = float(roc_auc_score(y, proba))
    mlflow.log_metric("train_auc", auc); mlflow.log_metric("n_real", len(real)); mlflow.log_metric("n_sim", len(sim)); mlflow.log_metric("n_bootstrap", K)
    mlflow.log_dict({"channels": CH, "actions": AC}, "encoders.json")
    sig_in = data[FEATURE_COLS + ["actionId", "channel"]].head(50)
    _info = mlflow.pyfunc.log_model("model", python_model=pymodel,
                                    registered_model_name=NAME, signature=infer_signature(sig_in, np.array([0.5])))
v = _info.registered_model_version
MlflowClient().set_registered_model_alias(NAME, "challenger", v)
print(f"{NAME}: new @challenger=v{v}  train_auc={auc:.3f}  (real={len(real)}, sim={len(sim)})")
dbutils.notebook.exit(json.dumps({"challenger": v, "train_auc": auc, "n_real": len(real)}))
