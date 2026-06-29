# Databricks notebook source
# ROLLBACK — incident recovery: re-point @champion to a known-good prior version. Pass target_version explicitly
# (operator picks the last-good vN), or leave it blank to step back to the highest version below the current champion.
# Aliases carry no history, so explicit target_version is the safe production path. Pure alias move — instant, no retrain.

# COMMAND ----------

# MAGIC %run ./nba_ml_common

# COMMAND ----------

import json, mlflow
from mlflow.tracking import MlflowClient
ML_NS, SRC_NS = ml_widgets()
dbutils.widgets.text("target_version", "")     # e.g. "7" — the known-good champion to restore
mlflow.set_registry_uri("databricks-uc")
NAME = model_name(ML_NS); c = MlflowClient()

cur = c.get_model_version_by_alias(NAME, "champion").version
tgt = dbutils.widgets.get("target_version").strip()
if not tgt:                                    # default: one step back (highest version below the current champion)
    vers = sorted((int(m.version) for m in c.search_model_versions(f"name='{NAME}'")), reverse=True)
    below = [v for v in vers if v < int(cur)]
    if not below: raise Exception(f"no prior version below champion v{cur} to roll back to — pass target_version")
    tgt = str(below[0])

if tgt == str(cur):
    print(f"@champion already at v{cur} — no-op"); dbutils.notebook.exit(json.dumps({"from": cur, "to": cur, "noop": True}))
c.set_registered_model_alias(NAME, "champion", tgt)
print(f"ROLLED BACK @champion: v{cur} -> v{tgt}")
dbutils.notebook.exit(json.dumps({"from": cur, "to": tgt}))
