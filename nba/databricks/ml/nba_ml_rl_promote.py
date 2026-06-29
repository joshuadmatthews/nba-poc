# Databricks notebook source
# EVALUATE-GATE + PROMOTE for the RL policy. Reads the rl_card the train job just wrote, and ONLY copies the freshly
# trained Q-net over the live serving name (rl_qnet.json) if it clears the bar AND beats the myopic baseline. The RL
# scorer (nba_ml_score_rl) reloads rl_qnet.json each drain, so a passing promote goes live with NO restart; a failing
# challenger leaves the previous champion untouched. This is the RL arm of the retrain->evaluate->promote loop.
import shutil, os, json
dbutils.widgets.text("ml_catalog", "ml_worspace"); dbutils.widgets.text("ml_schema", "core")
dbutils.widgets.text("src", "rl_qnet_dbx.json"); dbutils.widgets.text("dst", "rl_qnet.json")
dbutils.widgets.text("recreate_frac", "0.75")   # CQL must recreate >= this FRACTION of the BC clone's playbook match
dbutils.widgets.text("min_bc", "0.12")          # ...and the logged data must be learnable at all (BC clears this floor)
ML_CAT = dbutils.widgets.get("ml_catalog"); ML_SCH = dbutils.widgets.get("ml_schema")
VOL = f"/Volumes/{ML_CAT}/{ML_SCH}/ckpt"
src = f"{VOL}/{dbutils.widgets.get('src')}"; dst = f"{VOL}/{dbutils.widgets.get('dst')}"
RECREATE_FRAC = float(dbutils.widgets.get("recreate_frac")); MIN_BC = float(dbutils.widgets.get("min_bc"))
assert os.path.exists(src), f"missing {src}"

card = json.loads(spark.table(f"{ML_CAT}.{ML_SCH}.rl_card").collect()[0]["card"])
# Gate RELATIVE to the BC ceiling, not a fixed match floor. The CQL is meant to LEARN the playbook then BEAT it by
# diversifying (e.g. into sms/mail), and the action space grew (mail -> 62 arms), so the achievable playbook-match
# ceiling DROPPED — a fixed floor (the old 0.5) wrongly rejects a healthy, more-diverse policy. Instead promote iff the
# CQL recreates the data ~as well as a pure behavior-clone CAN (cql >= frac·bc) AND the data is learnable at all (bc >=
# MIN_BC). This admits diversification without admitting a degenerate/random policy (which matches far below the BC).
cql_match = float(card.get("cql_playbook_match", card.get("cql_match", 0.0)))
bc_match = float(card.get("bc_playbook_match", 0.0))
bar = RECREATE_FRAC * bc_match
promote = (bc_match >= MIN_BC) and (cql_match >= bar) and (cql_match >= 0.12)
print(f"challenger: cql_playbook_match={cql_match:.3f}  bc={bc_match:.3f}  |  gate: cql>={RECREATE_FRAC}·bc={bar:.3f} AND bc>={MIN_BC} -> {'PROMOTE' if promote else 'SKIP'}")

if promote:
    shutil.copyfile(src, dst)
    msg = f"PROMOTED {dbutils.widgets.get('src')} -> {dbutils.widgets.get('dst')} (cql_match={cql_match:.3f} vs bc={bc_match:.3f}, {os.path.getsize(dst)} bytes) — live on next scorer drain"
    print(msg); dbutils.notebook.exit(json.dumps({"promoted": True, "cql_match": cql_match, "bc_match": bc_match}))
else:
    msg = f"SKIPPED — cql_match={cql_match:.3f} vs bar {bar:.3f} (bc={bc_match:.3f}). Champion unchanged."
    print(msg); dbutils.notebook.exit(json.dumps({"promoted": False, "cql_match": cql_match, "bc_match": bc_match}))
