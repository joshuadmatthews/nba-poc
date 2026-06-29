#!/usr/bin/env python
"""Databricks lake verification tests — assert the medallion's invariants via SQL.

  python test_lake.py        # all checks; exits non-zero on any failure

Covers the guarantees the NBA Command Center + fact-reconcile rely on:
  - gold_member_snapshot is strictly latest-per-(member,fact) and stamped with nbaId
  - gold_member_idmap maps each member to exactly one nbaId (from snapshots)
  - silver_fact_history is a typed, parsed, append-only history (factClass/actionId/channel)
  - action_fact_map = the action->fact routing derived from factsUsed
  - dim_definitions carries the action/rule defs
  - gold_facts (NBA fact-source) has Change Data Feed on for delta-only
"""
import os, sys, json
HERE = os.path.dirname(os.path.abspath(__file__))
NS = "workspace.nba_poc"

def env():
    e = {}
    for line in open(os.path.join(HERE, "databricks.env")):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1); e[k] = v
    return e

E = env()
from databricks.sdk import WorkspaceClient
W = WorkspaceClient(host=E["DATABRICKS_HOST"], client_id=E["DATABRICKS_CLIENT_ID"],
                    client_secret=E["DATABRICKS_CLIENT_SECRET"], auth_type="oauth-m2m")
WH = next(iter(W.warehouses.list())).id

def q(sql):
    r = W.statement_execution.execute_statement(warehouse_id=WH, wait_timeout="50s", statement=sql)
    if r.status and r.status.state and r.status.state.value != "SUCCEEDED":
        raise RuntimeError("SQL failed: " + (str(r.status.error)[:120] if r.status.error else "?"))
    return r.result.data_array if (r.result and r.result.data_array) else []

def scalar(sql): return q(sql)[0][0]

PASS = FAIL = 0
def chk(desc, cond, detail=""):
    global PASS, FAIL
    if cond: print(f"  PASS  {desc}"); PASS += 1
    else: print(f"  FAIL  {desc}  {detail}"); FAIL += 1

print("== GOLD: latest-per-fact + nbaId ==")
total = int(scalar(f"SELECT count(*) FROM {NS}.gold_member_snapshot"))
distinct = int(scalar(f"SELECT count(*) FROM (SELECT DISTINCT entityType,entityId,key FROM {NS}.gold_member_snapshot)"))
chk("gold_member_snapshot is strictly latest-only (no dup member/fact)", total == distinct, f"(rows={total} distinct={distinct})")
nn = int(scalar(f"SELECT count(*) FROM {NS}.gold_member_snapshot WHERE nbaId IS NOT NULL"))
chk("gold rows are mostly nbaId-stamped (>90%)", total == 0 or nn / total > 0.9, f"({nn}/{total})")
cdf_row = q(f"SHOW TBLPROPERTIES {NS}.gold_member_snapshot ('delta.enableChangeDataFeed')")[0]
chk("gold_member_snapshot has Change Data Feed enabled", str(cdf_row[-1]).lower() == "true", f"({cdf_row})")

print("== IDMAP: memberId -> single nbaId ==")
dupmap = int(scalar(f"SELECT count(*) FROM (SELECT entityId FROM {NS}.gold_member_idmap GROUP BY entityId HAVING count(DISTINCT nbaId) > 1)"))
chk("each member maps to exactly one nbaId", dupmap == 0, f"(dup={dupmap})")
nullnba = int(scalar(f"SELECT count(*) FROM {NS}.gold_member_idmap WHERE nbaId IS NULL"))
chk("no null nbaId in idmap", nullnba == 0, f"(null={nullnba})")

print("== SILVER: typed parsed history ==")
classes = {r[0] for r in q(f"SELECT DISTINCT factClass FROM {NS}.silver_fact_history")}
chk("silver_fact_history parses factClass (activity/score/disposition/...)", {"activity", "score"}.issubset(classes), f"({sorted(classes)})")
badscore = int(scalar(f"SELECT count(*) FROM {NS}.silver_fact_history WHERE factClass='score' AND actionId IS NULL"))
chk("score facts have actionId+channel parsed", badscore == 0, f"(unparsed={badscore})")
hist = int(scalar(f"SELECT count(*) FROM {NS}.silver_fact_history"))
chk("silver_fact_history retains history (append-only, > current gold)", hist >= total, f"(hist={hist} gold={total})")

print("== DIMENSIONS: definitions + action->fact routing ==")
nacts = int(scalar(f"SELECT count(*) FROM {NS}.dim_definitions WHERE defType='ACTION'"))
chk("dim_definitions has actions", nacts > 0, f"(actions={nacts})")
afm = int(scalar(f"SELECT count(*) FROM {NS}.action_fact_map"))
chk("action_fact_map (factsUsed routing) is populated", afm > 0, f"(edges={afm})")
# every fact in the map should be a real fact key
badfact = int(scalar(f"SELECT count(*) FROM {NS}.action_fact_map WHERE fact NOT LIKE 'operator.%' AND fact NOT LIKE 'nba.%'"))
chk("action_fact_map facts are well-formed keys", badfact == 0, f"(bad={badfact})")

print("== RECONCILE STATE ==")
try:
    rfs = int(scalar(f"SELECT count(*) FROM {NS}.gold_rulefacts_state"))
    chk("gold_rulefacts_state initialized (reconcile ran)", rfs > 0, f"(facts={rfs})")
except Exception:
    chk("gold_rulefacts_state initialized (reconcile ran)", False, "(table missing — run reconcile)")

print(f"\n================ LAKE: {PASS} passed, {FAIL} failed ================")
sys.exit(1 if FAIL else 0)
