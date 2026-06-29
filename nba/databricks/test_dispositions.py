#!/usr/bin/env python
"""Per-channel disposition funnel test — proves the LAKE counts each funnel stage correctly.

  python test_dispositions.py     # exits non-zero on failure

The Command Center reads per-(channel, value) disposition counts to render the funnel the operator sees
(outbound: Sent -> Delivered -> Read -> LinkClicked; inbound pull: Presented -> Accepted -> Completed).
This produces a KNOWN set of disposition facts for a unique run — for 'email': 3x Delivered, 2x Read,
1x LinkClicked; for the inbound 'app' channel: 3x Presented, 2x Accepted, 1x Completed — each with a
distinct member + eventTs so the distinct-count aggregation counts every one. It ingests to the lake and
asserts the per-(channel, value) counts match exactly what was produced, using the same
count(distinct entityId, actionId, channel, eventTs) the BFF uses.
"""
import os, sys, json, time, subprocess
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
W = WorkspaceClient(host=E["DATABRICKS_HOST"], client_id=E["DATABRICKS_CLIENT_ID"], client_secret=E["DATABRICKS_CLIENT_SECRET"], auth_type="oauth-m2m")
WH = next(iter(W.warehouses.list())).id

def q(sql):
    r = W.statement_execution.execute_statement(warehouse_id=WH, wait_timeout="50s", statement=sql)
    return r.result.data_array if (r.result and r.result.data_array) else []

def produce(topic, key, payload):
    subprocess.run(["podman", "exec", "-i", "ais-nba-redpanda", "rpk", "topic", "produce", topic, "-k", key],
                   input=(payload + "\n").encode(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)

PASS = FAIL = 0
def chk(desc, cond, detail=""):
    global PASS, FAIL
    if cond: print(f"  PASS  {desc}"); PASS += 1
    else: print(f"  FAIL  {desc}  {detail}"); FAIL += 1

RUN = str(int(time.time()) % 100000)
ACTION = "dispfunnel"
# (channel, status) -> count produced. Outbound email + inbound 'app'.
PLAN = {
    ("email", "Delivered"):   3,
    ("email", "Read"):        2,
    ("email", "LinkClicked"): 1,
    ("app",   "Presented"):   3,
    ("app",   "Accepted"):    2,
    ("app",   "Completed"):   1,
}

print(f"== producing disposition funnel facts (run {RUN}) ==")
ids = []                         # every entityId we create, so we can scope queries to this run
ts = int(time.time() * 1000)
i = 0
for (ch, status), n in PLAN.items():
    for _ in range(n):
        i += 1
        ts += 1
        mid = f"disp_{RUN}_{i}"   # distinct member per produced row
        ids.append(mid)
        actionId = f"{ACTION}_{RUN}"
        fact = {"entityType": "OPERATOR", "entityId": mid,
                "key": f"nba.disposition.{actionId}.{ch}",
                "value": status, "valueType": "STRING", "eventTs": ts, "source": "test"}
        key = f"OPERATOR:{mid}:{fact['key']}"
        # produce to BOTH topics; the ingest reads nba.facts
        produce("nba.facts", key, json.dumps(fact))
        produce("nba.member.facts", key, json.dumps(fact))
print(f"  produced {len(ids)} disposition facts across {len(PLAN)} (channel,status) buckets")
time.sleep(2)

print("== ingesting to the lake ==")
out = subprocess.run([sys.executable, os.path.join(HERE, "run_kafka_jobs.py"), "datalake"], capture_output=True, text=True, cwd=HERE)
print("  " + (out.stdout.strip().splitlines() or ["(no output)"])[-1])

# give the lake a beat to settle if the ingest lags
time.sleep(3)

print("== assert the per-(channel,value) funnel counts match what was produced ==")
idlist = ",".join(f"'{m}'" for m in ids)
# count(distinct entityId, actionId, channel, eventTs) — exactly how the BFF counts a funnel stage
rows = q(f"""SELECT channel, value, count(distinct entityId, actionId, channel, eventTs) AS n
             FROM {NS}.silver_fact_history
             WHERE factClass='disposition' AND entityId IN ({idlist})
             GROUP BY channel, value""")
got = {(r[0], r[1]): int(r[2]) for r in rows}

for (ch, status), expected in PLAN.items():
    actual = got.get((ch, status), 0)
    chk(f"{ch} {status} = {expected}", actual == expected, f"(got {actual})")

# the funnel must contain ONLY what we produced for this run — no stray buckets
chk("no unexpected (channel,value) buckets for this run", set(got.keys()) == set(PLAN.keys()), f"(got {sorted(got.keys())})")

print(f"\n================ DISPOSITIONS: {PASS} passed, {FAIL} failed ================")
sys.exit(1 if FAIL else 0)
