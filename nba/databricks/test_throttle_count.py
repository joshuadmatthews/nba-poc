#!/usr/bin/env python
"""Cross-source throttle-count test — proves the LAKE counts NBA + external sends together.

  python test_throttle_count.py     # exits non-zero on failure

The throttle level is "every send on a channel, whoever sent it". NBA's own sends arrive as
nba.disposition.{action}.{channel}=sent; external systems send a variety of shapes that we normalize
into the SAME disposition shape on ingest (here: comms.sent.{channel}). This produces one of each for
a fresh channel-day, ingests to the lake, and asserts BOTH land as factClass='disposition' value='sent'
and are counted in the per-channel daily aggregate the throttle emit reads.
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

CH = "email"
RUN = str(int(time.time()) % 100000)
NBA_M = "thrcnt_nba_" + RUN          # NBA's own send (disposition)
EXT_M = "thrcnt_ext_" + RUN          # external system's send (comms.sent.*)
ts = int(time.time() * 1000)
print(f"== producing one NBA send + one EXTERNAL send on '{CH}' (run {RUN}) ==")
# NBA disposition=sent (what the action-layer emits)
nba_fact = {"entityType": "OPERATOR", "entityId": NBA_M, "key": f"nba.disposition.thrcnt.{CH}",
            "value": "sent", "valueType": "STRING", "eventTs": ts, "source": "test-nba", "nbaId": "nba_" + NBA_M}
# external send: a different shape (comms.sent.{channel}) normalized into our disposition shape on ingest
ext_fact = {"entityType": "OPERATOR", "entityId": EXT_M, "key": f"comms.sent.{CH}",
            "value": 1, "valueType": "LONG", "eventTs": ts + 1, "source": "some-external-system"}
for topic in ("nba.facts", "nba.member.facts"):
    produce(topic, f"OPERATOR:{NBA_M}:{nba_fact['key']}", json.dumps(nba_fact))
    produce(topic, f"OPERATOR:{EXT_M}:{ext_fact['key']}", json.dumps(ext_fact))
time.sleep(2)

print("== ingesting to the lake (normalizes external -> disposition shape) ==")
out = subprocess.run([sys.executable, os.path.join(HERE, "run_kafka_jobs.py"), "datalake"], capture_output=True, text=True, cwd=HERE)
print("  " + (out.stdout.strip().splitlines() or ["(no output)"])[-1])

print("== assert both sources land + are counted ==")
# external normalized into the disposition shape
ext = q(f"SELECT factClass, channel, value FROM {NS}.silver_fact_history WHERE entityId='{EXT_M}' AND key='comms.sent.{CH}' ORDER BY ingestTs DESC LIMIT 1")
chk("external comms.sent.email normalized to disposition shape", bool(ext) and ext[0][0] == "disposition" and ext[0][1] == CH and ext[0][2] == "sent", f"({ext})")
# NBA send present as disposition
nba = q(f"SELECT factClass, channel, value FROM {NS}.silver_fact_history WHERE entityId='{NBA_M}' AND value='sent' AND channel='{CH}' LIMIT 1")
chk("NBA disposition=sent present", bool(nba) and nba[0][0] == "disposition", f"({nba})")
# both counted in the per-channel aggregate the throttle emit reads (this run's two members)
cnt = q(f"""SELECT count(distinct entityId, actionId, channel, eventTs) FROM {NS}.silver_fact_history
            WHERE factClass='disposition' AND value='sent' AND channel='{CH}'
              AND entityId IN ('{NBA_M}','{EXT_M}')""")
n = int(cnt[0][0]) if cnt else 0
chk("BOTH the NBA send AND the external send counted (cross-source)", n == 2, f"(counted={n}, expected 2)")

print("== assert the throttle emit broadcasts an email level >= these sends ==")
out = subprocess.run([sys.executable, os.path.join(HERE, "run_kafka_jobs.py"), "throttle"], capture_output=True, text=True, cwd=HERE)
line = (out.stdout.strip().splitlines() or ["(no output)"])[-1]
print("  " + line)
try:
    counts = json.loads(line[line.index("{"):]).get("counts", {})
    chk("throttle emit counts the email channel (>= our 2 sends)", int(counts.get(CH, 0)) >= 2, f"(emit email={counts.get(CH)})")
except Exception as ex:
    chk("throttle emit parseable", False, f"({ex})")

print(f"\n================ THROTTLE COUNT: {PASS} passed, {FAIL} failed ================")
sys.exit(1 if FAIL else 0)
