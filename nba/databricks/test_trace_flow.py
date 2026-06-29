#!/usr/bin/env python
"""Cross-layer trace test — proves ONE fact flows through EVERY layer and the trace links up.

  python test_trace_flow.py     # exits non-zero on failure

Produces a fresh qualifying member, lets the live NBA pipeline cascade, ingests to the Databricks
lake, then asserts the SAME correlationId is present across silver_snapshots (with decision-time
facts), silver_eval_eligible, and silver_activations — and that the Command Center BFF returns an
explained, step-by-step trace for it. This is the end-to-end "everything flows across all layers".
"""
import os, sys, json, time, subprocess, urllib.request
HERE = os.path.dirname(os.path.abspath(__file__))
NS = "workspace.nba_poc"
BFF = os.environ.get("CC_BFF_URL", "http://localhost:4000")

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

def gql(query, variables=None):
    body = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(BFF + "/graphql", data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())["data"]

PASS = FAIL = 0
def chk(desc, cond, detail=""):
    global PASS, FAIL
    if cond: print(f"  PASS  {desc}"); PASS += 1
    else: print(f"  FAIL  {desc}  {detail}"); FAIL += 1

M = "traceflow" + str(int(time.time()) % 100000)
print(f"== producing qualifying member {M} ==")
ts = int(time.time() * 1000)
facts = [("operator.activity.usedChat", "true", "BOOL"), ("operator.activity.viewedDashboard", "true", "BOOL"),
         ("operator.activity.daysSinceLogin", "25", "LONG"), ("operator.activity.completedTasks", "2", "LONG"),
         ("operator.profile.isDNC", "false", "BOOL"), ("operator.comms.totalThisWeek", "0", "LONG"),
         ("operator.comms.emailsThisWeek", "0", "LONG")]
for topic in ("nba.facts", "nba.member.facts"):
    for k, v, vt in facts:
        ts += 1
        produce(topic, f"OPERATOR:{M}:{k}", json.dumps({"entityType": "OPERATOR", "entityId": M, "key": k, "value": json.loads(v) if vt != "STRING" else v, "valueType": vt, "eventTs": ts, "source": "traceflow"}))
    time.sleep(1)
print("  waiting for the pipeline to cascade…"); time.sleep(9)

print("== ingesting to the lake ==")
out = subprocess.run([sys.executable, os.path.join(HERE, "run_kafka_jobs.py"), "datalake"], capture_output=True, text=True, cwd=HERE)
print("  " + (out.stdout.strip().splitlines() or ["(no output)"])[-1])

print("== assert the trace spans every layer (by correlationId) ==")
# a correlationId for this member that reached an activation
rows = q(f"SELECT correlationId FROM {NS}.silver_activations WHERE entityId='{M}' AND correlationId IS NOT NULL ORDER BY eventTs DESC LIMIT 1")
cid = rows[0][0] if rows else None
chk("member reached an activation in the lake", cid is not None, f"(member={M})")
if cid:
    in_eval = int(q(f"SELECT count(*) FROM {NS}.silver_eval_eligible WHERE correlationId='{cid}'")[0][0])
    in_snap = int(q(f"SELECT count(*) FROM {NS}.silver_snapshots WHERE correlationId='{cid}'")[0][0])
    has_facts = int(q(f"SELECT count(*) FROM {NS}.silver_snapshots WHERE correlationId='{cid}' AND factsJson IS NOT NULL AND factsJson <> '{{}}'")[0][0])
    chk("same correlationId in silver_eval_eligible", in_eval > 0, f"(cid={cid[:8]})")
    chk("same correlationId in silver_snapshots", in_snap > 0)
    chk("snapshot captured decision-time facts (factsJson)", has_facts > 0)
    chk("member facts reached gold_member_snapshot", int(q(f"SELECT count(*) FROM {NS}.gold_member_snapshot WHERE entityId='{M}'")[0][0]) > 0)

    print("== assert the BFF returns an explained trace ==")
    try:
        t = gql("query($c:ID!){ trace(correlationId:$c) }", {"c": cid})["trace"]
        chk("BFF trace found", t and t.get("found"))
        nodes = [s["node"] for s in (t.get("steps") or [])]
        chk("trace steps span snapshot -> rules -> router", set(["snapshot", "rules", "router"]).issubset(nodes), f"({nodes})")
        rules_step = next((s for s in t["steps"] if s["node"] == "rules"), {})
        explained = sum(len(ex.get("channels", [])) for ex in (rules_step.get("explanations") or []))
        chk("eligibility is explained per action-channel", explained > 0, f"(explained={explained})")
    except Exception as ex:
        chk("BFF trace reachable", False, f"({ex})")
else:
    print("  (skipping correlationId-linkage checks — no activation)")

print(f"\n================ TRACE FLOW: {PASS} passed, {FAIL} failed ================")
sys.exit(1 if FAIL else 0)
