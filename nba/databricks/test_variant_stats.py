#!/usr/bin/env python
"""Content-VARIANT A/B stats test — proves the LAKE attributes dispositions to the content key the
member actually got, so the Command Center's Variants view can crown a winner.

  python test_variant_stats.py     # exits non-zero on failure

Every disposition fact now carries `contentKey` (the variant that was sent). This produces a KNOWN set
of dispositions for ONE action-channel (email) split across TWO variants — tmpl.A and tmpl.B — at
distinct funnel depths, ingests them, and asserts:
  - per (contentKey, value) counts match exactly (the A/B funnel data),
  - the deepest-stage conversion per variant is what we produced,
  - tmpl.B wins (deeper engagement) — the same winner the BFF variantPerformance resolver picks.
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
ACTION = f"variant_{RUN}"
CH = "email"
ORDER = ["Delivered", "Read", "LinkClicked"]   # email engagement funnel (deepest = LinkClicked)
# (contentKey, status) -> count. Same action-channel, two competing variants at different funnel depths.
PLAN = {
    ("tmpl.A", "sent"):        5,
    ("tmpl.A", "Delivered"):   3,
    ("tmpl.A", "Read"):        1,
    ("tmpl.B", "sent"):        5,
    ("tmpl.B", "Delivered"):   4,
    ("tmpl.B", "Read"):        3,
    ("tmpl.B", "LinkClicked"): 2,
}

print(f"== producing per-variant disposition facts (run {RUN}) ==")
ids = []
ts = int(time.time() * 1000)
i = 0
for (ck, status), n in PLAN.items():
    for _ in range(n):
        i += 1; ts += 1
        mid = f"var_{RUN}_{i}"
        ids.append(mid)
        fact = {"entityType": "OPERATOR", "entityId": mid,
                "key": f"nba.disposition.{ACTION}.{CH}",
                "value": status, "valueType": "STRING", "eventTs": ts, "source": "test",
                "contentKey": ck}        # <-- the variant attribution
        key = f"OPERATOR:{mid}:{fact['key']}"
        produce("nba.facts", key, json.dumps(fact))
        produce("nba.member.facts", key, json.dumps(fact))
print(f"  produced {len(ids)} disposition facts across {len(PLAN)} (contentKey,status) buckets")
time.sleep(2)

print("== ingesting to the lake ==")
out = subprocess.run([sys.executable, os.path.join(HERE, "run_kafka_jobs.py"), "datalake"], capture_output=True, text=True, cwd=HERE)
print("  " + (out.stdout.strip().splitlines() or ["(no output)"])[-1])
time.sleep(3)

print("== assert per-(contentKey,value) A/B counts match what was produced ==")
idlist = ",".join(f"'{m}'" for m in ids)
rows = q(f"""SELECT contentKey, value, count(distinct entityId, actionId, channel, eventTs) AS n
             FROM {NS}.silver_fact_history
             WHERE factClass='disposition' AND contentKey IS NOT NULL AND entityId IN ({idlist})
             GROUP BY contentKey, value""")
got = {(r[0], r[1]): int(r[2]) for r in rows}
for (ck, status), expected in PLAN.items():
    chk(f"{ck} {status} = {expected}", got.get((ck, status), 0) == expected, f"(got {got.get((ck, status), 0)})")
chk("no unexpected (contentKey,value) buckets for this run", set(got.keys()) == set(PLAN.keys()), f"(got {sorted(got.keys())})")

print("== assert the winner is tmpl.B (deeper conversion), mirroring the BFF resolver ==")
def deepest_conv(ck):
    sent = got.get((ck, "sent"), 0)
    deep_n = 0
    for s in ORDER:
        if got.get((ck, s), 0): deep_n = got[(ck, s)]
    return (deep_n / sent) if sent else 0
ca, cb = deepest_conv("tmpl.A"), deepest_conv("tmpl.B")
chk(f"tmpl.A conversion = 20% (Read 1 / sent 5)", abs(ca - 0.20) < 1e-9, f"(got {ca:.2f})")
chk(f"tmpl.B conversion = 40% (LinkClicked 2 / sent 5)", abs(cb - 0.40) < 1e-9, f"(got {cb:.2f})")
chk("tmpl.B wins the A/B (higher deepest-stage conversion)", cb > ca, f"(A={ca:.2f} B={cb:.2f})")

print(f"\n================ VARIANT STATS: {PASS} passed, {FAIL} failed ================")
sys.exit(1 if FAIL else 0)
