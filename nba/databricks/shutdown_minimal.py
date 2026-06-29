#!/usr/bin/env python3
"""Take the Databricks workspace to a minimal-cost state: stop the SQL warehouse, terminate running clusters,
and cancel any active job runs (the datalake streaming job). Auth = OAuth M2M from databricks.env. Idempotent.
Run again any time. Does NOT delete anything — just stops compute so nothing bills while idle."""
import base64, json, os, urllib.request, urllib.parse, urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
env = {}
with open(os.path.join(HERE, "databricks.env")) as f:
    for line in f:
        m = line.strip()
        if "=" in m and not m.startswith("#"):
            k, v = m.split("=", 1); env[k.strip()] = v.strip()

HOST = env["DATABRICKS_HOST"].rstrip("/")
if not HOST.startswith("http"):
    HOST = "https://" + HOST


def api(method, path, body=None, token=None, form=False, auth=None):
    url = HOST + path
    data, headers = None, {}
    if auth:
        headers["Authorization"] = "Basic " + base64.b64encode(auth.encode()).decode()
    if token:
        headers["Authorization"] = "Bearer " + token
    if body is not None:
        if form:
            data = urllib.parse.urlencode(body).encode(); headers["Content-Type"] = "application/x-www-form-urlencoded"
        else:
            data = json.dumps(body).encode(); headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            t = r.read().decode(); return json.loads(t) if t else {}
    except urllib.error.HTTPError as e:
        return {"_error": e.code, "_body": e.read().decode()[:200]}


tok = api("POST", "/oidc/v1/token", {"grant_type": "client_credentials", "scope": "all-apis"},
          form=True, auth=f"{env['DATABRICKS_CLIENT_ID']}:{env['DATABRICKS_CLIENT_SECRET']}").get("access_token")
if not tok:
    print("FAILED to get OAuth token — check databricks.env"); raise SystemExit(1)
print("authenticated.")

# 1) SQL warehouses -> stop any that aren't already stopped
wh = api("GET", "/api/2.0/sql/warehouses", token=tok).get("warehouses", [])
for w in wh:
    if w.get("state") not in ("STOPPED", "STOPPING", "DELETED"):
        r = api("POST", f"/api/2.0/sql/warehouses/{w['id']}/stop", {}, token=tok)
        print(f"  warehouse {w.get('name')} ({w['id']}) {w.get('state')} -> stop {'OK' if '_error' not in r else r}")
    else:
        print(f"  warehouse {w.get('name')} already {w.get('state')}")

# 2) active job runs -> cancel (the datalake streaming job holds a cluster while running)
runs = api("GET", "/api/2.1/jobs/runs/list?active_only=true&limit=25", token=tok).get("runs", [])
for run in runs:
    r = api("POST", "/api/2.1/jobs/runs/cancel", {"run_id": run["run_id"]}, token=tok)
    print(f"  job run {run['run_id']} ({run.get('run_name','')}) -> cancel {'OK' if '_error' not in r else r}")
if not runs:
    print("  no active job runs.")

# 3) all-purpose clusters -> terminate any that are running/pending
cl = api("GET", "/api/2.1/clusters/list", token=tok).get("clusters", [])
live = [c for c in cl if c.get("state") in ("RUNNING", "PENDING", "RESTARTING", "RESIZING")]
for c in live:
    r = api("POST", "/api/2.1/clusters/delete", {"cluster_id": c["cluster_id"]}, token=tok)   # 'delete' = terminate, not remove
    print(f"  cluster {c.get('cluster_name')} ({c['cluster_id']}) {c['state']} -> terminate {'OK' if '_error' not in r else r}")
if not live:
    print("  no running clusters.")

print("done — Databricks compute is idle (definitions/tables remain; nothing bills while stopped).")
