#!/usr/bin/env python3
"""Read-only inspection of the ML workspace + the lake workspace (no compute, no cost). Checks: can the SP from
databricks.env authenticate to the ML workspace; do both share a metastore (=> cross-workspace UC works); where
the lake's gold tables live; and what warehouses exist. Run any time."""
import base64, json, os, urllib.request, urllib.parse, urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
env = {}
with open(os.path.join(HERE, "..", "databricks.env")) as f:
    for line in f:
        m = line.strip()
        if "=" in m and not m.startswith("#"):
            k, v = m.split("=", 1); env[k.strip()] = v.strip()

LAKE_CID, LAKE_SEC = env["DATABRICKS_CLIENT_ID"], env["DATABRICKS_CLIENT_SECRET"]
LAKE_HOST = env["DATABRICKS_HOST"].rstrip("/")
LAKE_HOST = LAKE_HOST if LAKE_HOST.startswith("http") else "https://" + LAKE_HOST

# ML workspace creds from the gitignored ml.env (its own SP)
mle = {}
mlp = os.path.join(HERE, "ml.env")
if os.path.exists(mlp):
    for line in open(mlp):
        s = line.strip()
        if "=" in s and not s.startswith("#"):
            k, v = s.split("=", 1); mle[k.strip()] = v.strip()
ML_HOST = mle.get("ML_DATABRICKS_HOST", "https://<your-databricks-workspace>.cloud.databricks.com").rstrip("/")
ML_CID, ML_SEC = mle.get("ML_DATABRICKS_CLIENT_ID", ""), mle.get("ML_DATABRICKS_CLIENT_SECRET", "")


def token(host, cid, sec):
    d = urllib.parse.urlencode({"grant_type": "client_credentials", "scope": "all-apis"}).encode()
    req = urllib.request.Request(host + "/oidc/v1/token", data=d, method="POST", headers={
        "Authorization": "Basic " + base64.b64encode(f"{cid}:{sec}".encode()).decode(),
        "Content-Type": "application/x-www-form-urlencoded"})
    return json.load(urllib.request.urlopen(req, timeout=30))["access_token"]


def api(host, tok, path):
    req = urllib.request.Request(host + path, headers={"Authorization": "Bearer " + tok})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        return {"_error": e.code, "_body": e.read().decode()[:160]}


def inspect(label, host, cid, sec):
    print(f"\n===== {label}: {host} =====")
    try:
        tok = token(host, cid, sec)
        print("  auth: OK (SP can get a token here)")
    except Exception as e:
        print(f"  auth: FAILED — {e}\n  -> the SP in databricks.env is not provisioned on this workspace.")
        return None, None
    ms = api(host, tok, "/api/2.1/unity-catalog/metastore_summary")
    msid = ms.get("metastore_id", ms.get("_error", "?"))
    print(f"  metastore_id: {msid}  ({ms.get('name', '')})")
    cats = api(host, tok, "/api/2.1/unity-catalog/catalogs")
    names = [c.get("name") for c in cats.get("catalogs", [])] if "catalogs" in cats else cats
    print(f"  catalogs: {names}")
    wh = api(host, tok, "/api/2.0/sql/warehouses")
    print(f"  warehouses: {[(w.get('name'), w.get('state')) for w in wh.get('warehouses', [])] if 'warehouses' in wh else wh}")
    return tok, (msid, names)


lt, linfo = inspect("LAKE", LAKE_HOST, LAKE_CID, LAKE_SEC)
mt, minfo = inspect("ML", ML_HOST, ML_CID, ML_SEC)

# where do the lake gold tables live? probe a few likely full names on the lake host.
if lt:
    print("\n===== lake table locations =====")
    for cat in (linfo[1] if linfo and isinstance(linfo[1], list) else ["workspace", "hive_metastore"]):
        t = api(LAKE_HOST, lt, f"/api/2.1/unity-catalog/tables/{cat}.nba_poc.gold_member_snapshot")
        if "_error" not in t:
            print(f"  FOUND: {cat}.nba_poc.gold_member_snapshot  (owner={t.get('owner')})")

# can the ML workspace SEE the lake data? (same metastore => yes; else Delta Sharing needed)
print("\n===== cross-workspace verdict =====")
if lt and mt:
    same = linfo[0] == minfo[0]
    print(f"  same metastore: {same}  (lake={linfo[0]}  ml={minfo[0]})")
    if same:
        print("  -> ML workspace can read the lake's UC tables directly. Set src_catalog to the catalog above.")
        for cat in (minfo[1] if isinstance(minfo[1], list) else []):
            t = api(ML_HOST, mt, f"/api/2.1/unity-catalog/tables/{cat}.nba_poc.gold_member_snapshot")
            if "_error" not in t:
                print(f"  ML can already read: {cat}.nba_poc.gold_member_snapshot")
    else:
        print("  -> DIFFERENT metastores: need Delta Sharing (or run ML in the lake workspace). ")
