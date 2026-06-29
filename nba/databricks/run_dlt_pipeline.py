#!/usr/bin/env python
"""Manage the NBA Lakeflow Declarative (DLT) pipeline — cheap by default.

The pipeline is the work-faithful medallion (bronze -> silver -> gold APPLY CHANGES CDC).
Cost control is LIFECYCLE, not architecture: it's created in TRIGGERED mode (process-and-stop,
pay per run) and left STOPPED. Flip to always-on real-time only for a live demo.

  python run_dlt_pipeline.py up                 # create/update pipeline (triggered). No compute cost.
  python run_dlt_pipeline.py run                # one triggered update (process available data, stop). Cheap.
  python run_dlt_pipeline.py run --full-refresh # triggered + reset state (rebuild gold from scratch)
  python run_dlt_pipeline.py continuous on      # switch to always-on real-time (DEMO; costs while running)
  python run_dlt_pipeline.py continuous off      # back to triggered (cheap)
  python run_dlt_pipeline.py stop               # stop a running (continuous) pipeline
  python run_dlt_pipeline.py status             # latest update state
  python run_dlt_pipeline.py delete             # delete the pipeline (DLT-managed tables go too)
"""
import os, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
NAME = "nba-medallion-dlt"
CATALOG, SCHEMA = "workspace", "nba_poc"
NB = "/Shared/nba_poc/nba_medallion_dlt"
PID_FILE = os.path.join(HERE, ".dlt_pipeline_id")
BRONZE = f"{CATALOG}.{SCHEMA}.bronze_member_activity"


def load_env():
    env = {}
    for line in open(os.path.join(HERE, "databricks.env")):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1); env[k] = v
    return env


def client():
    env = load_env()
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient(host=env["DATABRICKS_HOST"], client_id=env["DATABRICKS_CLIENT_ID"],
                           client_secret=env["DATABRICKS_CLIENT_SECRET"], auth_type="oauth-m2m")


def pid():
    return open(PID_FILE).read().strip() if os.path.exists(PID_FILE) else None


def ensure_bronze(w):
    # bronze is the IN job's append target (NOT DLT-managed) — just make sure it exists.
    # IMPORTANT: never DROP silver/gold here. They are DLT-managed; dropping them out-of-band
    # corrupts APPLY CHANGES incremental state ("Staging Table does not exist"). Use `reset` for a
    # clean slate (one-time), or `run --full-refresh` to rebuild within DLT.
    wh = next(iter(w.warehouses.list()))
    w.statement_execution.execute_statement(warehouse_id=wh.id, wait_timeout="50s", statement=(
        f"CREATE TABLE IF NOT EXISTS {BRONZE} (memberId STRING, daysSinceLogin BIGINT, "
        f"completedTasks BIGINT, viewedDashboard BOOLEAN, usedChat BOOLEAN, isDNC BOOLEAN, "
        f"eventTs BIGINT, ingestTs TIMESTAMP) USING DELTA"))


def reset_tables(w):
    # one-time: drop DLT-managed silver/gold so the pipeline can recreate them cleanly
    wh = next(iter(w.warehouses.list()))
    for t in ("silver_member_activity", "gold_facts"):
        w.statement_execution.execute_statement(warehouse_id=wh.id, wait_timeout="50s",
            statement=f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{t}")
    print("dropped silver_member_activity + gold_facts (reset)")


def upload(w):
    from databricks.sdk.service import workspace
    w.workspace.mkdirs("/Shared/nba_poc")
    with open(os.path.join(HERE, "nba_medallion_dlt.py"), "rb") as f:
        w.workspace.upload(NB, f.read(), format=workspace.ImportFormat.SOURCE,
                           language=workspace.Language.PYTHON, overwrite=True)


def up(w, continuous=False):
    from databricks.sdk.service import pipelines
    libs = [pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=NB))]
    cfg = {"nba.bronze": BRONZE}
    args = dict(name=NAME, serverless=True, catalog=CATALOG, schema=SCHEMA,
                continuous=continuous, development=False, libraries=libs, configuration=cfg)
    existing = pid()
    if existing:
        w.pipelines.update(pipeline_id=existing, **args)
        print(f"updated pipeline {existing} (continuous={continuous})"); return existing
    p = w.pipelines.create(**args)
    open(PID_FILE, "w").write(p.pipeline_id)
    print(f"created pipeline {p.pipeline_id} (continuous={continuous})"); return p.pipeline_id


def run(w, full_refresh=False):
    p = pid() or up(w)
    u = w.pipelines.start_update(pipeline_id=p, full_refresh=full_refresh)
    print(f"triggered update {u.update_id} (full_refresh={full_refresh}) — polling...")
    for _ in range(60):
        upd = w.pipelines.get_update(pipeline_id=p, update_id=u.update_id).update
        st = upd.state.value if upd and upd.state else "?"
        print("  ", st)
        if st in ("COMPLETED", "FAILED", "CANCELED"):
            return st
        time.sleep(10)
    return "TIMEOUT"


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    w = client()
    if cmd == "up":
        ensure_bronze(w); upload(w); up(w)
    elif cmd == "run":
        ensure_bronze(w); upload(w); up(w)
        print("result:", run(w, full_refresh="--full-refresh" in sys.argv))
    elif cmd == "reset":
        reset_tables(w); ensure_bronze(w); upload(w); up(w)
        print("result:", run(w, full_refresh=True))
    elif cmd == "continuous":
        on = len(sys.argv) > 2 and sys.argv[2] == "on"
        upload(w); up(w, continuous=on)
        if on:
            p = pid(); w.pipelines.start_update(pipeline_id=p)
            print("continuous mode ON — pipeline is now always-on real-time (costs while running). stop with: stop")
    elif cmd == "stop":
        w.pipelines.stop(pipeline_id=pid()); print("stop requested")
    elif cmd == "status":
        p = pid()
        if not p: print("no pipeline (.dlt_pipeline_id missing) — run: up"); return
        g = w.pipelines.get(pipeline_id=p)
        print(f"{g.name} state={g.state.value if g.state else '?'} continuous={g.spec.continuous if g.spec else '?'}")
    elif cmd == "delete":
        w.pipelines.delete(pipeline_id=pid()); os.remove(PID_FILE); print("deleted")
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
