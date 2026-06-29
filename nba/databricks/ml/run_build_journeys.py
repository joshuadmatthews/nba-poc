#!/usr/bin/env python
"""Reconstruct REAL member journeys from the lake into journey_transitions (the data flywheel's input) on Databricks
serverless — the build_journey_set counterpart to run_rl_train.py. Uploads nba_ml_build_journey_set + its %run siblings
to /Shared/nba_ml, submits the reconstruction, polls, prints {transitions, members}. rl_train then unions the table.

  python run_build_journeys.py [window_hours]   # 0 = all history; >0 = only the last N hours (realistic-mode window)
"""
import os, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
REMOTE_DIR = "/Shared/nba_ml"
# build_journey_set %runs these three siblings (./nba_ml_common, ./nba_journey_env, ./nba_ml_journey) — upload all.
FILES = ["nba_ml_common.py", "nba_journey_env.py", "nba_ml_journey.py", "nba_ml_catalog.py", "nba_ml_build_journey_set.py"]


def load_env():
    env = {}
    for line in open(os.path.join(HERE, "ml.env")):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1); env[k] = v
    return env


def main():
    window_hours = sys.argv[1] if len(sys.argv) > 1 else "0"
    env = load_env()
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs, workspace, compute
    w = WorkspaceClient(host=env["ML_DATABRICKS_HOST"], client_id=env["ML_DATABRICKS_CLIENT_ID"],
                        client_secret=env["ML_DATABRICKS_CLIENT_SECRET"], auth_type="oauth-m2m")
    w.workspace.mkdirs(REMOTE_DIR)
    for fn in FILES:
        remote = f"{REMOTE_DIR}/{fn[:-3]}"
        with open(os.path.join(HERE, fn), "rb") as f:
            w.workspace.upload(remote, f.read(), format=workspace.ImportFormat.SOURCE,
                               language=workspace.Language.PYTHON, overwrite=True)
        print(f"uploaded {fn} -> {remote}")
    params = {"ml_catalog": "ml_worspace", "ml_schema": "core", "window_hours": window_hours}
    run = w.jobs.submit(run_name="nba-ml-build-journeys",
        environments=[jobs.JobEnvironment(environment_key="e", spec=compute.Environment(client="4"))],
        tasks=[jobs.SubmitTask(task_key="build_journeys", environment_key="e",
            notebook_task=jobs.NotebookTask(notebook_path=f"{REMOTE_DIR}/nba_ml_build_journey_set", base_parameters=params))])
    rid = run.response.run_id
    print(f"submitted run {rid} (window_hours={window_hours}) — polling...")
    for _ in range(180):
        r = w.jobs.get_run(rid)
        life = r.state.life_cycle_state.value if r.state and r.state.life_cycle_state else "?"
        if life in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            res = r.state.result_state.value if r.state.result_state else "?"
            out = w.jobs.get_run_output(r.tasks[0].run_id)
            print("RESULT:", res)
            print("OUTPUT:", out.notebook_output.result if out.notebook_output else (out.error or out.logs))
            return
        time.sleep(10)
    print(f"timeout (run {rid} still going) — check the workspace UI")


if __name__ == "__main__":
    main()
