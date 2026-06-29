#!/usr/bin/env python
"""Upload + run the PRODUCTION RL training on Databricks serverless (no d3rlpy, no cluster).
Uploads nba_journey_env + nba_ml_cql_torch + nba_ml_rl_train to one /Shared dir (so the `%run` siblings resolve),
submits nba_ml_rl_train, polls, prints the OPE result. This is the "one smooth Databricks process" path-check.

  python run_rl_train.py [cql_steps] [out_name]      # default 80000 rl_qnet.json
"""
import os, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
REMOTE_DIR = "/Shared/nba_ml"
FILES = ["nba_journey_env.py", "nba_ml_cql_numpy.py", "nba_ml_catalog.py", "nba_ml_rl_train.py"]


def load_env():
    env = {}
    for line in open(os.path.join(HERE, "ml.env")):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1); env[k] = v
    return env


def main():
    cql_steps = sys.argv[1] if len(sys.argv) > 1 else "80000"
    out_name = sys.argv[2] if len(sys.argv) > 2 else "rl_qnet.json"
    union_real = sys.argv[3] if len(sys.argv) > 3 else "true"
    ground_champion = sys.argv[4] if len(sys.argv) > 4 else "true"
    prior_strength = sys.argv[5] if len(sys.argv) > 5 else "0"   # sim-as-decaying-prior: cap sim to tau pseudo-obs
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
    params = {"ml_catalog": "ml_worspace", "ml_schema": "core", "n_episodes": "30000",
              "cql_steps": cql_steps, "alpha": "0.1", "out_name": out_name, "union_real": union_real,
              "ground_champion": ground_champion, "prior_strength": prior_strength}
    # serverless ENVIRONMENT VERSION 4 has mlflow 2.22 + scikit-learn in the base (needed to load propensity@champion
    # as the RL response head). NO extra dependencies -> no pip/index install. Lower versions lack mlflow.
    run = w.jobs.submit(run_name="nba-ml-rl-train",
        environments=[jobs.JobEnvironment(environment_key="e", spec=compute.Environment(client="4"))],
        tasks=[jobs.SubmitTask(task_key="rl_train", environment_key="e",
            notebook_task=jobs.NotebookTask(notebook_path=f"{REMOTE_DIR}/nba_ml_rl_train", base_parameters=params))])
    rid = run.response.run_id
    print(f"submitted run {rid} (cql_steps={cql_steps}, out={out_name}) — polling...")
    for _ in range(220):
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
