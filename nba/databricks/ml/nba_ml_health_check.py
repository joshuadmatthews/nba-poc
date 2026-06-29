#!/usr/bin/env python
"""NBA ML PIPELINE HEALTH INVARIANTS — the runtime checks that catch what unit tests can't: a stream silently down,
a job deployed with an empty config, or scores that stopped flowing. This is exactly the class of failure that took
the system down for ~62h (the external tunnel killed the datalake stream AND the scorer; the scorer also ran with an
EMPTY Kafka bootstrap and crashed every batch) with nothing alerting. Run on-demand or on a schedule.

  python nba_ml_health_check.py            # all invariants; exits non-zero if any fail

Checks (each is a real failure mode we hit):
  1. DATA BRIDGE   — silver_fact_history is fresh (the datalake Kafka->silver stream is alive).
  2. SCORING STREAM — the CQL scorer (score-rl) has an ACTIVE run (not silently canceled/dead).
  3. SCORER CONFIG  — the running scorer's Kafka bootstrap param is NON-EMPTY (the empty-bootstrap crash).
  4. FLYWHEEL       — the retrain schedule is UNPAUSED and GROUNDED (union_real=true + a window_hours scope), i.e. the
                      policy actually re-grounds on real journeys nightly instead of silently freezing on a stale model.
"""
import sys, time, json
import run_rl_train as R

SCORE_RL_JOB = <job-id>
RETRAIN_JOB = 582686942500258
DATALAKE_RUN_HINT = "datalake"
FRESH_HOURS = 2.0

PASS, FAIL = [], []
def check(name, ok, detail=""):
    (PASS if ok else FAIL).append(name)
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))

def main():
    env = R.load_env()
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs as J, workspace, compute
    w = WorkspaceClient(host=env["ML_DATABRICKS_HOST"], client_id=env["ML_DATABRICKS_CLIENT_ID"],
                        client_secret=env["ML_DATABRICKS_CLIENT_SECRET"], auth_type="oauth-m2m")

    # ---- 1. DATA BRIDGE: silver freshness (run a tiny notebook to read max eventTs) ----
    nb = ('# Databricks notebook source\nimport json, time\nfrom pyspark.sql import functions as F\n'
          'mx=spark.table("workspace.nba_poc.silver_fact_history").agg(F.max("eventTs")).collect()[0][0]\n'
          'dbutils.notebook.exit(json.dumps({"age_h": round((int(time.time()*1000)-int(mx))/3600000,2) if mx else None}))')
    try:
        w.workspace.upload("/Shared/nba_ml/_hc", nb.encode(), format=workspace.ImportFormat.SOURCE,
                           language=workspace.Language.PYTHON, overwrite=True)
        run = w.jobs.submit(run_name="hc", environments=[J.JobEnvironment(environment_key="e", spec=compute.Environment(client="4"))],
                            tasks=[J.SubmitTask(task_key="a", environment_key="e", notebook_task=J.NotebookTask(notebook_path="/Shared/nba_ml/_hc"))])
        rid = run.response.run_id; age = None
        for _ in range(45):
            r = w.jobs.get_run(rid)
            if r.state and r.state.life_cycle_state and r.state.life_cycle_state.value in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
                o = w.jobs.get_run_output(r.tasks[0].run_id)
                age = json.loads(o.notebook_output.result).get("age_h") if o.notebook_output and o.notebook_output.result else None
                break
            time.sleep(6)
        check("DATA BRIDGE: silver_fact_history is fresh (datalake stream alive)",
              age is not None and age < FRESH_HOURS, f"silver age = {age}h (bar {FRESH_HOURS}h)")
    except Exception as e:
        check("DATA BRIDGE: silver freshness", False, f"check errored: {type(e).__name__}: {str(e)[:80]}")

    # ---- 2. SCORING STREAM: the CQL scorer has an ACTIVE run ----
    active = list(w.jobs.list_runs(job_id=SCORE_RL_JOB, active_only=True))
    check("SCORING STREAM: CQL scorer (score-rl) has an active run", len(active) >= 1,
          f"{len(active)} active run(s)")

    # ---- 3. SCORER CONFIG: the scorer's Kafka bootstrap is RESOLVABLE — param OR the nba-kafka secret scope ----
    # Since the deploy-gap fix, an empty bootstrap PARAM is fine: nba_ml_common.kafka_cfg falls back to the 'nba-kafka'
    # secret scope (the durable source). So the invariant is "resolvable", not "param non-empty" — else the fix itself
    # trips a false alarm (it deliberately leaves the param empty and relies on the scope).
    boot = None
    if active:
        try:
            params = active[0].overriding_parameters
            np_ = (params.notebook_params if params else None) or {}
            boot = np_.get("bootstrap")
            if not boot:  # fall back to the job's deployed default
                j = w.jobs.get(SCORE_RL_JOB)
                boot = (j.settings.tasks[0].notebook_task.base_parameters or {}).get("bootstrap")
        except Exception as e:
            print(f"    (could not read scorer params: {type(e).__name__})")
    scope_ok = False
    try:
        scope_ok = any(s.key == "bootstrap" for s in w.secrets.list_secrets("nba-kafka"))
    except Exception as e:
        print(f"    (could not read nba-kafka scope: {type(e).__name__})")
    check("SCORER CONFIG: Kafka bootstrap resolvable (param OR nba-kafka secret scope)",
          bool(boot) or scope_ok, f"param={boot!r}, nba-kafka.bootstrap={scope_ok}")

    # ---- 4. FLYWHEEL: the retrain schedule is UNPAUSED and grounded (union_real=true + window_hours scope) ----
    try:
        g = w.jobs.get(RETRAIN_JOB).settings
        paused = (g.schedule.pause_status.value if g.schedule and g.schedule.pause_status else "?")
        tp = {t.task_key: ((t.notebook_task.base_parameters or {}) if t.notebook_task else {}) for t in (g.tasks or [])}
        union = tp.get("rl_train", {}).get("union_real")
        win = tp.get("build_journeys", {}).get("window_hours")
        grounded = (union == "true") and bool(win) and win != "0"
        check("FLYWHEEL: retrain schedule UNPAUSED + grounded (union_real=true, window_hours scope)",
              paused == "UNPAUSED" and grounded, f"schedule={paused}, union_real={union}, window_hours={win}")
    except Exception as e:
        check("FLYWHEEL: retrain schedule unpaused + grounded", False, f"check errored: {type(e).__name__}: {str(e)[:80]}")

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed" + (("  ::  " + "; ".join(FAIL)) if FAIL else ""))
    sys.exit(1 if FAIL else 0)

if __name__ == "__main__":
    main()
