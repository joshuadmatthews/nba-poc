#!/usr/bin/env python
"""Run the NBA Databricks-serverless jobs (NATIVE Kafka, SASL/SCRAM over the external tunnel).

  python run_kafka_jobs.py datalake [false|true] [availableNow|continuous]
        # streaming ingest -> silver/gold. continuous = LIVE: the EVENT path emits throttle+comms counts
        # as sends stream in. availableNow = drain + stop (backfill); true = reset+rebuild from earliest.
  python run_kafka_jobs.py out                          # gold CDF -> re-emit facts (origin=lake)
  python run_kafka_jobs.py reconcile                    # re-emit newly action-referenced facts for all members
  python run_kafka_jobs.py throttle [continuous|triggered] [window_s]   # full channel-throttle recompute + emit
  python run_kafka_jobs.py comms    [continuous|triggered] [window_s]   # full per-member comms recompute + emit
  python run_kafka_jobs.py rollover # create/update the daily-midnight Databricks job that does the throttle+comms
        # BOUNDARY rollover (daily reset / week roll) — what the event-path stream can't do on the clock alone.

Counts are MOSTLY EVENT-driven (folded into the datalake stream's foreachBatch); throttle/comms as standalone
jobs are the full-recompute used by `rollover` (and handy for a one-off). continuous/streaming runs submit and
return the run id (they run until killed); triggered runs wait for the result.
"""
import os, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
CATALOG, SCHEMA = "workspace", "nba_poc"
JOBS = {"out": ("nba_out_produce.py", "/Shared/nba_poc/nba_out_produce"),
        "datalake": ("nba_datalake_stream.py", "/Shared/nba_poc/nba_datalake_stream"),
        "reconcile": ("nba_fact_reconcile.py", "/Shared/nba_poc/nba_fact_reconcile"),
        "throttle": ("nba_throttle_emit.py", "/Shared/nba_poc/nba_throttle_emit"),
        "comms": ("nba_comms_count.py", "/Shared/nba_poc/nba_comms_count")}
# Every NBA job now speaks NATIVE Kafka (SASL/SCRAM over the external TCP tunnel) — headers + Structured
# Streaming, no pandaproxy. datalake also takes an optional reset arg (true|false) for a clean rebuild.
# `comms` owns per-member weekly comms-frequency counting from the transactional lake (it replaces the
# retired in-pod ais-nba-datalake Redis counter); like `throttle` it takes mode=continuous|triggered.
NATIVE = {"datalake", "out", "throttle", "reconcile", "comms"}


def load_env():
    env = {}
    for line in open(os.path.join(HERE, "databricks.env")):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1); env[k] = v
    return env


def schedule_rollover(w, jobs, workspace, base):
    """Create/update the BOUNDARY ROLLOVER job: a daily-at-midnight Databricks schedule that does a FULL
    recompute + emit of throttle + comms. The live counts come from the datalake stream's EVENT path (a send
    bumps the count instantly); this scheduled job exists only for what a data-driven stream can't do —
    reset the daily throttle to 0 at midnight and roll the comms week over, even when no new send arrives."""
    for key in ("throttle", "comms"):
        local, remote = JOBS[key]
        with open(os.path.join(HERE, local), "rb") as f:
            w.workspace.upload(remote, f.read(), format=workspace.ImportFormat.SOURCE,
                               language=workspace.Language.PYTHON, overwrite=True)
    tasks = [
        jobs.Task(task_key="throttle", notebook_task=jobs.NotebookTask(
            notebook_path=JOBS["throttle"][1], base_parameters={**base, "mode": "triggered", "window_seconds": "300"})),
        jobs.Task(task_key="comms", notebook_task=jobs.NotebookTask(
            notebook_path=JOBS["comms"][1], base_parameters={**base, "mode": "triggered", "window_seconds": str(7 * 24 * 3600)})),
    ]
    sched = jobs.CronSchedule(quartz_cron_expression="0 5 0 * * ?", timezone_id="UTC")   # 00:05 UTC daily
    name = "nba-kafka-rollover"
    existing = [j for j in w.jobs.list() if j.settings and j.settings.name == name]
    if existing:
        w.jobs.reset(job_id=existing[0].job_id, new_settings=jobs.JobSettings(name=name, tasks=tasks, schedule=sched))
        print(f"updated rollover job {existing[0].job_id} — daily 00:05 UTC: throttle + comms full recompute")
    else:
        j = w.jobs.create(name=name, tasks=tasks, schedule=sched)
        print(f"created rollover job {j.job_id} — daily 00:05 UTC: throttle + comms full recompute")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd not in JOBS and cmd != "rollover":
        print(__doc__); return
    env = load_env()
    boot = env.get("KAFKA_BOOTSTRAP_EXTERNAL", ""); su = env.get("KAFKA_SASL_USER", ""); sp = env.get("KAFKA_SASL_PASS", "")
    if not (boot and su and sp):
        print("KAFKA_BOOTSTRAP_EXTERNAL / KAFKA_SASL_USER / KAFKA_SASL_PASS missing from databricks.env"); return
    mode = sys.argv[2] if len(sys.argv) > 2 else "triggered"   # throttle/comms: continuous|triggered
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs, workspace
    w = WorkspaceClient(host=env["DATABRICKS_HOST"], client_id=env["DATABRICKS_CLIENT_ID"],
                        client_secret=env["DATABRICKS_CLIENT_SECRET"], auth_type="oauth-m2m")
    w.workspace.mkdirs("/Shared/nba_poc")
    base = {"catalog": CATALOG, "schema": SCHEMA, "bootstrap": boot, "sasl_user": su, "sasl_pass": sp}
    if cmd == "rollover":
        schedule_rollover(w, jobs, workspace, base); return
    local, remote = JOBS[cmd]
    with open(os.path.join(HERE, local), "rb") as f:
        w.workspace.upload(remote, f.read(), format=workspace.ImportFormat.SOURCE,
                           language=workspace.Language.PYTHON, overwrite=True)
    params = dict(base)
    if cmd == "datalake":
        params["reset"] = sys.argv[2] if (len(sys.argv) > 2 and sys.argv[2] in ("true", "false")) else "false"
        # availableNow (drain + stop) | continuous (live stream — the event path emits counts as sends arrive)
        params["trigger"] = sys.argv[3] if (len(sys.argv) > 3 and sys.argv[3] in ("availableNow", "continuous")) else "availableNow"
    if cmd == "throttle":
        params["mode"] = mode
        params["window_seconds"] = sys.argv[3] if len(sys.argv) > 3 else "300"   # rate window (5m default)
    if cmd == "comms":
        params["mode"] = mode
        params["window_seconds"] = sys.argv[3] if len(sys.argv) > 3 else str(7 * 24 * 3600)   # rolling week default
    run = w.jobs.submit(run_name=f"nba-kafka-{cmd}", tasks=[jobs.SubmitTask(
        task_key=cmd, notebook_task=jobs.NotebookTask(notebook_path=remote, base_parameters=params))])
    rid = run.response.run_id
    # A long-running loop / live stream never terminates within a poll window — submit and return the id.
    cont = (cmd in ("throttle", "comms") and mode == "continuous") or (cmd == "datalake" and params.get("trigger") == "continuous")
    if cont:
        print(f"submitted nba-kafka-{cmd} run {rid} — runs until killed ({'live stream' if cmd == 'datalake' else 'continuous loop'})")
        return
    print(f"submitted nba-kafka-{cmd} run {rid} — polling...")
    for _ in range(40):
        r = w.jobs.get_run(rid)
        life = r.state.life_cycle_state.value if r.state and r.state.life_cycle_state else "?"
        if life in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            res = r.state.result_state.value if r.state.result_state else "?"
            out = w.jobs.get_run_output(r.tasks[0].run_id)
            print("result:", res, "->", out.notebook_output.result if out.notebook_output else out.logs)
            return
        time.sleep(8)
    print("timeout")


if __name__ == "__main__":
    main()
