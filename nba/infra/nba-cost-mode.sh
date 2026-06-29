#!/usr/bin/env bash
# NBA Databricks COST MODE — park every continuous/scheduled cloud job to stop serverless billing, REVERSIBLY.
#
#   off    : cancel every active run (the continuous bridges) + PAUSE every unpaused schedule + stop the SQL
#            warehouse.  Keeps ALL data + checkpoints — nothing is wiped, so `on` resumes exactly where it left off.
#   on     : unpause the schedules + restart the continuous bridges (resume from checkpoints, NO reset).
#   status : show what is active / paused, in both workspaces.
#
# The local NBA loop (podman containers) is FREE (runs on your machine) and is left untouched — the UI keeps
# reading the existing gold/silver data. To force a little movement while parked WITHOUT a full resume, run one
# bridge by hand, e.g.:  databricks jobs run-now --json '{"job_id":<job-id>}'   (source-sim).
#
# NOT a cost while parked (so this script leaves them alone):
#   - SQL warehouse: auto-starts on any UI/BFF query (analytics, trace, member snapshot), cold-starts ~20s,
#     auto-stops after 5 min idle. Analytics tabs still work in cost mode.
#   - Model Serving endpoints (nba-cql, nba-propensity): already scale-to-zero -> $0 when not queried.
#
# !!! WHILE PARKED, PAUSE THE 20-MIN HEALTH-CHECK AUTOMATION !!!
#   It runs nba-health-probe.sh (which queries the warehouse, spinning it every 20 min) AND its instructions say
#   to restart any DOWN/stale job — which would UN-PARK everything you just turned off. Turn the cron back on with `on`.
set +e
REPO="197121cd "197121dirname "-e")/../.." 2>/dev/null && pwd)"; cd "$REPO" 2>/dev/null
WH=<warehouse-id>

# continuous bridges (run 24/7 via run-now) — the main serverless cost
SIM=<job-id>; OUTP=<job-id>; DI=<job-id>   # LAKE
SCORERL=<job-id>                                          # ML
# scheduled jobs that auto-trigger (must be paused so they don't re-bill)
RETRAINLOOP=<job-id>   # ML, hourly
RLRETRAIN=582686942500258     # ML, daily 02:00
SCOREB=<job-id>       # ML, score-batch (cancel if mid-run)
FACTREC=<job-id>       # LAKE, fact-reconcile (cancel if mid-run)

lake_auth(){ unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET DATABRICKS_TOKEN DATABRICKS_HOST; set -a; source nba/databricks/databricks.env 2>/dev/null; set +a; }
ml_auth(){ unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET DATABRICKS_TOKEN DATABRICKS_HOST; set -a; source nba/databricks/ml/ml.env 2>/dev/null; set +a; export DATABRICKS_HOST="$ML_DATABRICKS_HOST" DATABRICKS_CLIENT_ID="$ML_DATABRICKS_CLIENT_ID" DATABRICKS_CLIENT_SECRET="$ML_DATABRICKS_CLIENT_SECRET"; }
cancelall(){ MSYS_NO_PATHCONV=1 timeout 30 databricks api post /api/2.1/jobs/runs/cancel-all --json "{\"job_id\":$1}" >/dev/null 2>&1 && echo "    cancel-all $1"; }
runnow(){ MSYS_NO_PATHCONV=1 timeout 30 databricks jobs run-now --json "{\"job_id\":$1}" --no-wait -o json 2>/dev/null | python -c "import sys,json;print('    run-now',$1,'->',json.load(sys.stdin).get('run_id',''))" 2>/dev/null; }
runnow_cont(){ MSYS_NO_PATHCONV=1 timeout 30 databricks jobs run-now --json "{\"job_id\":$1,\"notebook_params\":{\"trigger\":\"continuous\"}}" --no-wait -o json 2>/dev/null | python -c "import sys,json;print('    run-now(cont)',$1,'->',json.load(sys.stdin).get('run_id',''))" 2>/dev/null; }
# flip a job's schedule pause_status (partial jobs/update — keeps cron + tz, preserves all other settings)
set_pause(){
  local sched
  sched=$(MSYS_NO_PATHCONV=1 timeout 30 databricks api get "/api/2.1/jobs/get?job_id=$1" 2>/dev/null | python -c "
import sys,json
try: s=json.load(sys.stdin).get('settings',{}).get('schedule')
except Exception: s=None
print(json.dumps({'quartz_cron_expression':s['quartz_cron_expression'],'timezone_id':s.get('timezone_id','UTC'),'pause_status':'$2'}) if s else '')")
  [ -z "$sched" ] && { echo "    job $1: no schedule"; return; }
  MSYS_NO_PATHCONV=1 timeout 30 databricks api post /api/2.1/jobs/update --json "{\"job_id\":$1,\"new_settings\":{\"schedule\":$sched}}" >/dev/null 2>&1 && echo "    job $1 schedule -> $2"
}
wh_stop(){ MSYS_NO_PATHCONV=1 timeout 40 databricks api post "/api/2.0/sql/warehouses/$WH/stop" >/dev/null 2>&1 && echo "    SQL warehouse $WH -> stop requested (auto-restarts on next UI query)"; }
active(){ MSYS_NO_PATHCONV=1 timeout 30 databricks api get "/api/2.1/jobs/runs/list?active_only=true&limit=50" 2>/dev/null | python -c "
import sys,json
try: d=json.load(sys.stdin)
except Exception: print('    (could not read active runs)'); sys.exit()
r=d.get('runs',[]); print('    active runs:',len(r))
for x in r: print('     ',x.get('job_id'),(x.get('run_name') or '')[:32])"; }

case "${1:-status}" in
  off)
    echo "### COST MODE: OFF — parking all cloud jobs (data + checkpoints kept)"
    echo "-- LAKE: cancel continuous bridges --"; lake_auth; for j in $SIM $OUTP $DI $FACTREC; do cancelall $j; done
    echo "-- ML: cancel continuous + score-batch + in-flight retrains --"; ml_auth; for j in $SCORERL $SCOREB $RETRAINLOOP $RLRETRAIN; do cancelall $j; done
    echo "-- ML: pause schedules --"; for j in $RETRAINLOOP $RLRETRAIN; do set_pause $j PAUSED; done
    echo "-- SQL warehouse --"; lake_auth; wh_stop
    echo "-- verify (expect 0 active) --"; echo "  LAKE:"; lake_auth; active; echo "  ML:"; ml_auth; active
    echo "### parked. resume anytime with:  bash $0 on";;
  on)
    echo "### COST MODE: ON — resuming bridges from existing checkpoints (NO reset, data kept)"
    echo "-- ML: unpause schedules --"; ml_auth; for j in $RETRAINLOOP $RLRETRAIN; do set_pause $j UNPAUSED; done
    echo "-- LAKE: restart bridges (staged so they don't stampede the tunnel) --"; lake_auth
    runnow_cont $DI;   sleep 22
    runnow $OUTP;      sleep 22
    runnow $SIM
    echo "-- ML: restart score-rl --"; ml_auth; runnow $SCORERL
    echo "### resumed. give the medallion ~3-5 min, then: bash nba/infra/nba-health-probe.sh";;
  status)
    echo "### COST MODE STATUS"
    echo "  LAKE:"; lake_auth; active
    echo "  ML:";   ml_auth;   active;;
  *) echo "usage: bash $0 <off|on|status>";;
esac
