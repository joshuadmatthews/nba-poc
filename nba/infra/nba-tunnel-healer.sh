#!/usr/bin/env bash
# NBA tunnel HEALER — the durable answer to "the external reconnect breaks the whole data stream every time."
#
# ROOT CAUSE (see project_nba_external_tunnel_pileup): the cloud medallion jobs all cross ONE external tunnel. It
# reconnects ~hourly (network blips, not a hard TTL). Each reconnect drops every Spark connection at once; the
# jobs' own self-heal usually resumes, but when stale half-open connections pile up they starve the (Pro) budget
# and jobs fail "Failed to create new KafkaAdminClient" — the tunnel then LOOKS wedged while local clients work.
#
# This watches the tunnel log for a reconnect, waits for it to settle, checks whether the loop actually FROZE
# (member.facts HWM stops advancing), and only then restarts the cloud jobs STAGED (dependency order, STAGGER
# apart, resume-from-checkpoint — never reset) so fresh connections come up without stampeding. A reconnect goes
# from a 6h outage to a ~2min self-heal. Modes:  watch (default, run in background) | heal (one-shot staged restart).
set +e
REPO="197121cd "197121dirname "-e")/../.." 2>/dev/null && pwd)"; cd "$REPO" 2>/dev/null
TUN=ais-nba-kafka-tunnel; PANDA=ais-nba-redpanda; STAGGER=${STAGGER:-25}; SETTLE=${SETTLE:-20}; FREEZE_WAIT=${FREEZE_WAIT:-40}
PX(){ MSYS_NO_PATHCONV=1 podman "$@"; }
DI_JOB=<job-id>; OUTP_JOB=<job-id>; SIM_JOB=<job-id>
SCORERL_JOB=<job-id>; RETRAIN_JOB=<job-id>
lake_auth(){ unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET DATABRICKS_TOKEN DATABRICKS_HOST; set -a; source nba/databricks/databricks.env 2>/dev/null; set +a; }
ml_auth(){ unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET DATABRICKS_TOKEN DATABRICKS_HOST; set -a; source nba/databricks/ml/ml.env 2>/dev/null; set +a; export DATABRICKS_HOST="$ML_DATABRICKS_HOST" DATABRICKS_CLIENT_ID="$ML_DATABRICKS_CLIENT_ID" DATABRICKS_CLIENT_SECRET="$ML_DATABRICKS_CLIENT_SECRET"; }
hwm(){ PX exec $PANDA rpk topic describe "$1" -p 2>/dev/null | awk 'NR>1{x+=$6} END{print x+0}'; }
restart(){ # $1=job_id $2=params_json -> cancel (clear stale conns), WAIT for it to terminate, then run-now
  MSYS_NO_PATHCONV=1 timeout 30 databricks api post /api/2.1/jobs/runs/cancel-all --json "{\"job_id\":$1}" 2>/dev/null >/dev/null
  # max_concurrent_runs=1: run-now COLLIDES (returns no run_id) if the cancelled run is still terminating. Poll
  # until there are 0 active runs (bounded ~36s) before launching the fresh one.
  local n; for _ in $(seq 1 12); do
    n=$(MSYS_NO_PATHCONV=1 timeout 20 databricks api get "/api/2.1/jobs/runs/list?job_id=$1&active_only=true&limit=1" 2>/dev/null | python -c "import sys,json;print(len(json.load(sys.stdin).get('runs',[])))" 2>/dev/null)
    [ "${n:-1}" = 0 ] && break; sleep 3
  done
  MSYS_NO_PATHCONV=1 timeout 30 databricks jobs run-now --json "{\"job_id\":$1,\"notebook_params\":$2}" --no-wait -o json 2>/dev/null | python -c "import sys,json;print(json.load(sys.stdin).get('run_id',''))" 2>/dev/null; }

heal(){
  echo "### HEAL — staged cloud-job restart (clears stale tunnel conns; resume-from-checkpoint, STAGGER=${STAGGER}s)"
  lake_auth
  echo "  datalake-ingest: $(restart $DI_JOB '{\"trigger\":\"continuous\"}')"; sleep "$STAGGER"
  echo "  out-produce:     $(restart $OUTP_JOB '{\"trigger\":\"continuous\"}')"; sleep "$STAGGER"
  echo "  source-sim:      $(restart $SIM_JOB '{}')"; sleep "$STAGGER"
  ml_auth
  echo "  score-rl:        $(restart $SCORERL_JOB '{\"explore_c\":\"2.5\",\"first_try_bonus\":\"8.0\"}')"
  echo "### heal dispatched."
}

frozen(){ local a b; a=$(hwm nba.member.facts); sleep "$FREEZE_WAIT"; b=$(hwm nba.member.facts); [ "$b" -le "$a" ]; }

watch_loop(){
  echo "### tunnel healer watching $TUN (heal only if a reconnect actually freezes the loop)"
  local saw=0
  while IFS= read -r line; do
    case "$line" in
      *reconnecting*) saw=1; echo "  tunnel reconnecting — arming";;
      *"=> [running]"*)
        if [ "$saw" = 1 ]; then
          saw=0; echo "  tunnel back; settling ${SETTLE}s then checking for a freeze"; sleep "$SETTLE"
          if frozen; then echo "  LOOP FROZEN after reconnect — healing"; heal
          else echo "  loop still flowing after reconnect (self-heal rode through) — no action"; fi
        fi;;
    esac
  done < <(PX logs -f --tail 0 "$TUN" 2>&1)
}

case "${1:-watch}" in
  heal)  heal;;
  watch) watch_loop;;
  *) echo "usage: bash $0 <watch|heal>";;
esac
