#!/usr/bin/env bash
# NBA flywheel CLEAN RESET — wipe every transient data/state store, reseed a clean 500-member baseline through
# bronze, and restart the flywheel from a known-good foundation.
#
# PRESERVES (your investment): trained RL policy /Volumes/nba_ml/core/ckpt/rl_qnet.json, the action/journey
# definitions (nba.definitions topic + Redis nba:rulefacts + dim_definitions),
# the model cards (gold_model_card*), the action->fact map, and the kafka-connect config topics.
#
# WIPES: the ~42M junk in the data topics, the ~7.9M-row medallion data tables, the Redis loop working state.
# No checkpoint clearing needed — trimming each topic to its HWM makes "earliest" the post-trim edge, so the
# streaming jobs read only the fresh seed (this is what was silently reprocessing 42M rows from offset 0).
#
# Usage:  bash nba/infra/nba-clean-reset.sh <stop|wipe|seed|start|verify|all>
set +e
REPO="197121cd "197121dirname "-e")/../.." 2>/dev/null && pwd)"; cd "$REPO" 2>/dev/null
PANDA=ais-nba-redpanda; REDIS=ais-nba-redis; WH=<warehouse-id>
PX(){ MSYS_NO_PATHCONV=1 podman "$@"; }

# data topics to EMPTY via trim-to-end (cleanup.policy=delete; config preserved). Excludes nba.definitions /
# nba.model.card / nba_connect_*.
DATA_TOPICS="nba.member.facts nba.facts nba.activations datalake.streaming-inbound \
nba.member.activity.raw nba.dlq.snapshot-builder nba.member.facts.dlq nba.activations.dlq nba.dlq.temporal-bridge \
nba.facts.dlq nba.action.requests.dlq nba.dispositions.dlq nba.dlq.action-layer nba.dlq.action-router \
nba.dlq.ml-scorer-features nba.dlq.ml-scorer-scorer nba.dlq.temporal-disposition nba.evaluations.dlq"
# COMPACTED topics — deleteRecords/trim is policy-blocked (POLICY_VIOLATION), so delete+recreate to empty them
# while preserving the compact config.
COMPACT_TOPICS="nba.snapshots nba.evaluations"
# medallion DATA tables to TRUNCATE. Preserves dim_definitions, action_fact_map, gold_model_card(_history).
LAKE_TABLES="silver_fact_history silver_activations silver_eval_eligible silver_snapshots silver_milestones \
silver_member_activity gold_facts gold_member_facts gold_member_idmap gold_member_snapshot gold_rulefacts_state \
gold_system_stats bronze_member_activity _out_watermark streampoc_out"
LOOP="snapshot-builder rules-engine action-router temporal-worker action-layer"
# cloud jobs
DI_JOB=<job-id>; OUTP_JOB=<job-id>; SIM_JOB=<job-id>
SCORERL_JOB=<job-id>; SCOREB_JOB=<job-id>; RETRAIN_JOB=<job-id>

lake_auth(){ unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET DATABRICKS_TOKEN DATABRICKS_HOST; set -a; source nba/databricks/databricks.env 2>/dev/null; set +a; }
ml_auth(){ unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET DATABRICKS_TOKEN DATABRICKS_HOST; set -a; source nba/databricks/ml/ml.env 2>/dev/null; set +a; export DATABRICKS_HOST="$ML_DATABRICKS_HOST" DATABRICKS_CLIENT_ID="$ML_DATABRICKS_CLIENT_ID" DATABRICKS_CLIENT_SECRET="$ML_DATABRICKS_CLIENT_SECRET"; }
sql(){ MSYS_NO_PATHCONV=1 timeout 90 databricks api post /api/2.0/sql/statements --json "{\"warehouse_id\":\"$WH\",\"wait_timeout\":\"50s\",\"statement\":\"$1\"}" 2>/dev/null; }
runnow(){ MSYS_NO_PATHCONV=1 timeout 30 databricks jobs run-now --json "{\"job_id\":$1}" --no-wait -o json 2>/dev/null | python -c "import sys,json;print(json.load(sys.stdin).get('run_id',''))" 2>/dev/null; }
# datalake-ingest's base_param is trigger=availableNow (single drain -> terminates). Override to continuous so it
# keeps draining (source-sim has an unconditional while-loop, score-rl defaults to continuous; only this one needs it).
runnow_cont(){ MSYS_NO_PATHCONV=1 timeout 30 databricks jobs run-now --json "{\"job_id\":$1,\"notebook_params\":{\"trigger\":\"continuous\"}}" --no-wait -o json 2>/dev/null | python -c "import sys,json;print(json.load(sys.stdin).get('run_id',''))" 2>/dev/null; }
# out-produce streams gold_member_snapshot's CDF via a UC-volume checkpoint; the wipe TRUNCATEd that table, so the
# stored offset is stale -> pass reset=true to clear the checkpoint and re-bootstrap the full fresh snapshot.
runnow_reset(){ MSYS_NO_PATHCONV=1 timeout 30 databricks jobs run-now --json "{\"job_id\":$1,\"notebook_params\":{\"reset\":\"true\"}}" --no-wait -o json 2>/dev/null | python -c "import sys,json;print(json.load(sys.stdin).get('run_id',''))" 2>/dev/null; }
# datalake-ingest SUBSCRIBES to nba.snapshots + nba.evaluations, which the wipe DELETE+RECREATEs (compacted topics
# can't be trimmed). The recreate invalidates those topics' checkpoint offsets and WEDGES the whole multi-topic
# stream (it loops in self-heal, never draining ANY topic incl. the seed) — bronze stays 0. reset=true clears the
# checkpoint so it rebuilds offsets from earliest cleanly; +continuous so it keeps draining. (silver/gold already
# truncated, so the reset's truncate is a no-op here.)
runnow_reset_cont(){ MSYS_NO_PATHCONV=1 timeout 30 databricks jobs run-now --json "{\"job_id\":$1,\"notebook_params\":{\"reset\":\"true\",\"trigger\":\"continuous\"}}" --no-wait -o json 2>/dev/null | python -c "import sys,json;print(json.load(sys.stdin).get('run_id',''))" 2>/dev/null; }
cancelall(){ MSYS_NO_PATHCONV=1 timeout 30 databricks api post /api/2.1/jobs/runs/cancel-all --json "{\"job_id\":$1}" 2>/dev/null >/dev/null; }
backlog(){ PX exec $PANDA rpk topic describe "$1" -p 2>/dev/null | grep -E "^[0-9]" | awk '{s+=$6-$5} END{print s+0}'; }

phase_stop(){
  echo "### STOP — halt every producer/consumer"
  lake_auth; for j in $SIM_JOB $OUTP_JOB $DI_JOB; do cancelall $j; echo "  LAKE job $j cancel-all"; done
  ml_auth;   for j in $SCORERL_JOB $SCOREB_JOB $RETRAIN_JOB; do cancelall $j; echo "  ML   job $j cancel-all"; done
  for c in $LOOP; do PX stop -t 8 ais-nba-$c >/dev/null 2>&1 && echo "  stopped ais-nba-$c"; done
}

phase_wipe(){
  echo "### WIPE"
  echo "-- topics (trim to end, config preserved) --"
  for t in $DATA_TOPICS; do
    PX exec $PANDA rpk topic trim-prefix "$t" -o end --no-confirm >/dev/null 2>&1
    printf "   %-34s backlog=%s\n" "$t" "$(backlog "$t")"
  done
  echo "-- compacted topics (delete+recreate; compact policy preserved) --"
  for t in $COMPACT_TOPICS; do
    PX exec $PANDA rpk topic delete "$t" >/dev/null 2>&1
    PX exec $PANDA rpk topic create "$t" -p 1 -r 1 -c cleanup.policy=compact -c message.timestamp.type=LogAppendTime >/dev/null 2>&1
    printf "   %-34s backlog=%s\n" "$t" "$(backlog "$t")"
  done
  echo "-- medallion (truncate data tables; defs + model cards kept) --"
  lake_auth
  for tb in $LAKE_TABLES; do sql "TRUNCATE TABLE workspace.nba_poc.$tb" >/dev/null; printf "   truncated %s\n" "$tb"; done
  echo "-- redis loop state (keep nba:rulefacts) --"
  for p in "nba:snapshot:*" "nba:idmap:*" "nba:eval:*" "nba:disposition:*" "nba:activation:*" "nba:latch:*" "nba:lock:*"; do
    PX exec $REDIS sh -c "redis-cli --scan --pattern '$p' | xargs -r -n 500 redis-cli del" >/dev/null 2>&1
  done
  echo "   redis dbsize now: $(PX exec $REDIS redis-cli dbsize 2>/dev/null) (definitions retained)"
  echo "-- temporal (terminate all running workflows; closed ones age out via retention) --"
  PX exec ais-nba-temporal temporal workflow terminate --query 'ExecutionStatus="Running"' --reason "nba clean reset" --yes >/dev/null 2>&1
  sleep 6
  RUN=$(PX exec ais-nba-temporal temporal workflow count --query 'ExecutionStatus="Running"' 2>/dev/null | awk -F': ' '/Total/{print $2}')
  echo "   running workflows now: ${RUN:-?} (batch terminate is async; may still be draining)"
}

phase_seed(){
  echo "### SEED — 500 baseline members -> datalake.streaming-inbound (bronze)"
  python nba/infra/reseed-members-local.py 500 2>&1 | tail -1
  echo "   datalake-inbound HWM now: $(PX exec $PANDA rpk topic describe datalake.streaming-inbound -p 2>/dev/null | grep -E '^[0-9]' | awk '{print $6}')"
}

# The cloud jobs all cross the SAME external tunnel. Launching them in one burst stampedes it: connections pile up
# (esp. right after a tunnel reconnect), exceed the budget, and jobs fail "Failed to create KafkaAdminClient" — the
# tunnel then looks wedged when it isn't. So STAGE the startup (STAGGER seconds apart, in dependency order: ingest
# fills gold -> out-produce bridges it -> sim/score feed off it) so each job's connections settle before the next.
# Tunnel reconnects are normal (~hourly); staged, fresh connections ride through them. See project_nba_external_tunnel_pileup.
STAGGER=${STAGGER:-25}
phase_start(){
  echo "### START — loop consumers, then STAGED cloud jobs (${STAGGER}s apart so they don't stampede the tunnel)"
  for c in $LOOP; do PX start ais-nba-$c >/dev/null 2>&1 && echo "  started ais-nba-$c"; done
  PX restart ais-nba-command-center >/dev/null 2>&1 && echo "  restarted ais-nba-command-center (BFF in-memory STATE_OF reset; re-tails clean edge)"
  lake_auth
  echo "  datalake-ingest run: $(runnow_reset_cont $DI_JOB)  (reset=true — clears checkpoint wedged by the nba.snapshots/evaluations recreate; continuous)"; sleep "$STAGGER"
  echo "  out-produce    run: $(runnow_reset $OUTP_JOB)  (reset=true -> fresh checkpoint + bootstrap)"; sleep "$STAGGER"
  echo "  source-sim     run: $(runnow $SIM_JOB)"; sleep "$STAGGER"
  ml_auth
  echo "  score-rl       run: $(runnow $SCORERL_JOB)"; sleep "$STAGGER"
  echo "  retrain-loop   run: $(runnow $RETRAIN_JOB)"
}

phase_verify(){
  echo "### VERIFY"
  lake_auth
  echo "-- gold respondedToOutreach (expect ~500 False / 0 True) --"
  sql "SELECT value, count(*) c FROM workspace.nba_poc.gold_member_snapshot WHERE key='operator.activity.respondedToOutreach' GROUP BY value" | python -c "import sys,json;d=json.load(sys.stdin);[print('  ',r) for r in d.get('result',{}).get('data_array',[])]" 2>/dev/null
  echo "-- gold member count --"
  sql "SELECT count(distinct entityId) FROM workspace.nba_poc.gold_member_snapshot" | python -c "import sys,json;d=json.load(sys.stdin);print('   members:',d.get('result',{}).get('data_array',[]))" 2>/dev/null
  echo "(then run the full health probe: bash nba/infra/nba-health-probe.sh)"
}

case "${1:-}" in
  stop) phase_stop;; wipe) phase_wipe;; seed) phase_seed;; start) phase_start;; verify) phase_verify;;
  all) phase_stop; phase_wipe; phase_seed; phase_start; echo; echo "Seeded + started. Give the medallion ~3-5 min, then: bash $0 verify";;
  *) echo "usage: bash $0 <stop|wipe|seed|start|verify|all>";;
esac
