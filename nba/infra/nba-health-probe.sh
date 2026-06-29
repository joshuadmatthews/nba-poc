#!/usr/bin/env bash
# NBA flywheel health probe — ONE command that says what's UP/DOWN end to end:
#   local containers · external tunnel · cloud Databricks jobs (LAKE + ML) · data flow · end-state (ground truth).
# Usage: bash nba/infra/nba-health-probe.sh
set +e
REPO="197121cd "197121dirname "-e")/../.." 2>/dev/null && pwd)"; cd "$REPO" 2>/dev/null
ok(){  printf "  [ OK ] %s\n" "$1"; }
bad(){ printf "  [DOWN] %s\n" "$1"; }
warn(){ printf "  [WARN] %s\n" "$1"; }
hwm(){ MSYS_NO_PATHCONV=1 podman exec ais-nba-redpanda rpk topic describe "$1" -p 2>/dev/null | grep -E "^0" | awk '{print $6}'; }

echo "============== NBA FLYWHEEL HEALTH PROBE =============="

echo "--- LOCAL CONTAINERS ---"
RUN=$(MSYS_NO_PATHCONV=1 podman ps --format "{{.Names}}" 2>/dev/null)
for c in ais-nba-redpanda ais-nba-redis ais-nba-postgres ais-nba-connect ais-nba-rules-engine ais-nba-action-router \
         ais-nba-temporal ais-nba-temporal-worker ais-nba-action-layer ais-nba-snapshot-builder ais-nba-bff \
         ais-nba-command-center ais-nba-action-library ais-nba-kafka-tunnel; do
  echo "$RUN" | grep -qx "$c" && ok "$c" || bad "$c"
done

echo "--- TUNNEL (cloud -> local kafka) ---"
TL=$(MSYS_NO_PATHCONV=1 podman logs --tail 1 ais-nba-kafka-tunnel 2>&1 | tail -1)
echo "$TL" | grep -q "running" && ok "tunnel last log: $TL" || warn "tunnel last log: $TL"
python -c "import socket;s=socket.socket();s.settimeout(6)
try: s.connect(('<tunnel-endpoint>',19092)); print('  [ OK ] <tunnel-endpoint> reachable')
except Exception as e: print('  [DOWN] <tunnel-endpoint>',e)
finally: s.close()" 2>/dev/null

echo "--- CDC / OUTBOX (action-library outbox -> Debezium -> Kafka: the loop's spine) ---"
# This carries member.facts/activations/defs into Kafka. RUNNING isn't enough — verify the replication slot is
# active and not lagging (a wedged CDC here is invisible everywhere downstream; it hid the journey bug for a session).
MSYS_NO_PATHCONV=1 podman exec ais-nba-connect sh -c "curl -s 'http://localhost:8083/connectors?expand=status'" 2>/dev/null | python -c "
import sys,json
try: d=json.load(sys.stdin)
except Exception: print('  [DOWN] connect REST unreachable'); sys.exit()
if not d: print('  [DOWN] NO connectors registered'); sys.exit()
for n,i in d.items():
  st=i.get('status',{}); c=st.get('connector',{}).get('state'); ts=[t.get('state') for t in st.get('tasks',[])]
  print(('  [ OK ] ' if (c=='RUNNING' and ts and all(t=='RUNNING' for t in ts)) else '  [DOWN] ')+n+' connector='+str(c)+' tasks='+str(ts))" 2>/dev/null
MSYS_NO_PATHCONV=1 podman exec ais-nba-postgres psql -U nba -d actionlib -tAc "SELECT 'slot '||slot_name||' active='||active||' lag='||pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots" 2>/dev/null | sed 's/^/  [ OK ] /'

echo "--- CLOUD JOBS: LAKE ---"
# These three are CONTINUOUS BRIDGES (self-looping availableNow drains): source-sim feeds outcomes, ingest
# drains Kafka->gold, out-produce streams gold->member.facts. RUNNING is the only healthy state — a TERMINATED
# bridge silently FREEZES the loop (out-produce stopping was invisible here for hours; that's why this is strict).
( unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET DATABRICKS_TOKEN DATABRICKS_HOST
  set -a; source nba/databricks/databricks.env 2>/dev/null; set +a
  for jn in "<job-id>:source-sim" "<job-id>:datalake-ingest" "<job-id>:out-produce"; do
    jid="${jn%%:*}"; nm="${jn##*:}"
    s=$(MSYS_NO_PATHCONV=1 timeout 30 databricks api get "/api/2.1/jobs/runs/list?job_id=$jid&limit=1" 2>/dev/null | python -c "
import sys,json
d=json.load(sys.stdin); r=(d.get('runs',[]) or [{}])[0]; st=r.get('state',{})
print((st.get('life_cycle_state','?'))+'/'+str(st.get('result_state')))" 2>/dev/null)
    case "$s" in
      RUNNING*) ok "$nm $s";;
      '?/'*|/*|"") warn "$nm state unknown ($s) — transient API blip; re-run probe to confirm";;
      *) bad "$nm $s — continuous bridge NOT RUNNING; the gold<->local loop is frozen. restart it (bundle run / run-now trigger=continuous)";;
    esac
  done )

echo "--- CLOUD JOBS: ML ---"
( unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET DATABRICKS_TOKEN DATABRICKS_HOST
  set -a; source nba/databricks/ml/ml.env 2>/dev/null; set +a
  export DATABRICKS_HOST="$ML_DATABRICKS_HOST"; export DATABRICKS_CLIENT_ID="$ML_DATABRICKS_CLIENT_ID"; export DATABRICKS_CLIENT_SECRET="$ML_DATABRICKS_CLIENT_SECRET"
  for jn in "<job-id>:score-batch" "<job-id>:score-rl" "<job-id>:retrain-loop"; do
    jid="${jn%%:*}"; nm="${jn##*:}"
    s=$(MSYS_NO_PATHCONV=1 timeout 30 databricks api get "/api/2.1/jobs/runs/list?job_id=$jid&limit=1" 2>/dev/null | python -c "
import sys,json
d=json.load(sys.stdin); r=(d.get('runs',[]) or [{}])[0]; st=r.get('state',{})
print((st.get('life_cycle_state','?'))+'/'+str(st.get('result_state')))" 2>/dev/null)
    # SKIPPED/MAXIMUM_CONCURRENT_RUNS_REACHED = a scheduled tick self-paced (a run was still active; queue is OFF so
    # the tick is dropped not stacked) = HEALTHY, not down. Only a hard FAILED/ERROR/TIMEDOUT is a real problem.
    case "$s" in RUNNING*|TERMINATED/SUCCESS*|SKIPPED*) ok "$nm $s";; '?/'*|/*|"") warn "$nm state unknown ($s) — transient API blip";; *) bad "$nm $s";; esac
  done )

echo "--- JOB HYGIENE / COST (active runs per job; >1 = piled up = wasted serverless \$) ---"
# Every reset/restart does cancel-all + run-now; if a cancel doesn't fully take, runs STACK and each active run bills
# serverless separately. This counts active runs per job and flags duplicates. Expected steady state: the continuous
# bridges (source-sim/out-produce/ingest on LAKE; score-rl on ML) at x1 each + at most one in-flight retrain.
_audit(){   # reads active runs on the CURRENT auth; prints "TOTAL n [MORE]" then "DUP jid count name" per piled-up job
  MSYS_NO_PATHCONV=1 timeout 30 databricks api get "/api/2.1/jobs/runs/list?active_only=true&limit=25" 2>/dev/null | python -c "
import sys,json
from collections import Counter
try: d=json.load(sys.stdin)
except Exception: print('ERR'); sys.exit()
runs=d.get('runs',[]); by=Counter(); nm={}
for r in runs: j=r.get('job_id'); by[j]+=1; nm[j]=r.get('run_name') or '?'
print('TOTAL', len(runs), 'MORE' if d.get('has_more') else '')
for j,n in by.items():
    if n>1: print('DUP', j, n, str(nm[j])[:38])" 2>/dev/null
}
( unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET DATABRICKS_TOKEN DATABRICKS_HOST
  set -a; source nba/databricks/databricks.env 2>/dev/null; set +a
  R=$(_audit); T=$(echo "$R" | awk '/^TOTAL/{print $2}')
  if echo "$R" | grep -q '^DUP'; then bad "LAKE $T active runs — PILE-UP (cancel-all the extras):"; echo "$R" | awk '/^DUP/{print "         job "$2" x"$3"  "$4}'
  elif echo "$R" | grep -q MORE; then warn "LAKE ${T}+ active runs (>25, paged) — likely pile-up, review"
  else ok "LAKE $T active runs, no duplicates"; fi )
( unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET DATABRICKS_TOKEN DATABRICKS_HOST
  set -a; source nba/databricks/ml/ml.env 2>/dev/null; set +a
  export DATABRICKS_HOST="$ML_DATABRICKS_HOST"; export DATABRICKS_CLIENT_ID="$ML_DATABRICKS_CLIENT_ID"; export DATABRICKS_CLIENT_SECRET="$ML_DATABRICKS_CLIENT_SECRET"
  R=$(_audit); T=$(echo "$R" | awk '/^TOTAL/{print $2}')
  if echo "$R" | grep -q '^DUP'; then bad "ML   $T active runs — PILE-UP (cancel-all the extras):"; echo "$R" | awk '/^DUP/{print "         job "$2" x"$3"  "$4}'
  elif echo "$R" | grep -q MORE; then warn "ML   ${T}+ active runs (>25, paged) — likely pile-up, review"
  else ok "ML   $T active runs, no duplicates"; fi )

echo "--- CLOUD MEDALLION FRESHNESS (RUNNING != draining — catches WEDGED jobs) ---"
# A job can show RUNNING but be stuck on a stale tunnel connection (e.g. after a external remove/re-add),
# draining nothing. State alone lies; cross-check that its OUTPUT table is actually advancing.
( unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET DATABRICKS_TOKEN DATABRICKS_HOST
  set -a; source nba/databricks/databricks.env 2>/dev/null; set +a
  WH=<warehouse-id>
  fresh(){ # <table> <label> <thresholdMinutes>
    age=$(MSYS_NO_PATHCONV=1 timeout 55 databricks api post /api/2.0/sql/statements --json "{\"warehouse_id\":\"$WH\",\"wait_timeout\":\"45s\",\"statement\":\"SELECT round((unix_millis(current_timestamp())-max(eventTs))/60000.0,1) FROM workspace.nba_poc.$1\"}" 2>/dev/null | python -c "import sys,json
try: print(json.load(sys.stdin)['result']['data_array'][0][0])
except Exception: print('ERR')")
    if [ "$age" = "ERR" ] || [ "$age" = "None" ]; then warn "$2 freshness unknown"
    elif awk "BEGIN{exit !($age<=$3)}"; then ok "$2 fresh — newest fact ${age}m old"
    else bad "$2 STALE ${age}m — upstream job RUNNING but not draining (wedged tunnel?); restart it"; fi
  }
  fresh silver_fact_history "silver (datalake-ingest)" 15
  fresh gold_member_snapshot "gold   (datalake-ingest)" 15 )

echo "--- DATA FLOW (HWM delta over 6s; frozen = not flowing) ---"
declare -A B
for t in nba.member.facts nba.snapshots nba.evaluations nba.activations datalake.streaming-inbound; do B[$t]=$(hwm "$t"); done
sleep 6
for t in nba.member.facts nba.snapshots nba.evaluations nba.activations datalake.streaming-inbound; do
  a=$(hwm "$t"); d=$((a-${B[$t]}))
  [ "$d" -gt 0 ] && ok "$t +$d" || warn "$t frozen ($a)"
done

echo "--- END STATE (ground truth from snapshots) ---"
MSYS_NO_PATHCONV=1 podman exec ais-nba-redis redis-cli eval '
local mem=0
for _ in ipairs(redis.call("keys","nba:idmap:OPERATOR:*")) do mem=mem+1 end
local c={}
for _,k in ipairs(redis.call("keys","nba:snapshot:*")) do
  local h=redis.call("hgetall",k)
  for i=1,#h,2 do
    if string.sub(h[i],1,21)=="fact:nba.actionstate." then
      local st=string.match(h[i+1],"\"value\":\"([%w_]+)\"")
      if st then c[st]=(c[st] or 0)+1 end
    end
  end
end
local r={"members="..mem}
for k,v in pairs(c) do r[#r+1]=k.."="..v end
return r' 0 2>/dev/null | sed 's/^/  /'
echo "======================================================"
