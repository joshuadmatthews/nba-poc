#!/usr/bin/env bash
# ISOLATED pure snapshot-write throughput. Stops the whole downstream flywheel (rules-engine, router, library,
# layer, scorer, sims, temporal-worker, cdc) AND the other front-end, so the only thing running is the one
# snapshot front-end under test draining a standing backlog. This removes the confound that gated the earlier
# runs (the flywheel reacting to every snapshot contends for CPU + Redis). Measures the PURE state-write rate:
# snapshot-builder's synchronous Redis HGETALL+HSET-per-fact vs the engine's local RocksDB put-per-fact.
set -uo pipefail
export MSYS_NO_PATHCONV=1
N=${1:-500000}; MEMBERS=${2:-30000}
RP=ais-nba-redpanda; SB=ais-nba-snapshot-builder; EN=ais-nba-decision-engine
FLYWHEEL="ais-nba-rules-engine ais-nba-action-router ais-nba-action-library ais-nba-action-layer ais-nba-journey-scorer ais-nba-conversion-sim ais-nba-temporal-worker ais-nba-command-center ais-nba-connect ais-agent-nba"
lag(){ podman exec $RP rpk group describe "$1" 2>/dev/null | awk '$1=="nba.member.facts"{s+=$6} END{print s+0}'; }
cpuusec(){ podman exec "$1" cat /sys/fs/cgroup/cpu.stat 2>/dev/null | awk '/^usage_usec/{print $2}'; }
memcur(){ podman exec "$1" cat /sys/fs/cgroup/memory.current 2>/dev/null; }
rate_of(){ # CONTAINER GROUP WINDOW_S -> "rate cpu%"
  local c=$1 g=$2 w=$3 l0 l1 c0 c1; l0=$(lag "$g"); c0=$(cpuusec "$c"); sleep "$w"; l1=$(lag "$g"); c1=$(cpuusec "$c")
  echo "$(( (l0-l1)/w )) $(( (c1-c0)/(w*10000) ))"
}

echo "=== stop downstream flywheel + both front-ends (clean isolation) ==="
podman stop $FLYWHEEL $SB $EN >/dev/null 2>&1; echo "  stopped flywheel + front-ends"
echo "=== fill $N fresh facts / $MEMBERS members ==="
podman run --rm -i --network aiservices_default -e NBA_LOAD_N="$N" -e NBA_LOAD_MEMBERS="$MEMBERS" -e NBA_LOAD_PREFIX=iso_ \
  localhost/nba-journey-scorer:latest python - < nba/infra/loadgen.py 2>&1 | tail -1

echo "=== Phase A: SNAPSHOT-BUILDER alone (engine + flywheel stopped) ==="
podman start $SB >/dev/null 2>&1
sleep 12   # boot + consumer join
read sb_rate sb_cpu < <(rate_of $SB snapshot-builder 40)
sbmem=$(memcur $SB)
echo "  snapshot-builder: ${sb_rate} facts/s | cpu ${sb_cpu}% of 1 core | mem $(( sbmem/1048576 ))MB"
podman stop $SB >/dev/null 2>&1

echo "=== Phase B: DECISION-ENGINE alone (snapshot-builder + flywheel stopped) ==="
podman start $EN >/dev/null 2>&1
for i in $(seq 1 60); do podman logs --tail 2 $EN 2>&1 | grep -q '> RUNNING' && break; sleep 1; done
sleep 5
read en_rate en_cpu < <(rate_of $EN nba-decision-engine 40)
enmem=$(memcur $EN)
echo "  decision-engine:  ${en_rate} facts/s | cpu ${en_cpu}% of 1 core | mem $(( enmem/1048576 ))MB"

echo "=== restore everything ==="
podman start $SB $FLYWHEEL >/dev/null 2>&1; echo "  restored"
echo
echo "===================== ISOLATED PURE SNAPSHOT-WRITE RATE (no flywheel, no mutual contention) ====================="
printf "  snapshot-builder (Redis HGETALL+HSET/fact): %6s facts/s | cpu %3s%% | mem %4sMB\n" "$sb_rate" "$sb_cpu" "$(( sbmem/1048576 ))"
printf "  decision-engine  (RocksDB put/fact):        %6s facts/s | cpu %3s%% | mem %4sMB\n" "$en_rate" "$en_cpu" "$(( enmem/1048576 ))"
