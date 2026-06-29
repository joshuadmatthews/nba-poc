#!/usr/bin/env bash
# CLEAN snapshot-stage load test. Stop both consumers, fill a backlog of N fresh facts, then drain each engine
# in ISOLATION from a standing start: snapshot-builder (classic -> Redis HSET) first with the engine stopped,
# then the KStreams engine (-> RocksDB state + nba.snapshots.shadow). No producer interference, no mutual CPU
# contention. Measures DRAIN throughput (excludes rebalance/recovery via drain-start detection), CPU during the
# drain window (cgroup usage_usec), resident mem, and verifies real writes (Redis snapshot-key delta /
# nba.snapshots.shadow offset delta). Single partition => per-instance throughput.
#   ./loadtest-clean.sh [N=1000000] [MEMBERS=50000]
set -uo pipefail
export MSYS_NO_PATHCONV=1
N=${1:-1000000}; MEMBERS=${2:-50000}
RP=ais-nba-redpanda; SB=ais-nba-snapshot-builder; EN=ais-nba-decision-engine; RD=ais-nba-redis
now(){ date +%s%3N; }
lag(){ podman exec $RP rpk group describe "$1" 2>/dev/null | awk '$1=="nba.member.facts"{s+=$6} END{print s+0}'; }
cpuusec(){ podman exec "$1" cat /sys/fs/cgroup/cpu.stat 2>/dev/null | awk '/^usage_usec/{print $2}'; }
memcur(){ podman exec "$1" cat /sys/fs/cgroup/memory.current 2>/dev/null; }
snapcount(){ podman exec $RD redis-cli --scan --pattern 'nba:snapshot:*' 2>/dev/null | wc -l; }
shadowhw(){ podman exec $RP rpk topic describe -p nba.snapshots.shadow 2>/dev/null | awk 'NR==2{print $6}'; }

# measure_drain GROUP CONTAINER INITLAG -> "drain_ms throughput cpu_ms" (cpu measured drain-start..done, excl. startup)
measure_drain(){
  local g=$1 c=$2 init=$3 ds=0 cds=0 t0; t0=$(now)
  while :; do
    local l; l=$(lag "$g"); l=${l:-0}
    if [ "$ds" -eq 0 ] && [ "$l" -lt "$((init-1000))" ]; then ds=$(now); cds=$(cpuusec "$c"); fi
    if [ "$l" -le 5 ] && [ "$ds" -ne 0 ]; then
      local dt=$(( $(now)-ds )); [ "$dt" -le 0 ] && dt=1
      local c1; c1=$(cpuusec "$c")
      echo "$dt $(( init*1000/dt )) $(( (c1-cds)/1000 ))"; return
    fi
    [ "$(( $(now)-t0 ))" -gt 200000 ] && { echo "TIMEOUT($l) 0 0"; return; }
    sleep 1
  done
}

echo "=== 1) stop both consumers (build a clean backlog) ==="
podman stop $SB $EN >/dev/null 2>&1; echo "  stopped"
echo "=== 2) fill $N fresh facts / $MEMBERS members ==="
hw0=$(podman exec $RP rpk topic describe -p nba.member.facts 2>/dev/null | awk 'NR==2{print $6}')
podman run --rm -i --network aiservices_default -e NBA_LOAD_N="$N" -e NBA_LOAD_MEMBERS="$MEMBERS" -e NBA_LOAD_PREFIX=clt_ \
  localhost/nba-journey-scorer:latest python - < nba/infra/loadgen.py 2>&1 | tail -1
hw1=$(podman exec $RP rpk topic describe -p nba.member.facts 2>/dev/null | awk 'NR==2{print $6}')
sb_lag=$(lag snapshot-builder); en_lag=$(lag nba-decision-engine)
echo "  topic grew by $((hw1-hw0)); backlog sb=$sb_lag en=$en_lag"

echo "=== 3) drain SNAPSHOT-BUILDER alone (classic -> Redis) ==="
snap0=$(snapcount)
podman start $SB >/dev/null 2>&1
read sb_ms sb_tp sb_cpu < <(measure_drain snapshot-builder $SB "$sb_lag")
snap1=$(snapcount); sbmem=$(memcur $SB)
echo "  drain=${sb_ms}ms tp=${sb_tp}/s cpu=${sb_cpu}ms snap_delta=$((snap1-snap0)) mem=$(( sbmem/1048576 ))MB"

echo "=== 4) drain ENGINE (KStreams -> RocksDB); start, rebalance, then drain ==="
sh0=$(shadowhw)
podman start $EN >/dev/null 2>&1
en_init=$(lag nba-decision-engine)
read en_ms en_tp en_cpu < <(measure_drain nba-decision-engine $EN "$en_init")
sh1=$(shadowhw); enmem=$(memcur $EN)
echo "  drain=${en_ms}ms tp=${en_tp}/s cpu=${en_cpu}ms shadow_emit_delta=$((sh1-sh0)) mem=$(( enmem/1048576 ))MB"

echo
echo "===================== RESULT  N=$N / $MEMBERS members (clean, isolated, real writes) ====================="
printf "  snapshot-builder (classic -> Redis HSET):  %7s facts/s | drain %6sms | cpu %5sms | mem %4sMB\n" "$sb_tp" "$sb_ms" "$sb_cpu" "$(( sbmem/1048576 ))"
printf "  decision-engine  (KStreams -> RocksDB):    %7s facts/s | drain %6sms | cpu %5sms | mem %4sMB\n" "$en_tp" "$en_ms" "$en_cpu" "$(( enmem/1048576 ))"
