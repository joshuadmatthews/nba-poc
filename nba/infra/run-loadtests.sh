#!/usr/bin/env bash
# NBA per-stage throughput matrix — ONE command to regenerate the core numbers in loadtest-results.md.
#
# Measures the warm STANDING-BACKLOG drain rate (records/s) of each spine stage: drive a synthetic burst to the
# stage's input topic, then measure how fast the stage's consumer group drains it (no producer interference —
# the burst finishes before the window). This is the methodology behind §6 of loadtest-results.md.
#
#   bash nba/infra/run-loadtests.sh [N] [WINDOW_S]      (default N=150000 per stage, 20s window)
#   bash nba/infra/run-loadtests.sh --engines [N]       (also measure the Flink shadow rules stage)
#
# The Temporal start-rate sweep, the authoritative write-through test, and the read-latency matrix are finicky
# (self-loop / mode switches / OOM headroom) and are documented as procedures in loadtest-results.md §6a/6b/2.
set -uo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/../.." 2>/dev/null   # repo root (so nba/infra/*.py resolve)

ENGINES=0; if [ "${1:-}" = "--engines" ]; then ENGINES=1; shift; fi
N=${1:-150000}; W=${2:-20}
RP=ais-nba-redpanda; IMG=localhost/nba-journey-scorer:latest
now(){ date +%s%3N; }
lag(){ podman exec $RP rpk group describe "$1" 2>/dev/null | awk -v t="$2" '$1==t{s+=$6} END{print s+0}'; }
cpu(){ podman exec "$1" cat /sys/fs/cgroup/cpu.stat 2>/dev/null | awk '/usage_usec/{print $2}'; }

# drive <mode> <topic> <prefix>  — produce N synthetic records of the given shape (see gen.py)
drive(){ podman run --rm -i --network aiservices_default -e NBA_GEN_MODE="$1" -e NBA_TOPIC="$2" \
  -e NBA_LOAD_N="$N" -e NBA_LOAD_MEMBERS="$N" -e NBA_LOAD_PREFIX="$3" "$IMG" python - < nba/infra/gen.py 2>&1 | tail -1 | sed 's/^/    /'; }

# measure <label> <container> <group> <topic>  — standing-backlog drain over W seconds
measure(){
  local label=$1 c=$2 g=$3 t=$4
  local l0 c0 t0 l1 c1 t1 dt
  l0=$(lag "$g" "$t"); c0=$(cpu "$c"); t0=$(now)
  echo "    standing backlog=$l0 ; measuring ${W}s..."
  sleep "$W"
  l1=$(lag "$g" "$t"); c1=$(cpu "$c"); t1=$(now); dt=$((t1-t0)); [ "$dt" -le 0 ] && dt=1
  printf "  >>> %-22s %7d rec/s | cpu %3d%% | backlog %d->%d\n" \
    "$label" "$(( (l0-l1)*1000/dt ))" "$(( (c1-c0)/(dt*10) ))" "$l0" "$l1"
}

echo "=== NBA per-stage throughput matrix (N=$N/stage, ${W}s window) ==="
echo "--- classic spine ---"
echo "[snapshot] driving facts -> nba.member.facts"
drive fact nba.member.facts lt_snap_
measure "classic snapshot" ais-nba-snapshot-builder snapshot-builder nba.member.facts

echo "[rules] driving snapshots -> nba.snapshots"
drive snapshot nba.snapshots lt_rule_
measure "classic rules" ais-nba-rules-engine rules-engine-snapshots nba.snapshots

echo "[router] driving evaluations -> nba.evaluations"
drive eval nba.evaluations lt_rout_
measure "classic router" ais-nba-action-router action-router nba.evaluations

if [ "$ENGINES" = "1" ]; then
  echo "--- Flink reference engine (shadow; local-state compute, write-through OFF) ---"
  if ! podman ps --format '{{.Names}}' | grep -q ais-nba-flink-engine; then
    echo "  (flink-engine not running — start it: pwsh nba/services/nba-flink-engine/run.ps1 -Mode shadow)"
  else
    echo "[flink rules] driving snapshots -> nba.snapshots.shadow"
    drive snapshot nba.snapshots.shadow lt_frule_
    measure "flink rules" ais-nba-flink-engine nba-flink-engine-rules-snaps nba.snapshots.shadow
  fi
fi

echo ""
echo "Done. Full study (Temporal start sweep, authoritative write-through, read-latency, the RAM wall):"
echo "  nba/infra/loadtest-results.md"
