#!/usr/bin/env bash
# Warm-isolated stage throughput. The MEASURED container must ALREADY be warm + caught up (the caller isolates
# by stopping every other consumer + the flywheel, and waits for lag~0 first — this avoids the stop/start
# rejoin artifact). Produces a burst to the stage's input topic, then measures the STANDING-backlog drain rate
# (records/s) + CPU% + mem, with no producer interference (the produce finishes before the drain window).
#   loadtest-stage.sh <container> <group> <input-topic> [N] [MEMBERS] [PREFIX] [WINDOW_S]
set -uo pipefail
export MSYS_NO_PATHCONV=1
C=$1; GROUP=$2; TOPIC=$3; N=${4:-1500000}; MEMBERS=${5:-60000}; PREFIX=${6:-st_}; W=${7:-20}
RP=ais-nba-redpanda
now(){ date +%s%3N; }
lag(){ podman exec $RP rpk group describe "$GROUP" 2>/dev/null | awk -v t="$TOPIC" '$1==t{s+=$6} END{print s+0}'; }
cpuusec(){ podman exec "$C" cat /sys/fs/cgroup/cpu.stat 2>/dev/null | awk '/usage_usec/{print $2}'; }
memcur(){ podman exec "$C" cat /sys/fs/cgroup/memory.current 2>/dev/null; }

base=$(lag); echo "  [$C/$GROUP@$TOPIC] baseline lag=$base ; producing $N (prefix=$PREFIX)..."
podman run --rm -i --network aiservices_default -e NBA_LOAD_N="$N" -e NBA_LOAD_MEMBERS="$MEMBERS" \
  -e NBA_LOAD_PREFIX="$PREFIX" -e NBA_TOPIC="$TOPIC" localhost/nba-journey-scorer:latest \
  python - < nba/infra/loadgen.py 2>&1 | tail -1 | sed 's/^/    /'
l0=$(lag); c0=$(cpuusec); t0=$(now)
echo "  standing backlog after produce: $l0 ; measuring drain ${W}s..."
sleep "$W"
l1=$(lag); c1=$(cpuusec); t1=$(now)
drained=$((l0-l1)); dt=$((t1-t0)); [ "$dt" -le 0 ] && dt=1
rate=$(( drained*1000/dt )); cpu=$(( (c1-c0)/(dt*10) )); mem=$(( $(memcur)/1048576 ))
printf "  >>> %-26s %8d rec/s | cpu %3d%% of 1 core | mem %4dMB | backlog %d->%d\n" "$C" "$rate" "$cpu" "$mem" "$l0" "$l1"
