#!/usr/bin/env bash
# Snapshot-stage load test (LOCAL): drive a burst of member facts onto nba.member.facts and measure how fast
# snapshot-builder (classic -> Redis hash + nba.snapshots) vs the KStreams engine (shadow, RocksDB state +
# nba.snapshots.shadow) each DRAIN it. Both consume the SAME burst concurrently from the live edge, so it is a
# fair side-by-side throughput race on identical input. Reports per-engine drain throughput + CPU/mem sampled
# mid-drain. Single-partition topic => this measures PER-INSTANCE throughput, not horizontal (more-pods) scaling.
#
#   ./loadtest-snapshot.sh [N_FACTS=200000] [N_MEMBERS=10000]
set -uo pipefail
export MSYS_NO_PATHCONV=1
N=${1:-200000}; MEMBERS=${2:-10000}
RP=ais-nba-redpanda
SB_GROUP=snapshot-builder
EN_GROUP=nba-decision-engine
TOPIC=nba.member.facts
now(){ date +%s%3N; }
# lag on a SPECIFIC topic for a group (LAG is field 6; isolates the snapshot-stage source from any other topic)
lag(){ podman exec $RP rpk group describe "$1" 2>/dev/null | awk -v t="$TOPIC" '$1==t {s+=$6} END{print s+0}'; }

echo "=== baseline (both should be ~caught up) ==="
echo "  snapshot-builder=$(lag $SB_GROUP)  engine=$(lag $EN_GROUP)"

echo "=== producing $N facts across $MEMBERS members ==="
t0=$(now)
python -c "
import sys
keys=['operator.profile.diabetic','operator.activity.daysSinceLogin','operator.activity.hraCompleted','operator.activity.pcpSelected','operator.activity.careTeamEngaged','operator.comms.totalThisWeek','operator.activity.respondedToOutreach']
N=$N; M=$MEMBERS
w=sys.stdout.write
for i in range(N):
    m='load%d'%(i%M); k=keys[i%len(keys)]; v=i%30
    w('OPERATOR:%s|{\"entityType\":\"OPERATOR\",\"entityId\":\"%s\",\"key\":\"%s\",\"value\":%d,\"valueType\":\"LONG\",\"eventTs\":%d,\"source\":\"load\"}\n'%(m,m,k,v,1782700000000+i))
" | podman exec -i $RP rpk topic produce $TOPIC -f '%k|%v' >/dev/null 2>&1
t1=$(now)
echo "  produced in $((t1-t0))ms (~$(( N*1000/(t1-t0+1) ))/s raw ingest)"

# poll both groups' lag on the burst until drained (<=5); record total time from produce-start; sample CPU once mid-drain
sb=0; en=0; sampled=0
while :; do
  sl=$(lag $SB_GROUP); el=$(lag $EN_GROUP); sl=${sl:-0}; el=${el:-0}
  if [ "$sampled" -eq 0 ] && [ "$sl" -gt 2000 ]; then
    echo "  --- resource mid-drain (sl=$sl el=$el) ---"
    podman stats --no-stream --format '    {{.Name}}: cpu={{.CPUPerc}} mem={{.MemUsage}}' \
      ais-nba-snapshot-builder ais-nba-decision-engine ais-nba-redis 2>/dev/null
    sampled=1
  fi
  [ "$sl" -le 5 ] && [ "$sb" -eq 0 ] && sb=$(( $(now)-t0 ))
  [ "$el" -le 5 ] && [ "$en" -eq 0 ] && en=$(( $(now)-t0 ))
  { [ "$sb" -ne 0 ] && [ "$en" -ne 0 ]; } && break
  [ "$(( $(now)-t0 ))" -gt 240000 ] && { echo "  TIMEOUT (sl=$sl el=$el)"; [ "$sb" -eq 0 ] && sb=$(( $(now)-t0 )); [ "$en" -eq 0 ] && en=$(( $(now)-t0 )); break; }
  sleep 1
done

sbtp=$(( N*1000/(sb>0?sb:1) )); entp=$(( N*1000/(en>0?en:1) ))
echo "=== RESULT  N=$N facts / $MEMBERS members ==="
printf "  snapshot-builder (classic -> Redis):  %6dms  ~%d facts/s\n" "$sb" "$sbtp"
printf "  nba-decision-engine (KStreams):       %6dms  ~%d facts/s\n" "$en" "$entp"
