#!/usr/bin/env bash
# Read latency under MAX write load — the hot-path question. Flywheel stopped; a CONTINUOUS fact stream drives
# both front-ends to write their stores at full tilt while we probe the hot-path READ on each: Redis HGETALL
# (classic getSnapshot) vs the engine IQ GET /snapshot/{id} (RocksDB-backed). Reports idle vs under-load
# latency (avg/p50/p99) and the IQ error/503 rate (rebalance/commit stalls show up here).
set -uo pipefail
export MSYS_NO_PATHCONV=1
RP=ais-nba-redpanda; SB=ais-nba-snapshot-builder; EN=ais-nba-decision-engine; RD=ais-nba-redis
IMG=localhost/nba-journey-scorer:latest
FLYWHEEL="ais-nba-rules-engine ais-nba-action-router ais-nba-action-library ais-nba-action-layer ais-nba-journey-scorer ais-nba-conversion-sim ais-nba-temporal-worker ais-nba-command-center ais-nba-connect ais-agent-nba"

probe_redis(){ podman exec $RD redis-benchmark -n 60000 -c 10 hgetall "nba:snapshot:$1" 2>/dev/null | grep -A2 -iE 'throughput summary|latency summary' | grep -vE 'throughput summary|latency summary'; }
probe_iq(){ podman run --rm -i --network aiservices_default -e IQ_URL="http://nba-decision-engine:7020/snapshot/$1" -e N=4000 -e C=4 $IMG python - < nba/infra/iqprobe.py 2>&1 | tail -1; }

echo "=== stop flywheel; ensure both front-ends running ==="
podman stop $FLYWHEEL >/dev/null 2>&1
podman start $SB $EN >/dev/null 2>&1
sleep 8

NBAID=$(podman exec $RD redis-cli get nba:idmap:OPERATOR:clt_0 2>/dev/null)
[ -z "$NBAID" ] && NBAID=$(podman exec $RD redis-cli --scan --pattern 'nba:snapshot:*' 2>/dev/null | head -1 | sed 's#nba:snapshot:##')
echo "  probe nbaId=$NBAID  (Redis exists=$(podman exec $RD redis-cli exists "nba:snapshot:$NBAID" 2>/dev/null))"
iqcheck=$(podman run --rm --network aiservices_default $IMG python -c "import urllib.request;print(urllib.request.urlopen('http://nba-decision-engine:7020/snapshot/$NBAID',timeout=5).status)" 2>&1 | tail -1)
echo "  IQ has it (status): $iqcheck"

echo "=== IDLE baseline (no producer) ==="
echo "  Redis HGETALL avg/min/p50/p95/p99/max:"; probe_redis "$NBAID" | sed 's/^/    /'
echo "  IQ /snapshot: $(probe_iq "$NBAID")"

echo "=== start CONTINUOUS write load (4M facts, ~5min stream) ==="
podman rm -f nba-loadproducer >/dev/null 2>&1
podman run -d --name nba-loadproducer --network aiservices_default -e NBA_LOAD_N=4000000 -e NBA_LOAD_MEMBERS=100000 -e NBA_LOAD_PREFIX=rl_ \
  $IMG python - < nba/infra/loadgen.py >/dev/null 2>&1
sleep 18  # ramp; both front-ends now writing under load
echo "  backlog building: sb=$(podman exec $RP rpk group describe snapshot-builder 2>/dev/null | awk '$1=="nba.member.facts"{s+=$6}END{print s+0}') en=$(podman exec $RP rpk group describe nba-decision-engine 2>/dev/null | awk '$1=="nba.member.facts"{s+=$6}END{print s+0}')"

echo "=== UNDER MAX WRITE LOAD ==="
echo "  Redis HGETALL avg/min/p50/p95/p99/max:"; probe_redis "$NBAID" | sed 's/^/    /'
echo "  IQ /snapshot: $(probe_iq "$NBAID")"

podman rm -f nba-loadproducer >/dev/null 2>&1
echo "=== restore flywheel ==="
podman start $FLYWHEEL >/dev/null 2>&1
echo "  done"
