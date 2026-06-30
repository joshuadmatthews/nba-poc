#!/usr/bin/env bash
# Reset the LOCAL NBA stack to a FRESH, deterministic baseline — so a test run NEVER depends on residue
# (a stale 1.6M-record DLQ, old journeys still progressing, leaked consumer offsets, a definitions topic
# bloated by a session of edits). Every test suite sources/runs this FIRST.
#
# What it resets ("topics, pointers, DLQs, and all"):
#   - recreates every nba.* topic (data + DLQ + .shadow) -> empties them AND resets consumer-group offsets
#     (a recreated topic has hwm 0, so each group's committed offset is out of range and snaps to the reset),
#   - flushes Redis runtime state (snapshots / idmaps / eligibility / sim / channel_touch / optout / throttle),
#   - re-seeds definitions (nba.definitions) + Redis def-state (rulefacts / action catalog / sim params)
#     exactly as up.ps1 does, then RESTARTS the spine consumers so they rejoin the clean topics and re-read the
#     definitions from the start (a fresh consumer reads earliest -> it can't miss the re-seeded defs).
# No container *recreation* (just `restart`), so it's fast (~25-35s) and keeps every image/volume intact.
#
#   bash nba/test/reset-fresh.sh                # reset; seed NO members (each test seeds its own)
#   bash nba/test/reset-fresh.sh --members 50   # reset + seed 50 demo members (for parity / load runs)
set -uo pipefail
export MSYS_NO_PATHCONV=1
P=ais-nba-redpanda; R=ais-nba-redis
cd "$(cd "$(dirname "$0")/.." && pwd)/.." 2>/dev/null   # repo root
INFRA=nba/infra
MEMBERS=0; [ "${1:-}" = "--members" ] && MEMBERS="${2:-0}"
# spine consumers (NOT infra: redpanda/redis/temporal stay up). `restart` preserves the container + env.
SPINE="ais-nba-snapshot-builder ais-nba-rules-engine ais-nba-journey-scorer ais-nba-conversion-sim ais-nba-action-router ais-nba-temporal-worker ais-nba-action-layer"

recreate(){ local t cfg parts
  for t in "$@"; do [ -z "$t" ] && continue
    cfg=$(podman exec $P rpk topic describe "$t" -c 2>/dev/null | awk '/cleanup.policy/{print $2}')
    parts=$(podman exec $P rpk topic describe "$t" -p 2>/dev/null | awk '/^[0-9]+[[:space:]]/{n++} END{print (n>0?n:1)}')
    podman exec $P rpk topic delete "$t" >/dev/null 2>&1
    podman exec $P rpk topic create "$t" -p "$parts" ${cfg:+-c "cleanup.policy=$cfg"} >/dev/null 2>&1
  done; }

echo "== reset: recreate all nba.* topics (empties data/DLQ/shadow + resets offsets) =="
ALL=$(podman exec $P rpk topic list 2>/dev/null | awk 'NR>1{print $1}' | grep -E '^nba\.')
recreate $ALL
echo "   recreated $(echo "$ALL" | grep -c .) topics"

echo "== reset: flush Redis runtime state =="
podman exec $R redis-cli flushdb >/dev/null 2>&1

echo "== reset: re-seed definitions (topic) + Redis def-state (rulefacts/catalog/sim) =="
if [ -f "$INFRA/seed/definitions.jsonl" ]; then
  podman cp "$INFRA/seed/definitions.jsonl" $P:/tmp/nba-defs.jsonl >/dev/null 2>&1
  podman exec $P sh -c "rpk topic produce nba.definitions -f '%k\t%v\n' < /tmp/nba-defs.jsonl" >/dev/null 2>&1
  echo "   replayed $(wc -l < "$INFRA/seed/definitions.jsonl") definition records -> nba.definitions"
else echo "   [warn] seed/definitions.jsonl missing"; fi
if [ -f "$INFRA/seed/redis-defs.sh" ]; then
  podman cp "$INFRA/seed/redis-defs.sh" $R:/tmp/nba-redis-defs.sh >/dev/null 2>&1
  podman exec $R sh /tmp/nba-redis-defs.sh >/dev/null 2>&1
fi

echo "== reset: restart spine consumers (rejoin clean topics, re-read defs from earliest) =="
for c in $SPINE; do podman restart "$c" >/dev/null 2>&1 & done; wait

echo "== settle: defs propagate + snapshots rejoin =="
sleep 18
echo "   rulefacts=$(podman exec $R redis-cli scard nba:rulefacts 2>/dev/null) | dlq.snapshot-builder hwm=$(podman exec $P rpk topic describe nba.dlq.snapshot-builder -p 2>/dev/null | awk '/^[0-9]/{print $6}')"

if [ "$MEMBERS" -gt 0 ]; then
  echo "== reset: seed $MEMBERS demo members =="
  python "$INFRA/reseed-members-local.py" "$MEMBERS" 2>&1 | tail -1
  sleep 8
fi
echo "== reset-fresh complete =="
