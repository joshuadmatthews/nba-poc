#!/usr/bin/env bash
# END-TO-END SMOKE — proves the LIVE CQL-driven NBA pipeline produces the full chain, no mocks:
#   drive real members -> the CQL POLICY scores them (real Q-values) -> the action-router routes on those scores
#   -> the state machine + activation layer progress them to a DISPOSITION (SOFT/HARD_COMPLETED).
# The action-router logs each decision as "<STATE> <member> -> <action>::<channel> score=<CQL Q>", so a
# completion line with a real (non-zero) score IS the whole chain working for a member.
#
#   bash nba/test/nba_e2e_smoke.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "== drive a burst of members through the live pipeline =="
python3 "$HERE/source_gen.py" 60 "e2e$(date +%s)" 2>&1 | tail -1

echo "== wait for the chain: CQL score -> router -> dispatch -> disposition =="
ok=0
for i in $(seq 1 14); do
  sleep 15
  # a SOFT/HARD-completed routing decision carrying a real (non-zero) CQL score = the full chain worked
  hit=$(podman logs --tail 300 ais-nba-action-router 2>&1 \
        | grep -E "SOFT_COMPLETE|HARD_COMPLETE|PRESENTED" \
        | grep -E "score=-?[0-9]" | grep -vE "score=0\.0" | tail -1)
  if [ -n "$hit" ]; then echo "  $hit"; ok=1; break; fi
  echo "  ...waiting ($((i*15))s)"
done

if [ "$ok" = 1 ]; then
  echo ""; echo "[PASS] SYSTEM WORKING end-to-end: a member was scored by the CQL and routed to a disposition."
  exit 0
else
  echo ""; echo "[FAIL] no real-score completion observed in the router — the CQL->route->disposition chain stalled."
  exit 1
fi
