#!/usr/bin/env bash
# Find representative member snapshots at journey stages by classifying on the NBA facts present:
#   EARLY     = only operator.* inbound facts, no nba.* engine facts (member just entered, not yet acted on)
#   MID       = scored + workflow(s) in flight (nba.score.*, nba.actionstate.*), no/few completions
#   COMPLETED = >=2 nba.completion.* goals reached (+ terminal action states)
# Prints the first match of each stage (key + counts). Then dump those keys' full snapshots.
export MSYS_NO_PATHCONV=1
podman exec ais-nba-redis sh -c '
redis-cli --scan --pattern "nba:snapshot:*" | head -8000 | while read k; do
  fields=$(redis-cli hkeys "$k" 2>/dev/null)
  nc=$(printf "%s\n" "$fields" | grep -c "^fact:nba.completion.")
  ns=$(printf "%s\n" "$fields" | grep -c "^fact:nba.actionstate.")
  nsc=$(printf "%s\n" "$fields" | grep -c "^fact:nba.score.")
  nop=$(printf "%s\n" "$fields" | grep -c "^fact:operator.")
  if [ "$nc" -ge 2 ]; then echo "COMPLETED $k nc=$nc ns=$ns nsc=$nsc nop=$nop"; fi
  if [ "$nc" -eq 0 ] && [ "$ns" -ge 1 ] && [ "$nsc" -ge 1 ]; then echo "MID $k ns=$ns nsc=$nsc nop=$nop"; fi
  if [ "$nc" -eq 0 ] && [ "$ns" -eq 0 ] && [ "$nsc" -eq 0 ] && [ "$nop" -ge 3 ]; then echo "EARLY $k nop=$nop"; fi
done | awk "!seen[\$1]++ {print} END{}"
'
