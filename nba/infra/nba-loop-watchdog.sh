#!/usr/bin/env bash
# NBA loop WATCHDOG — the missing piece. The health probe TELLS you the loop is frozen, but only if something
# RUNS it. This runs continuously: every POLL_S it samples the loop's heartbeat (kafka HWMs across the spine +
# the redis snapshot end-state) and EXITS NON-ZERO the moment flow stalls — no HWM advances across STALL_NEED
# consecutive polls past an initial GRACE — so a freeze is caught in minutes, not the 6h it took by hand.
# Also prints progress (hard-completions climbing, expired flat) and flags first re-energization.
#
# Local-only (podman rpk + redis lua) — zero cloud-SQL cost, safe to run hot. Every cloud stage (source-sim,
# datalake-ingest, out-produce) ultimately feeds a LOCAL topic, so a cloud-side freeze still shows as a flat
# local HWM here. Exit 2 = FROZEN (act now). Exit 0 = window elapsed healthy (relaunch to keep watching).
#
# Usage:  POLL_S=150 MAX_POLLS=30 bash nba/infra/nba-loop-watchdog.sh
set +e
REPO="C:/Users/Josh/source/repos/AIServices"; cd "$REPO" 2>/dev/null
PANDA=ais-nba-redpanda; REDIS=ais-nba-redis
PX(){ MSYS_NO_PATHCONV=1 podman "$@"; }
POLL_S=${POLL_S:-150}; MAX_POLLS=${MAX_POLLS:-30}; GRACE=${GRACE:-3}; STALL_NEED=${STALL_NEED:-2}
# the loop's spine — every stage's output lands in one of these LOCAL topics; total HWM strictly rises while flowing
TOPICS="datalake.streaming-inbound nba.member.facts nba.activations nba.evaluations nba.snapshots"

total_hwm(){ local s=0 t; for t in $TOPICS; do s=$((s + $(PX exec $PANDA rpk topic describe "$t" -p 2>/dev/null | awk 'NR>1{x+=$6} END{print x+0}'))); done; echo $s; }
endstate(){ PX exec $REDIS redis-cli eval '
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
return table.concat(r," ")' 0 2>/dev/null; }
hc(){ echo "$1" | grep -oE 'HARD_COMPLETED=[0-9]+' | cut -d= -f2; }

echo "### WATCHDOG start  poll=${POLL_S}s  window=$((POLL_S*MAX_POLLS/60))min  stall=${STALL_NEED}x past grace=${GRACE}"
prev=-1; stall=0; energized=0; first_hc=""
for p in $(seq 1 $MAX_POLLS); do
  cur=$(total_hwm); es=$(endstate); H=$(hc "$es"); H=${H:-0}; [ -z "$first_hc" ] && first_hc=$H
  d=$(( prev<0 ? 0 : cur-prev ))
  flag=""
  if [ "$prev" -ge 0 ] && [ "$p" -gt "$GRACE" ]; then
    if [ "$d" -le 0 ]; then stall=$((stall+1)); else stall=0; fi
  fi
  [ "$energized" -eq 0 ] && [ "$H" -gt "${first_hc:-0}" ] && { energized=1; flag=" <<RE-ENERGIZED (hard-completions climbing)"; }
  printf "[poll %02d] totalHWM=%s (+%s) stall=%s/%s | %s%s\n" "$p" "$cur" "$d" "$stall" "$STALL_NEED" "$es" "$flag"
  if [ "$stall" -ge "$STALL_NEED" ]; then
    echo "### FROZEN — totalHWM flat ${stall} polls (no flow across the spine). Loop is stalled — investigate source-sim / datalake-ingest / out-produce NOW."
    exit 2
  fi
  prev=$cur
  [ "$p" -lt "$MAX_POLLS" ] && sleep "$POLL_S"
done
echo "### window elapsed — loop healthy through $((POLL_S*MAX_POLLS/60))min (final HARD_COMPLETED=$(hc "$(endstate)")). Relaunch to keep watching."
exit 0
