#!/usr/bin/env bash
# fact -> router-decision latency. Seeds a fresh, qualifying member (the t_eligibility_basic fact set) and times
# from the member.facts produce to the action-router emitting its decision ("op":"CREATE"/"CREATE_BATCH",
# kind=router) on nba.member.facts. Spans the whole async spine: fact -> snapshot -> rules eval -> router decision
# (the FIRST CREATE doesn't wait on the dbx scorer — it activates the top eligible action). Works pre/post cutover.
#
#   bash nba/infra/bench-router-latency.sh [N]
#
# COARSE: the router CREATE is detected by tailing nba.member.facts (a background rpk consumer) + grep, so the
# detection granularity is the poll interval (~50ms) + tap lag; the RELATIVE classic-vs-kstreams comparison is the signal.
set -uo pipefail
N=${1:-10}
now(){ date +%s%3N; }
median(){ printf '%s\n' "$@" | sort -n | awk '{a[NR]=$1} END{if(NR==0){print"NA"}else{print (NR%2)?a[(NR+1)/2]:int((a[NR/2]+a[NR/2+1])/2)}}'; }
lohi(){ printf '%s\n' "$@" | sort -n | sed -n "1p;\$p" | paste -sd'/'; }

# healthcare eligibility set (copied from a real eligible diabetic member -> ~7 eligible channelActions)
QUAL="operator.profile.diabetic:true:BOOL operator.profile.isDNC:false:BOOL operator.activity.respondedToOutreach:true:BOOLEAN operator.activity.daysSinceLogin:0:LONG operator.activity.hraCompleted:true:BOOLEAN operator.activity.pcpSelected:true:BOOLEAN operator.activity.careTeamEngaged:true:BOOLEAN operator.comms.totalThisWeek:0:LONG operator.comms.emailsThisWeek:0:LONG"

TAP=$(mktemp)
podman exec ais-nba-redpanda rpk topic consume nba.member.facts -o end -f '%v\n' >"$TAP" 2>/dev/null &
TAPPID=$!
trap 'kill $TAPPID 2>/dev/null; rm -f "$TAP"' EXIT
sleep 3   # let the tap attach at the live edge

lat=(); fail=0
for i in $(seq 1 "$N"); do
  e="rtr$(now)x$i"; ts=$(now)
  { for kv in $QUAL; do IFS=: read -r k v ty <<<"$kv"
      echo "OPERATOR:$e|{\"entityType\":\"OPERATOR\",\"entityId\":\"$e\",\"key\":\"$k\",\"value\":$v,\"valueType\":\"$ty\",\"eventTs\":$ts,\"source\":\"rtrbench\"}"
    done; } | podman exec -i ais-nba-redpanda rpk topic produce nba.member.facts -f '%k|%v' >/dev/null 2>&1
  t0=$(now); d=0
  while :; do
    grep "$e" "$TAP" 2>/dev/null | grep -q '"op":"CREATE' && { d=$(( $(now) - t0 )); break; }
    [ "$(( $(now) - t0 ))" -gt 20000 ] && break
    sleep 0.05
  done
  if [ "$d" -ne 0 ]; then lat+=("$d"); else fail=$((fail+1)); fi
  printf '  iter %2d: %6sms %s\n' "$i" "$d" "$([ "$d" -eq 0 ] && echo '(timeout)')"
done

echo ""
echo "=== fact -> router decision (CREATE), n=$N ==="
echo "  median=$(median "${lat[@]}")ms  min/max=$(lohi "${lat[@]}")  timeouts=$fail"
