#!/usr/bin/env bash
# fact -> snapshot-readable latency: classic (Redis nba:snapshot, written by snapshot-builder) vs KStreams
# (the nba-decision-engine's Interactive-Query endpoint). Produces a marked disposition fact (an alwaysAttach
# key, so BOTH paths snapshot it regardless of the rulefacts lean-filter) and times how long until each path
# can serve it back. Run with the engine in shadow, IQ published on :7020.
#
#   bash nba/infra/bench-snapshot-latency.sh [N]
#
# COARSE BY DESIGN: every poll is a podman-exec / curl (~tens of ms), so the absolute ms include poll overhead;
# the RELATIVE comparison + the poll-interval floor each path settles at are the signal, not the exact ms.
set -uo pipefail
N=${1:-20}
IQ=${NBA_IQ:-http://localhost:7020}
KEY="nba.disposition.latencytest.email"     # alwaysAttach -> snapshot-builder AND the engine both snapshot it
R(){ podman exec ais-nba-redis redis-cli "$@" 2>/dev/null; }
now(){ date +%s%3N; }
median(){ printf '%s\n' "$@" | sort -n | awk '{a[NR]=$1} END{if(NR==0){print "NA"}else{print (NR%2)?a[(NR+1)/2]:int((a[NR/2]+a[NR/2+1])/2)}}'; }
lohi(){ printf '%s\n' "$@" | sort -n | sed -n "1p;\$p" | paste -sd'/'; }

rs=(); ks=(); rfail=0; kfail=0
for i in $(seq 1 "$N"); do
  nba="nba_lat$(now)_$i"; ent="latent$(now)_$i"; ts=$(now)
  body="{\"entityType\":\"OPERATOR\",\"entityId\":\"$ent\",\"nbaId\":\"$nba\",\"key\":\"$KEY\",\"value\":\"Delivered\",\"valueType\":\"STRING\",\"eventTs\":$ts,\"source\":\"latbench\"}"
  t0=$(now)
  printf '%s|%s' "OPERATOR:$ent" "$body" | podman exec -i ais-nba-redpanda rpk topic produce nba.member.facts -f '%k|%v' >/dev/null 2>&1
  rT=0; kT=0
  while :; do
    # IQ checked FIRST this time (curl ~10ms) so it no longer rides behind the slower redis-cli podman-exec;
    # each detection timestamps independently (not a shared per-pass elapsed) so the two are actually separable.
    [ "$kT" -eq 0 ] && { curl -s "$IQ/snapshot/$nba" 2>/dev/null | grep -q "$KEY" && kT=$(( $(now) - t0 )); }
    [ "$rT" -eq 0 ] && { v=$(R hget "nba:snapshot:$nba" "fact:$KEY"); [ -n "$v" ] && rT=$(( $(now) - t0 )); }
    { [ "$rT" -ne 0 ] && [ "$kT" -ne 0 ]; } && break
    [ "$(( $(now) - t0 ))" -gt 8000 ] && break
  done
  [ "$rT" -ne 0 ] && rs+=("$rT") || rfail=$((rfail+1))
  [ "$kT" -ne 0 ] && ks+=("$kT") || kfail=$((kfail+1))
  printf '  iter %2d: redis=%6sms  kstreams=%6sms\n' "$i" "${rT}" "${kT}"
done

echo ""
echo "=== fact -> snapshot-readable (n=$N) ==="
echo "  classic  (Redis nba:snapshot): median=$(median "${rs[@]}")ms  min/max=$(lohi "${rs[@]}")  timeouts=$rfail"
echo "  kstreams (engine IQ):          median=$(median "${ks[@]}")ms  min/max=$(lohi "${ks[@]}")  timeouts=$kfail"
echo "  NOTE: ms include poll overhead; IQ (curl ~10ms) is polled BEFORE redis (podman-exec redis-cli ~30ms) each pass, so if anything a slight IQ-bias. Both are bounded by the shared consumer poll-cycle."
