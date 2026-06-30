#!/usr/bin/env bash
# Cross-engine PARITY — live shadow-diff of the three spine implementations' snapshots/evals per member.
#
#   classic (snapshot-builder + rules-engine) = golden (Redis nba:snapshot / nba:eligibility)
#   Flink (nba-flink-engine)  -> snapshots + evaluations    KStreams (nba-decision-engine) -> snapshots
#
# !!! IMPORTANT — this is a SMOKE/DIAGNOSTIC, not a clean equivalence proof. !!!
# Run against the LIVE stack it measures a TIMING RACE, not logic divergence: the classic golden is a MOVING TARGET
# (the journey keeps progressing — score/actionstate/disposition/completion facts keep landing), while a shadow
# engine captures a DIFFERENT moment (Flink at the live edge lags; KStreams on `earliest` is mid-catch-up reprocessing
# the whole topic). So derived facts (e.g. an in-flight score of ~-49 vs a settled ~12) legitimately differ by
# journey-point. A run here showed 0/14 "match" for exactly this reason — the engines are NOT proven to disagree.
#
# For a RIGOROUS cross-engine equivalence proof, feed a FIXED golden fact-set to each engine's snapshot/rules logic
# and assert identical output (the per-engine SnapshotLogicTest / SnapshotStageTest / SnapshotBuilderTest already do
# this in isolation — a shared cross-engine golden fixture is the proper next step). Or freeze the input (stop the
# journey-driving services so member.facts holds only the deterministic seed, then diff the base-fact snapshots).
#
# Use this script as a settled-stack sanity check; read diffs as "investigate", not "fail".
#   bash nba/test/engine-parity.sh [N_MEMBERS]      (default 12)
set -uo pipefail
export MSYS_NO_PATHCONV=1
cd "$(cd "$(dirname "$0")/.." && pwd)/.." 2>/dev/null   # repo root
P=ais-nba-redpanda; R=ais-nba-redis; N=${1:-12}; PASS=0; FAIL=0
TMP=$(mktemp -d 2>/dev/null || echo /tmp/parity.$$); mkdir -p "$TMP"
hwm(){ podman exec $P rpk topic describe "$1" -p 2>/dev/null | awk '/^[0-9]/{print $6}'; }
freshshadow(){ for t in member.facts snapshots evaluations activations facts; do podman exec $P rpk topic delete "nba.$t.shadow" >/dev/null 2>&1; podman exec $P rpk topic create "nba.$t.shadow" -c cleanup.policy=compact >/dev/null 2>&1; done
  podman exec $P rpk topic create nba.dlq.flink-engine.shadow >/dev/null 2>&1; podman exec $P rpk topic create nba.dlq.decision-engine.shadow >/dev/null 2>&1; }

# consume a snapshots/evaluations topic ONCE -> "nbaId<TAB>normalized" per member (latest), into $2
dump_snaps(){ local h; h=$(hwm "$1"); { [ -z "$h" ] || [ "$h" -le 0 ]; } && { :>"$2"; return; }
  podman exec $P rpk topic consume "$1" -o start -n "$h" -f '%v\n' 2>/dev/null | cat | python -c "
import sys,json
L={}
for line in sys.stdin:
    try: d=json.loads(line)
    except: continue
    nid=d.get('nbaId')
    if not nid: continue
    L[nid]=';'.join(sorted(f'{k}={(v.get(chr(118)+chr(97)+chr(108)+chr(117)+chr(101)) if isinstance(v,dict) else v)}' for k,v in d.get('facts',{}).items()))
for k,v in L.items(): print(k+'\t'+v)" > "$2"; }
dump_evals(){ local h; h=$(hwm "$1"); { [ -z "$h" ] || [ "$h" -le 0 ]; } && { :>"$2"; return; }
  podman exec $P rpk topic consume "$1" -o start -n "$h" -f '%v\n' 2>/dev/null | cat | python -c "
import sys,json
L={}
for line in sys.stdin:
    try: d=json.loads(line)
    except: continue
    nid=d.get('nbaId')
    if not nid: continue
    L[nid]=';'.join(sorted(f\"{c.get('actionId')}:{c.get('channel')}=e{c.get('eligible')},s{c.get('score')}\" for c in d.get('channelActions',[])))
for k,v in L.items(): print(k+'\t'+v)" > "$2"; }
# classic golden (Redis), per id
gold_snap(){ podman exec $R redis-cli hgetall "nba:snapshot:$1" 2>/dev/null | python -c "
import sys,json
A=[l.rstrip(chr(13)) for l in sys.stdin]; o=[]
for i in range(0,len(A)-1,2):
    if not A[i].startswith('fact:'): continue
    try: val=json.loads(A[i+1]).get('value')
    except: val=A[i+1]
    o.append(f'{A[i][5:]}={val}')
print(';'.join(sorted(o)))"; }
gold_eval(){ podman exec $R redis-cli get "nba:eligibility:$1" 2>/dev/null | python -c "
import sys,json
try: d=json.load(sys.stdin)
except: raise SystemExit
print(';'.join(sorted(f\"{c.get('actionId')}:{c.get('channel')}=e{c.get('eligible')},s{c.get('score')}\" for c in d.get('channelActions',[]))))"; }
compare(){ local label="$1" candfile="$2" goldfn="$3" id g c; declare -A cand
  while IFS=$'\t' read -r nid fs; do cand[$nid]="$fs"; done < "$candfile"
  for id in $IDS; do g=$($goldfn "$id"); c="${cand[$id]:-__none__}"
    if [ "$c" = "__none__" ] || [ -z "$c" ]; then echo "    [skip] $label $id (engine produced nothing)"; continue; fi
    if [ "$g" = "$c" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); echo "    [DIFF] $label $id"; diff <(echo "$g"|tr ';' '\n') <(echo "$c"|tr ';' '\n') 2>/dev/null | sed 's/^/        /' | head -8; fi
  done; }
start_flink(){ podman rm -f ais-nba-flink-engine >/dev/null 2>&1
  podman run -d --name ais-nba-flink-engine --network aiservices_default --network-alias nba-flink-engine \
    -e NBA_BOOTSTRAP=nba-redpanda:9092 -e NBA_REDIS_HOST=nba-redis -e NBA_FLINK_MODE=shadow -e NBA_FLINK_PARALLELISM=1 \
    localhost/nba-flink-engine:latest >/dev/null
  for i in $(seq 1 30); do podman logs --tail 4 ais-nba-flink-engine 2>&1 | grep -q 'to RUNNING' && break; sleep 2; done; sleep 4; }

# Reset to a fresh baseline first (small topics = fast consumes, no residue/old-journey carry-over). Skip with
# NBA_TEST_NO_RESET=1 if the stack is already clean.
if [ "${NBA_TEST_NO_RESET:-0}" != "1" ]; then bash "$(dirname "$0")/reset-fresh.sh" || exit 1; fi
echo "=== fresh .shadow topics + start Flink (shadow) before seeding ==="; freshshadow; start_flink
echo "=== seed $N deterministic members ==="
NBA_SEED_TOPIC=nba.member.facts python nba/infra/reseed-members-local.py "$N" 2>&1 | tail -1
echo "  settling 35s..."; sleep 35
IDS=$(podman exec $R redis-cli --scan --pattern 'nba:idmap:OPERATOR:hcm-*' 2>/dev/null | head -"$N" | while read k; do podman exec $R redis-cli get "$k" 2>/dev/null | tr -d '\r'; done)
echo "  members: $(echo "$IDS" | grep -c .)"
echo "=== classic golden vs FLINK ==="
dump_snaps nba.snapshots.shadow   "$TMP/f_snap"; compare "snap/flink" "$TMP/f_snap" gold_snap
dump_evals nba.evaluations.shadow "$TMP/f_eval"; compare "eval/flink" "$TMP/f_eval" gold_eval
podman stop ais-nba-flink-engine >/dev/null 2>&1

echo "=== classic golden vs KSTREAMS (snapshots; earliest reprocesses the seeded members) ==="
freshshadow
podman rm -f ais-nba-decision-engine >/dev/null 2>&1
podman run -d --name ais-nba-decision-engine --network aiservices_default --network-alias nba-decision-engine \
  -e NBA_BOOTSTRAP=nba-redpanda:9092 -e NBA_REDIS_HOST=nba-redis -e NBA_DECISION_ENGINE_MODE=shadow \
  -e NBA_ENGINE_ADVERTISED=nba-decision-engine:7020 -e NBA_METRICS_PORT=9410 -e NBA_OFFSET_RESET=earliest \
  localhost/nba-decision-engine:latest >/dev/null
sleep 45
dump_snaps nba.snapshots.shadow "$TMP/k_snap"; compare "snap/kstreams" "$TMP/k_snap" gold_snap
podman stop ais-nba-decision-engine >/dev/null 2>&1
rm -rf "$TMP"
echo ""; echo "=== PARITY: $PASS ok, $FAIL diff ==="
[ "$FAIL" -eq 0 ] && echo "All engines match the classic golden." || echo "Divergences above."
exit $([ "$FAIL" -eq 0 ] && echo 0 || echo 1)
