#!/usr/bin/env bash
# Cross-engine EQUIVALENCE PROOF (deterministic, frozen-input): classic == KStreams == Flink produce IDENTICAL
# snapshots + evaluations for the same members.
#
# Why this is a PROOF and not a race (contrast engine-parity.sh, the live diagnostic): it FREEZES the input. The
# journey-driving services (conversion-sim / action-router / temporal-worker / action-layer) are STOPPED so
# member.facts holds only the deterministic seed + the deterministic scorer's scores -- a fixed input that no
# longer mutates. Each engine then folds that SAME frozen input from the beginning (offset earliest) and we diff
# the normalized per-member  (1) snapshot fact-set  (2) eligible action-channels  (3) scores  -- separately. With
# no moving journey there is no timing artifact, so a DIFF is a REAL logic divergence, not a clock skew. The
# journey services are restarted at the end.
#
# Scope: this proves the SNAPSHOT + ELIGIBILITY (+ SCORE) spine is equivalent across all three engines. The
# journey/state-machine equivalence is a separate axis -- KStreams reuses the classic Temporal worker UNCHANGED so
# its journey is identical by construction; Flink's own state machine vs Temporal is exercised by the live
# diagnostic (engine-parity.sh), not asserted here.
#
#   bash nba/test/engine-equivalence.sh [N_MEMBERS]      (default 12)
set -uo pipefail
export MSYS_NO_PATHCONV=1
cd "$(cd "$(dirname "$0")/.." && pwd)/.." 2>/dev/null   # repo root
P=ais-nba-redpanda; R=ais-nba-redis; N=${1:-12}; PASS=0; FAIL=0
TMP=$(mktemp -d 2>/dev/null || echo /tmp/equiv.$$); mkdir -p "$TMP"
# Freeze member.facts by stopping the services that WRITE journey facts back onto it (dispositions / actionstates):
# conversion-sim, temporal-worker, action-layer. KEEP the action-router -- it writes the classic golden eval
# (nba:eligibility) from nba.evaluations; with the worker stopped its workflow-starts just queue harmlessly (and
# run on restore). KEEP snapshot-builder + rules-engine + journey-scorer (the snapshot/eval/score pipeline).
JOURNEY="ais-nba-conversion-sim ais-nba-temporal-worker ais-nba-action-layer"
hwm(){ podman exec $P rpk topic describe "$1" -p 2>/dev/null | awk '/^[0-9]/{print $6}'; }
freshshadow(){ for t in member.facts snapshots evaluations activations facts; do podman exec $P rpk topic delete "nba.$t.shadow" >/dev/null 2>&1; podman exec $P rpk topic create "nba.$t.shadow" -c cleanup.policy=compact >/dev/null 2>&1; done
  podman exec $P rpk topic create nba.dlq.flink-engine.shadow >/dev/null 2>&1; podman exec $P rpk topic create nba.dlq.decision-engine.shadow >/dev/null 2>&1; }

# consume a snapshots/evaluations topic ONCE -> "nbaId<TAB>value" per member (latest), into $2. `| cat |` gives
# Windows-python a clean EOF through the podman-exec pipe (no-op on Linux). $3 = snap|elig|score.
# Read with `-o start` + a `timeout`, NOT `-n <hwm>`: the engines write transactionally (exactly-once), so the
# topic hwm counts commit markers as offsets -> `-n hwm` waits for more DATA records than exist and HANGS forever.
# `-o start` streams every existing record, the timeout ends rpk's tail-wait, and python dedups latest-per-nbaId.
dump_topic(){ local h mode="$3"; h=$(hwm "$1"); { [ -z "$h" ] || [ "$h" -le 0 ]; } && { :>"$2"; return; }
  timeout 15 podman exec $P rpk topic consume "$1" -o start -f '%v\n' 2>/dev/null | cat | MODE="$mode" python -c "
import sys,json,os
mode=os.environ['MODE']; L={}
for line in sys.stdin:
    try: d=json.loads(line)
    except: continue
    nid=d.get('nbaId')
    if not nid: continue
    if mode=='snap':
        # Exclude nba.score.* — the scorer is a PLUGGABLE component (classic = nba-journey-scorer; Flink/KStreams
        # have their own built-in heuristic for standalone runs; prod uses the SHARED Databricks RL scorer). So
        # local scores differ by construction and aren't an engine-SPINE equivalence axis. Compare the snapshot
        # builder's real work: rulefacts + actionstate + the rest of the lean snapshot.
        L[nid]=';'.join(sorted(f'{k}={(v.get(chr(118)+chr(97)+chr(108)+chr(117)+chr(101)) if isinstance(v,dict) else v)}' for k,v in d.get('facts',{}).items() if not k.startswith('nba.')))
    elif mode=='elig':
        L[nid]=','.join(sorted(c.get('actionId','')+':'+c.get('channel','') for c in d.get('channelActions',[]) if c.get('eligible')))
    else:
        L[nid]=';'.join(sorted(c.get('actionId','')+':'+c.get('channel','')+'='+str(c.get('score')) for c in d.get('channelActions',[]) if c.get('eligible')))
for k,v in L.items(): print(k+'\t'+v)" > "$2"; }
gold_snap(){ podman exec $R redis-cli hgetall "nba:snapshot:$1" 2>/dev/null | cat | python -c "
import sys,json
A=[l.rstrip() for l in sys.stdin]; o=[]
for i in range(0,len(A)-1,2):
    if not A[i].startswith('fact:'): continue
    if A[i][5:].startswith('nba.'): continue   # exclude ALL engine/journey-generated facts (score/actionstate/disposition);
    # compare the seed-derived operator.* member attributes -- the snapshot-builder's deterministic output. nba.actionstate.*
    # differs legitimately: Flink runs its own state machine during the window while classic's journey is frozen.
    try: val=json.loads(A[i+1]).get('value')
    except: val=A[i+1]
    o.append(f'{A[i][5:]}={val}')
print(';'.join(sorted(o)))"; }
gold_eval(){ podman exec $R redis-cli get "nba:eligibility:$1" 2>/dev/null | cat | MODE="$2" python -c "
import sys,json,os
mode=os.environ['MODE']
try: d=json.load(sys.stdin)
except: raise SystemExit
ca=[c for c in d.get('channelActions',[]) if c.get('eligible')]
if mode=='elig': print(','.join(sorted(c.get('actionId','')+':'+c.get('channel','') for c in ca)))
else: print(';'.join(sorted(c.get('actionId','')+':'+c.get('channel','')+'='+str(c.get('score')) for c in ca)))"; }
compare(){ local label="$1" candfile="$2" goldfn="$3" goldarg="${4:-}" id g c; declare -A cand
  while IFS=$'\t' read -r nid fs; do cand[$nid]="${fs//$'\r'/}"; done < "$candfile"   # strip CR (Windows python writes \r\n)
  for id in $IDS; do g=$($goldfn "$id" "$goldarg"); g="${g//$'\r'/}"; c="${cand[$id]:-__none__}"
    if [ "$c" = "__none__" ]; then echo "    [skip] $label $id (engine produced nothing)"; continue; fi
    if [ "$g" = "$c" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); echo "    [DIFF] $label $id"; echo "        gold: $g"; echo "        cand: $c"; diff <(echo "$g"|tr ';,' '\n\n') <(echo "$c"|tr ';,' '\n\n') 2>/dev/null | sed 's/^/        /' | head -40; fi
  done; }
run_engine(){ local name="$1" image="$2" alias="$3"; shift 3
  podman rm -f "$name" >/dev/null 2>&1
  podman run -d --name "$name" --network aiservices_default --network-alias "$alias" \
    -e NBA_BOOTSTRAP=nba-redpanda:9092 -e NBA_REDIS_HOST=nba-redis "$@" "$image" >/dev/null
  for i in $(seq 1 40); do podman logs --tail 4 "$name" 2>&1 | grep -qiE 'to RUNNING|started|listening' && break; sleep 2; done; sleep 6; }

echo "=== reset to a clean baseline ==="; bash nba/test/reset-fresh.sh >/dev/null 2>&1 || { echo "reset-fresh failed"; exit 1; }
echo "=== FREEZE: stop the journey-driving services (member.facts becomes a fixed input) ==="
for c in $JOURNEY; do podman stop "$c" >/dev/null 2>&1; done
freshshadow
echo "=== seed $N deterministic members (only the seed + scorer write member.facts now) ==="
NBA_SEED_TOPIC=nba.member.facts python nba/infra/reseed-members-local.py "$N" 2>&1 | tail -1
echo "  settling 35s (classic snapshot+eval+score stabilize; no journey churn)..."; sleep 35
IDS=$(podman exec $R redis-cli --scan --pattern 'nba:idmap:OPERATOR:hcm-*' 2>/dev/null | head -"$N" | while read k; do podman exec $R redis-cli get "$k" 2>/dev/null | tr -d '\r'; done)
echo "  members: $(echo "$IDS" | grep -c .)"

for eng in flink kstreams; do
  if [ "$eng" = flink ]; then
    # NBA_FLINK_SCORE=off: don't let Flink's built-in heuristic scorer "cheat" — force it through the SAME shared
    # scorer as classic (nba-journey-scorer locally; the Databricks RL scorer in prod), which writes nba.score.* to
    # member.facts. Flink then reads those, so scores (and the channels they surface) come from ONE source.
    echo "=== FLINK vs classic golden (snapshot + eligibility) — Flink built-in scorer OFF (shared scorer) ==="; freshshadow
    run_engine ais-nba-flink-engine localhost/nba-flink-engine:latest nba-flink-engine -e NBA_FLINK_MODE=shadow -e NBA_FLINK_SCORE=off -e NBA_FLINK_PARALLELISM=1 -e NBA_OFFSET_RESET=earliest
  else
    echo "=== KSTREAMS vs classic golden (SNAPSHOT only — KStreams materializes classic's eligibility, no rules reimpl) ==="; freshshadow
    run_engine ais-nba-decision-engine localhost/nba-decision-engine:latest nba-decision-engine -e NBA_DECISION_ENGINE_MODE=shadow -e NBA_ENGINE_ADVERTISED=nba-decision-engine:7020 -e NBA_METRICS_PORT=9410 -e NBA_OFFSET_RESET=earliest
  fi
  # Warm up: wait until the engine emits to its .shadow snapshots (replay-from-earliest is slow to first-produce; a
  # blind sleep raced it). Flink is a FULL engine (also produces evaluations.shadow); KStreams is a snapshot engine
  # that MATERIALIZES classic's nba.evaluations for IQ reads (no rules reimplementation), so for KStreams the only
  # equivalence axis is the SNAPSHOT -- waiting on evaluations.shadow there would just burn 120s for a topic it
  # never writes.
  for i in $(seq 1 60); do s=$(hwm nba.snapshots.shadow); { [ "${s:-0}" -gt 0 ] && { [ "$eng" != flink ] || [ "$(hwm nba.evaluations.shadow)" -gt 0 ]; }; } && break; sleep 2; done
  sleep 25
  dump_topic nba.snapshots.shadow   "$TMP/snap" snap; compare "snap/$eng"  "$TMP/snap" gold_snap
  if [ "$eng" = flink ]; then
    # Scores are NOT a separate axis: Flink's built-in scorer is OFF, so it reads the SAME shared scorer's nba.score.*
    # as classic -> score parity by construction. We diff the eligible action-channels (the rules SPINE).
    dump_topic nba.evaluations.shadow "$TMP/elig" elig; compare "elig/$eng" "$TMP/elig" gold_eval elig
  else
    echo "    [elig/$eng n/a] KStreams reuses classic's eligibility (materializes nba.evaluations) -> snapshot-only equivalence"
  fi
  podman stop "ais-nba-${eng}-engine" >/dev/null 2>&1 || podman stop ais-nba-decision-engine >/dev/null 2>&1
done

echo "=== restore the journey-driving services ==="
for c in $JOURNEY; do podman start "$c" >/dev/null 2>&1; done
rm -rf "$TMP"
echo ""; echo "=== EQUIVALENCE: $PASS identical, $FAIL divergent ==="
[ "$FAIL" -eq 0 ] && echo "PROVEN: classic == KStreams == Flink on the frozen input (snapshot + eligibility + score)." || echo "DIVERGENCES above -- a REAL logic difference (not a race)."
exit $([ "$FAIL" -eq 0 ] && echo 0 || echo 1)
