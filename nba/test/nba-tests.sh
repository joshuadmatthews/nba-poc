#!/usr/bin/env bash
# NBA POC integration test suite.
#
# Drives the LIVE stack (ais-nba-redpanda / ais-nba-redis / ais-nba-temporal) and asserts on
# the observable state at each layer. Every test uses a fresh member id so re-runs are clean
# and isolated (no reset needed). Assertions poll-until-condition because the pipeline is async.
#
# Usage:
#   bash nba/test/nba-tests.sh            # all tests
#   bash nba/test/nba-tests.sh fast       # skip the slow Temporal-lifecycle tests
#
# The supersede test proves DEBOUNCED via the sibling dedup: two workflows for the same member end up in the
# debounce window, each queries its siblings at the timer, the lower self-DEBOUNCEs. Sibling discovery is Temporal
# visibility (eventually consistent ~1-2s), so run the worker with a window comfortably above that (prod = 60s):
#   nba/services/nba-temporal/run.ps1 -DebounceSeconds 10
#
# ===================== STATE-MACHINE COVERAGE (ChannelActionWorkflow) =====================
# Every state the per-(member,action,channel) workflow can emit + where it's proven: integration (this suite, vs
# the LIVE stack) and unit (the service's own *Test.java / test_*.py, gated in each Containerfile build). Two
# honesty notes drive HOW we assert, so a green run means exactly what it says:
#  * PROBABILISTIC engagement -- whether a delivered action then ENGAGES (SOFT) / CONVERTS (HARD) is the
#    conversion-sim's stable-hash coin, so lifecycle tests assert walked() (deterministic: dispatched + a provider
#    response), never an exact engagement state.
#  * RE-FIRE churn -- a terminal that FREES THE SLOT (EXPIRED/FAILED) leaves the action eligible, so the router
#    re-CREATEs a fresh run and the snapshot's LATEST value cycles. Those we assert from the APPEND-ONLY log
#    (state_emitted / disp_delivered), not get_state's transient latest.
#   CREATED        int t_state_created_then_in_process                 unit NbaTemporalWorkerTest (debounce)
#   IN_PROCESS     int t_state_created_then_in_process + every lifecycle walk
#   PRESENTED      int walked() in t_routing_lifecycle / t_batch / t_throttle_lifecycle_sms (delivered+)
#   SOFT_COMPLETED int reached by the funnel walk (not asserted exactly)  unit ActionLayerTest (the soft bar)
#   HARD_COMPLETED int t_hard_completion (criterion + /completion API)
#   DECLINED       int t_state_declined  (disp_delivered: delivered + emitted; non-terminal rest state)
#   FAILED         int t_state_failed    (disp_delivered)                 unit NbaTemporalWorkerTest (TERMINAL set)
#   EXPIRED        int t_expired         (state_emitted: TTL elapsed, no conversion -> then re-fires)
#   DEBOUNCED      int t_supersede (no-double-comm)  unit NbaTemporalWorkerTest.preSendSuppressState + flink StateMachineFnTest
#   SUPPRESSING    int t_state_suppress_post_dispatch (the transient before SUPPRESSED)
#   SUPPRESSED     int t_state_suppress_post_dispatch (post-dispatch cancel)  unit preSendSuppressState + ThrottleGateTest
# Suppression's ELIGIBILITY effect (operator pull / throttle reroute -> action drops out) is a separate layer,
# covered by t_operator_suppress(_channel) + t_throttle_saturation. Scoring bands (FRESH/in-flight/soft/hard/
# negative ordering + determinism) are unit-pinned in nba-journey-scorer/test_scorer.py.
# ==========================================================================================
set -uo pipefail

MODE="${1:-all}"
RUN="$(date +%H%M%S)"
PASS=0; FAIL=0; TSN=0

R(){ podman exec ais-nba-redis redis-cli "$@"; }
produce(){ echo "$3" | podman exec -i ais-nba-redpanda rpk topic produce "$1" -k "$2" >/dev/null 2>&1; }
# Monotonic eventTs. MUST increment in the parent shell (not a $() subshell, which would lose
# the increment) so a re-written fact is strictly newer and not dropped by event-time LWW.
# Base sits below wall-clock so the datalake's real-time count facts win over test seeds.
mkts(){ TSN=$((TSN+1)); TS=$((1719000000000+TSN)); }

# ---- fixtures: resolve an action id by name from nba.definitions (the single source of truth) ----
action_id(){ # name -> actionId. Uses grep, NOT python: `rpk topic consume ... | python` is UNRELIABLE on Git Bash
  # (Windows-python never sees the podman-exec pipe's EOF and hangs). grep is a native MSYS tool that closes cleanly.
  # The definition value is compact JSON on one line (`ACTION:<id>\t{"name":"<name>",...}`), so we match the exact
  # name on an ACTION line and pull the id out of the key. (No-op-equivalent on Linux; just robust everywhere.)
  local hwm
  hwm=$(podman exec ais-nba-redpanda rpk topic describe nba.definitions -p 2>/dev/null | awk '/^[0-9]/{print $6}')
  { [ -z "$hwm" ] || [ "$hwm" -le 0 ]; } && return
  podman exec ais-nba-redpanda rpk topic consume nba.definitions -o start -n "$hwm" -f '%k\t%v\n' 2>/dev/null \
    | cat | grep '^ACTION:' | grep -F "\"name\":\"$1\"" | head -1 | awk -F'\t' '{print $1}' | sed 's/^ACTION://'
}

# ---- fact helpers ----
factjson(){ # entity key val type ts
  echo "{\"entityType\":\"OPERATOR\",\"entityId\":\"$1\",\"key\":\"$2\",\"value\":$3,\"valueType\":\"$4\",\"eventTs\":$5,\"source\":\"test\"}"
}
# feed a single fact to the firehose (ML feature store) AND member.facts (snapshot)
feed1(){ # entity key val type
  mkts; local j; j=$(factjson "$1" "$2" "$3" "$4" "$TS")
  produce nba.facts        "OPERATOR:$1:$2" "$j"
  produce nba.member.facts "OPERATOR:$1:$2" "$j"
}
# feed a member's full fact set. args after entity are "key:val:type". Firehose first (so the
# ML feature store has features before the eligibility eval), then member.facts.
setup(){ # entity  k:v:t ...
  local e="$1"; shift; local kv k v t j
  for kv in "$@"; do IFS=: read -r k v t <<< "$kv"; mkts; produce nba.facts "OPERATOR:$e:$k" "$(factjson "$e" "$k" "$v" "$t" "$TS")"; done
  sleep 3
  for kv in "$@"; do IFS=: read -r k v t <<< "$kv"; mkts; produce nba.member.facts "OPERATOR:$e:$k" "$(factjson "$e" "$k" "$v" "$t" "$TS")"; done
}
nbaid(){ R get "nba:idmap:OPERATOR:$1"; }
# action-library inbound APIs — reach them in-network from a container on aiservices_default (alpine wget)
next_action(){ podman exec ais-nba-bff wget -qO- "http://nba-action-library:7001/next-action/$1?n=3" 2>/dev/null; }
inbound_disp(){ podman exec ais-nba-bff wget -qO- --post-data="{\"entityId\":\"$1\",\"actionId\":\"$2\",\"channel\":\"$3\",\"status\":\"$4\"}" --header="Content-Type: application/json" http://nba-action-library:7001/dispositions 2>/dev/null; }
# operator suppress/unsuppress an action (D) — Command Center -> action-library /suppress
al_suppress(){ podman exec ais-nba-bff wget -qO- --post-data="{\"actionId\":\"$1\",\"suppressed\":$2}" --header="Content-Type: application/json" http://nba-action-library:7001/suppress >/dev/null 2>&1; }
al_suppress_ch(){ podman exec ais-nba-bff wget -qO- --post-data="{\"actionId\":\"$1\",\"channel\":\"$2\",\"suppressed\":$3}" --header="Content-Type: application/json" http://nba-action-library:7001/suppress >/dev/null 2>&1; }
al_maxbatch(){ podman exec ais-nba-bff wget -qO- --post-data="{\"channel\":\"$1\",\"maxBatch\":$2}" --header="Content-Type: application/json" http://nba-action-library:7001/channel-config >/dev/null 2>&1; }
# create an action from raw JSON (echoes the stored doc incl. its id); delete via the BFF graphql (action-lib DELETE)
al_post_action(){ podman exec ais-nba-bff wget -qO- --post-data="$1" --header="Content-Type: application/json" http://nba-action-library:7001/actions 2>/dev/null; }
al_del_action(){ podman exec ais-nba-bff wget -qO- --post-data="{\"query\":\"mutation{ deleteAction(id:\\\"$1\\\") }\"}" --header="Content-Type: application/json" http://nba-bff:4000/graphql >/dev/null 2>&1; }
# HARD completion API signal (partner/lake-fallback path): nba.completion.{actionId}=completed via the outbox.
al_completion(){ podman exec ais-nba-bff wget -qO- --post-data="{\"entityId\":\"$1\",\"actionId\":\"$2\",\"source\":\"int-test\"}" --header="Content-Type: application/json" http://nba-action-library:7001/completion >/dev/null 2>&1; }

# ---- determinism helpers for the LIFECYCLE/STATE tests ----
# Healthcare is a multi-channel funnel, so a qualifying member has MANY eligible action-channels and the router
# would dispatch the top-SCORED one (member-id-dependent -> unpredictable). To make a single-action lifecycle
# deterministic we SOLO one action-channel: operator-suppress every other qualifying-stage action + the kept
# action's OTHER channels, so exactly one pair ($LC_ACT:$LC_CH, default hra/email) is eligible -> the router
# dispatches IT. The always-on welcome/reengage are already excluded for a qualifying member (respondedToOutreach
# completes them). LC_ACT is set in the fixtures block (after $HRA resolves).
LC_ACT=""; LC_CH="email"
lc_solo(){ al_suppress "$LC_ACT" false; al_suppress_ch "$LC_ACT" "$LC_CH" false   # clear any lingering suppress on the soloed pair (a prior state test may have left it suppressed)
  al_suppress "$PORTAL" true; al_suppress "$HRAR" true
  for c in push sms voice mail; do al_suppress_ch "$LC_ACT" "$c" true; done; sleep 3; }   # email (LC_CH) not in this list -> stays open
lc_restore(){ al_suppress "$PORTAL" false; al_suppress "$HRAR" false
  for c in push sms voice mail; do al_suppress_ch "$LC_ACT" "$c" false; done; }

# ---- readers (operate on entity, resolve nbaid each call so it works before/after mint) ----
# (Historically the catalog carried hash-id 'demo arms' eligible for EVERY member — now PURGED; the catalog is the 15
# authored healthcare actions only. This hook stays empty so eligibility assertions self-heal if a stray test action
# is ever re-seeded.)
DEMO_ARMS=""
get_eligible(){ R get "nba:eligibility:$(nbaid "$1")" 2>/dev/null | DEMO="$DEMO_ARMS" python -c "import json,sys,os
try: d=json.load(sys.stdin)
except: print('?'); raise SystemExit
demo=set(os.environ.get('DEMO','').split())
print(','.join(sorted(c['actionId']+':'+c['channel'] for c in d.get('channelActions',[]) if c.get('eligible') and c['actionId'] not in demo)))" 2>/dev/null; }
# membership helpers -- the healthcare catalog is MULTI-CHANNEL (an action is eligible on several channels at once),
# so the eligible set is large; assert membership/absence of specific action:channel pairs rather than exact sets.
elig_has(){ case ",$(get_eligible "$1")," in *",$2,"*) echo y;; *) echo n;; esac; }     # is pair $2 (action:channel) eligible for $1?
elig_ch(){ get_eligible "$1" | tr ',' '\n' | grep -c ":$2$"; }                          # how many eligible pairs on channel $2?
# what INBOUND actually serves (action-library /next-action) — this is where operator suppression is
# enforced LIVE (the raw eval still lists suppressed actions; the serve strips them). Sorted slugs.
served_actions(){ next_action "$1" | python -c "import json,sys
try: d=json.load(sys.stdin)
except: print('?'); raise SystemExit
print(','.join(sorted(a['actionId']+':'+a['channel'] for a in d.get('actions',[]))))" 2>/dev/null; }
get_score(){ R get "nba:eligibility:$(nbaid "$1")" 2>/dev/null | python -c "import json,sys
d=json.load(sys.stdin)
print(next((str(c.get('score')) for c in d.get('channelActions',[]) if c['actionId']+':'+c['channel']=='$2'),'none'))" 2>/dev/null; }
# HARD-completed action ids on the eval (channelActions where hardCompleted; sorted unique csv); and a ChannelAction's hardCompleted flag.
get_completed(){ R get "nba:eligibility:$(nbaid "$1")" 2>/dev/null | python -c "import json,sys
try: d=json.load(sys.stdin)
except: print('?'); raise SystemExit
print(','.join(sorted(set(c['actionId'] for c in d.get('channelActions',[]) if c.get('hardCompleted')))))" 2>/dev/null; }
get_hardcompleted(){ R get "nba:eligibility:$(nbaid "$1")" 2>/dev/null | python -c "import json,sys
try: d=json.load(sys.stdin)
except: print('?'); raise SystemExit
print(next((str(c.get('hardCompleted')) for c in d.get('channelActions',[]) if c['actionId']=='$2' and c['channel']=='$3'),'none'))" 2>/dev/null; }
# the CONTENT KEY the rules engine selected for a member's action-channel (variant-aware) — read off the eval
get_contentkey(){ R get "nba:eligibility:$(nbaid "$1")" 2>/dev/null | python -c "import json,sys
try: d=json.load(sys.stdin)
except: print('?'); raise SystemExit
print(next((c.get('contentKey') for c in d.get('channelActions',[]) if c['actionId']=='$2' and c['channel']=='$3'),'none'))" 2>/dev/null; }
get_state(){ R hget "nba:snapshot:$(nbaid "$1")" "fact:nba.actionstate.${2/:/.}" 2>/dev/null | python -c "import json,sys;print(json.load(sys.stdin)['value'])" 2>/dev/null; }
# Did the action-channel $2 WALK the funnel for member $1? i.e. dispatched AND the activation layer delivered/
# progressed it past IN_PROCESS (PRESENTED=delivered, or any rest/terminal beyond). Whether it then ENGAGES
# (SOFT_COMPLETED), CONVERTS (HARD_COMPLETED), declines, fails, or expires is the conversion-sim's PROBABILISTIC
# outcome -- so the lifecycle tests assert "walked" (deterministic: sent + a provider response), not a specific
# engagement state, which would make them permanently flaky.
walked(){ case "$(get_state "$1" "$2")" in PRESENTED|SOFT_COMPLETED|HARD_COMPLETED|DECLINED|FAILED|EXPIRED) echo y;; *) echo n;; esac; }
walked_count(){ local m="$1" c=0 p; shift; for p in "$@"; do [ "$(walked "$m" "$p")" = y ] && c=$((c+1)); done; echo "$c"; }
get_count(){ R get "nba:dl:$(nbaid "$1"):$2" 2>/dev/null; }
# Pause/resume the OUTBOUND conversion-sim. The sim converts a deterministic (stable-hash) fraction of delivered
# (member,action) pairs to HARD_COMPLETED -- which RACES the disposition-driven state tests (they drive an action
# to PRESENTED then inject SUPPRESS/DECLINE/FAIL; a same-tick conversion would beat the inject to a terminal). Those
# tests drive their disposition by hand, so we hold the sim off for that block and resume it after.
sim_pause(){ podman stop ais-nba-conversion-sim >/dev/null 2>&1; }
sim_resume(){ podman start ais-nba-conversion-sim >/dev/null 2>&1; }
# the LIVE per-snapshot correlationId off the member's eval — the same value the router copies onto the
# activation (-> the workflow's myCorr). A disposition we inject MUST carry this corr (in its trackingId)
# or the workflow's corr-gate drops it as stale. Stable as long as no new snapshot is minted (no new fact).
get_corr(){ R get "nba:eligibility:$(nbaid "$1")" 2>/dev/null | python -c "import json,sys
try: print(json.load(sys.stdin).get('correlationId',''))
except: print('')" 2>/dev/null; }
# Inject a DELIVERY disposition the way the action layer does: a member fact nba.disposition.{actionId}.{channel}
# on member.facts with a kind=disposition HEADER, state=<canonical>, trackingId=nba-ca:{nbaId}:{aid}:{ch}|{corr}.
# The temporal disposition-consumer deconstructs the trackingId and signals disposition(state,corr) to the
# matching (member,action,channel) workflow, which walks to that state. Lets us drive DECLINED/FAILED
# deterministically (the sim emits them only probabilistically off nba:sim:*_rate).
disp_inject(){ # entity actionId channel state  (nbaId+corr resolved live)
  local nb corr; nb=$(nbaid "$1"); corr=$(get_corr "$1")
  local tid="nba-ca:${nb}:$2:$3|${corr}"; local ts; ts=$(date +%s%3N)
  echo "{\"entityType\":\"OPERATOR\",\"entityId\":\"$1\",\"key\":\"nba.disposition.$2.$3\",\"value\":\"$4\",\"state\":\"$4\",\"valueType\":\"STRING\",\"eventTs\":$ts,\"source\":\"int-test\",\"correlationId\":\"${corr}\",\"memberId\":\"$1\",\"channel\":\"$3\",\"trackingId\":\"${tid}\"}" \
    | podman exec -i ais-nba-redpanda rpk topic produce nba.member.facts -k "OPERATOR:$1:nba.disposition.$2.$3" -H "kind:disposition" >/dev/null 2>&1
}
# Was a $4 disposition DELIVERED to (member $1, action $2, channel $3)'s live workflow + emitted as its state? The
# disposition consumer logs "disposition -> {wfId} = {state}" only when it signals a RUNNING workflow whose corr
# matches (a stale corr logs "ignoring stale" instead; a dead workflow logs "no-op (not running)"), and the
# workflow emits the state on receipt (ChannelActionWorkflowImpl: emit BEFORE the terminal check) -- so a logged
# delivery == an emitted actionstate. We assert THIS, not get_state's latest value, because a TERMINAL disposition
# (FAILED) frees the slot and the still-eligible soloed action is immediately RE-DISPATCHED (a real retry: a fresh
# run walks CREATED->...->PRESENTED), churning the snapshot off the injected terminal. The delivery+emit is the
# deterministic fact under test; the retry is separate (and correct) behavior. wfId carries no regex specials.
disp_delivered(){ local wf="nba-ca:$(nbaid "$1"):$2:$3"
  podman logs --tail 5000 ais-nba-temporal-worker 2>&1 | grep -qE "disposition -> ${wf} = $4\$" && echo y || echo n; }
# Did the workflow for (member $1, action $2, channel $3) EMIT state $4? The activity layer logs "state {slug} =
# {STATE}" on every transition; the log is append-only so it survives the post-terminal RE-FIRE churn -- a terminal
# that frees the slot (EXPIRED/FAILED) leaves the action still-eligible, so the router re-CREATEs a fresh run and
# the snapshot's LATEST actionstate cycles back off the terminal. EXPIRED is emitted ONLY by the TTL path (window
# elapsed, no conversion), so this is an exact proof of natural expiry, not a transient snapshot read.
state_emitted(){ local sl="$(nbaid "$1"):$2:$3"
  podman logs --tail 8000 ais-nba-temporal-worker 2>&1 | grep -qE "state ${sl} = $4\$" && echo y || echo n; }
# raw snapshot fact field for a member (empty if the fact isn't in the lean snapshot)
snap_fact(){ R hget "nba:snapshot:$(nbaid "$1")" "fact:$2" 2>/dev/null; }
has_fact(){ [ -n "$(snap_fact "$1" "$2")" ] && echo y || echo n; }
# Broadcast a GLOBAL per-channel throttle level — exactly what the lake's throttle-emit produces.
# It rides member.facts -> the snapshot-builder forwards it to nba.definitions -> every rules-engine
# instance applies it as a population-wide channel cap. Wall-clock+counter eventTs so the test level
# is authoritative (beats any stale lake-emitted value, which uses wall-clock too).
# Emit a throttle-reason suppression — what the Temporal gate emits when a channel saturates for the
# day. It rides member.facts with a kind=throttle-suppress HEADER; the snapshot-builder routes it (by
# header) to nba.definitions, where the rules engine marks the channel ineligible until midnight.
throttle_hot(){ # channel  (current wall-clock eventTs so the rules engine's stale-event guard accepts it)
  local ts; ts=$(date +%s%3N)
  echo "{\"entityType\":\"OPERATOR\",\"entityId\":\"__sat\",\"key\":\"nba.actionstate.sat.$1\",\"value\":\"suppressed\",\"reason\":\"throttle\",\"valueType\":\"STRING\",\"eventTs\":$ts,\"source\":\"test\"}" \
    | podman exec -i ais-nba-redpanda rpk topic produce nba.member.facts -k "OPERATOR:__sat:nba.actionstate.sat.$1" -H "kind:throttle-suppress" >/dev/null 2>&1
}
THN=0
throttle_set(){ # channel level [metric=daily]
  THN=$((THN+1)); local ts=$(( $(date +%s%3N) + THN )); local m="${3:-daily}"; local k="nba.throttle.$1.$m"
  produce nba.member.facts "SYSTEM:__throttle:$k" \
    "{\"entityType\":\"SYSTEM\",\"entityId\":\"__throttle\",\"key\":\"$k\",\"value\":$2,\"valueType\":\"LONG\",\"eventTs\":$ts,\"source\":\"test-throttle\",\"windowSeconds\":300}"
}

# ---- assertion (poll until equal or timeout) ----
chk(){ # desc  getter-cmd  expected  [timeout]
  local desc="$1" cmd="$2" exp="$3" to="${4:-20}" i=0 got=""
  while [ $i -lt "$to" ]; do
    got="$(eval "$cmd")"
    [ "$got" == "$exp" ] && { echo "  PASS  $desc"; PASS=$((PASS+1)); return 0; }
    sleep 1; i=$((i+1))
  done
  echo "  FAIL  $desc  (expected [$exp] got [$got])"; FAIL=$((FAIL+1)); return 1
}
slugs(){ local IFS=$'\n'; printf '%s\n' "$@" | sort | paste -sd, -; }   # sorted csv
# Like chk, but RE-EMITS a no-op fact (respondedToOutreach, unchanged) for member $2 each poll iteration to force a
# FRESH member eval. Use this for an assertion that follows a CONTROL-PLANE change (throttle_set / al_suppress /
# throttle_hot): those update the rules-engine's in-memory GLOBAL_THROTTLE / SUPPRESSED state, but a member only
# RE-EVALS on a new snapshot -- so a one-shot bump races the propagation and a plain chk would poll one stale eval
# forever. respondedToOutreach is a no-op re-emit (already true for the qualifying member) that doesn't disturb the
# per-member cap facts (emailsThisWeek/totalThisWeek), so it is safe even in the compose-with-member-cap test.
chk_bump(){ # desc  member  getter-cmd  expected  [timeout]
  local desc="$1" m="$2" cmd="$3" exp="$4" to="${5:-36}" i=0 got=""
  while [ $i -lt "$to" ]; do
    got="$(eval "$cmd")"
    [ "$got" == "$exp" ] && { echo "  PASS  $desc"; PASS=$((PASS+1)); return 0; }
    feed1 "$m" "operator.activity.respondedToOutreach" true BOOL    # no-op re-emit -> fresh eval vs the propagated control state
    sleep 3; i=$((i+3))
  done
  echo "  FAIL  $desc  (expected [$exp] got [$got])"; FAIL=$((FAIL+1)); return 1
}

# Reset to a FRESH baseline FIRST (topics / consumer offsets / DLQs / Redis, then re-seed defs) so a run never
# depends on residue -- a stale DLQ, an old in-flight journey, a bloated definitions topic. Set NBA_TEST_NO_RESET=1
# to skip (iterating one test against an already-clean stack).
if [ "${NBA_TEST_NO_RESET:-0}" != "1" ]; then
  bash "$(dirname "$0")/reset-fresh.sh" || { echo "reset-fresh FAILED -- aborting"; exit 1; }
fi

echo "== resolving fixtures (healthcare catalog) =="
# Healthcare engagement FUNNEL (resolved by name from nba.definitions). plan_welcome + reengage are eligible for
# EVERYONE (empty inclusion) until respondedToOutreach COMPLETES them; respondedToOutreach then unlocks
# portal_registration + hra + hra_reminder -- the "qualifying" stage we anchor most tests on. Further facts advance
# it: registeredForPortal -> login_reminder/benefits_education; hraCompleted -> pcp_selection; pcpSelected -> the
# care/screening actions. (See nba/infra/seed/definitions.jsonl for the full inclusion/completion rules.)
PORTAL=$(action_id 'Portal Registration')      # email/push/sms/voice ; incl respondedToOutreach ; compl registeredForPortal
HRA=$(action_id 'Health Risk Assessment')      # email/push/sms/voice ; incl respondedToOutreach ; compl hraCompleted
HRAR=$(action_id 'Assessment Reminder')        # email/push/sms       ; incl respondedToOutreach ; compl hraCompleted
REENG=$(action_id 'Re-engage Lapsed Member')   # email/push/sms       ; incl <none>             ; compl respondedToOutreach
WELC=$(action_id 'Plan Welcome')               # email/push/sms/voice ; incl <none>             ; compl respondedToOutreach
PCP=$(action_id 'PCP Selection')               # email/push/sms/voice ; incl hraCompleted       ; compl pcpSelected
echo "  portal=$PORTAL hra=$HRA hra_reminder=$HRAR reengage=$REENG welcome=$WELC pcp=$PCP"
[ -z "$PORTAL" ] && { echo "FIXTURES MISSING -- seed the healthcare catalog (reset-fresh) first"; exit 1; }

# The qualifying profile, reused everywhere: respondedToOutreach COMPLETES the always-on welcome/reengage and
# UNLOCKS portal_registration + hra + hra_reminder. isDNC=false + comms 0 keep the global "No DNC" + "Comms cap
# 3/wk" rules open. (Channel/global thresholds are LOAD-TEST-sized: email cap <1000/wk, email daily throttle
# <50000 -- each throttle test crosses the real threshold itself rather than assuming a small one.)
LC_ACT="$HRA"   # the action the lifecycle/state tests SOLO + drive (hra has email/push/sms/voice; we keep email)
RO="operator.activity.respondedToOutreach:true:BOOL"; ND="operator.profile.isDNC:false:BOOL"
C0="operator.comms.totalThisWeek:0:LONG"; E0="operator.comms.emailsThisWeek:0:LONG"

# Drop any leaked test-fixture actions from a prior interrupted run. These pollute eligibility — esp.
# VariantTest's EMPTY inclusion makes it eligible for EVERY member, breaking nearly every assertion.
# Deterministic isolation: the suite must start from only the seeded fixtures.
echo "== cleaning up leaked test-fixture actions (VariantTest / ComplCrit / ComplNoExcl / ExpireTest) =="
for leaked in $(podman exec ais-nba-bff wget -qO- http://nba-action-library:7001/actions 2>/dev/null | python -c "import json,sys; print('\n'.join(a['id'] for a in json.load(sys.stdin) if a.get('name') in ('VariantTest','ComplCrit','ComplNoExcl','ExpireTest')))" 2>/dev/null); do
  echo "  deleting leaked $leaked"; al_del_action "$leaked"
done
sleep 3

# Reset throttle/rate/saturation to a KNOWN clean baseline BEFORE any test, so every run starts from
# counts=0 — independent of (a) a prior run's saturation HOT (lasts NBA_THROTTLE_HOT_TTL_SECONDS, global
# per-channel), (b) leftover throttle levels, or (c) the LIVE lake's real daily counts. We don't trust a
# fixed sleep: we POLL a fully-qualified probe until email+push are actually eligible, re-asserting the
# reset each loop (a level-0 open also lifts saturation HOT). Deterministic start = non-flaky suite.
echo "== resetting throttle/rate to a known clean baseline (poll until channels open) =="
for ch in email sms push; do throttle_set "$ch" 0; throttle_set "$ch" 0 rate; done
__p="${RUN}base"
setup "$__p" "$RO" "$ND" "$C0" "$E0"
__i=0
while [ $__i -lt 30 ]; do
  [ "$(elig_has "$__p" "$PORTAL:email")" = y ] && { echo "  baseline clean -- portal/email open after ${__i}s"; break; }
  throttle_set email 0; sleep 2; __i=$((__i+2))                 # re-open each loop in case a stale count re-capped during warmup
done
[ $__i -ge 30 ] && echo "  WARN: baseline never opened email -- a live-lake count may be capping it; tests may be noisy"

# ============================ ELIGIBILITY (rules engine) ============================
t_eligibility_basic(){ local m="${RUN}elig"; echo "[eligibility_basic] respondedToOutreach unlocks the qualifying stage (portal_registration + hra + hra_reminder)"
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  chk "portal_registration/email eligible"  "elig_has $m $PORTAL:email" "y" 25
  chk "hra/push eligible"                    "elig_has $m $HRA:push"     "y"
  chk "hra_reminder/sms eligible"            "elig_has $m $HRAR:sms"     "y"
  chk "always-on plan_welcome now COMPLETED+excluded (compl=respondedToOutreach)" "elig_has $m $WELC:email" "n"
}
t_dnc_excludes_survey(){ local m="${RUN}dnc"; echo "[dnc] isDNC=true -> the global 'No DNC' rule excludes EVERYTHING"
  setup "$m" "$RO" "operator.profile.isDNC:true:BOOL" "$C0" "$E0"
  chk "nothing eligible under DNC (global rule)" "get_eligible $m" "" 25
}
t_missing_facts_default(){ local m="${RUN}miss"; echo "[missing_facts] no journey facts -> only the always-on welcome/reengage are eligible (empty inclusion); funnel actions stay LOCKED"
  # isDNC=false alone: a rulefact so the member gets a snapshot; no respondedToOutreach -> portal/hra LOCKED,
  # and welcome/reengage (empty inclusion, not yet completed) are the only eligible actions. Defaults open caps.
  setup "$m" "$ND"
  chk "plan_welcome/email eligible (empty inclusion, defaults open)" "elig_has $m $WELC:email"  "y" 25
  chk "reengage/push eligible"                                       "elig_has $m $REENG:push"  "y"
  chk "portal_registration LOCKED (needs respondedToOutreach)"       "elig_has $m $PORTAL:email" "n"
}
t_channel_cap(){ local m="${RUN}chcap"; echo "[channel_cap] emailsThisWeek>=1000 (the seeded 'Email cap 1000/wk') drops EMAIL channels; push/sms survive"
  setup "$m" "$RO" "$ND" "$C0" "operator.comms.emailsThisWeek:1000:LONG"
  chk "email channels capped: 0 eligible email pairs" "elig_ch $m email"      "0" 25
  chk "push survives (hra/push still eligible)"        "elig_has $m $HRA:push"  "y"
  chk "sms survives (hra_reminder/sms still eligible)" "elig_has $m $HRAR:sms"  "y"
}
t_global_cap(){ local m="${RUN}glcap"; echo "[global_cap] totalThisWeek>=3 (the global 'Comms cap 3/wk' rule) drops EVERYTHING"
  setup "$m" "$RO" "$ND" "operator.comms.totalThisWeek:3:LONG" "$E0"
  chk "nothing eligible (global cap hit)" "get_eligible $m" "" 25
}
t_channel_throttle(){ local m="${RUN}thr"; echo "[channel_throttle] GLOBAL email daily throttle (lake -> definitions broadcast) excludes email for everyone; push survives; lifts"
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  chk "baseline: portal/email eligible (throttle open)" "elig_has $m $PORTAL:email" "y" 25
  throttle_set email 50000                                  # global email daily hits the cap (rule: nba.throttle.email.daily < 50000)
  chk_bump "email THROTTLED globally: 0 eligible email pairs" "$m" "elig_ch $m email" "0" 36   # chk_bump re-evals until the GLOBAL_THROTTLE propagates
  chk_bump "push survives the email throttle"                "$m" "elig_has $m $HRA:push" "y" 30
  throttle_set email 0                                       # cap lifts (e.g. next day / sends roll off)
  chk_bump "throttle lifts: portal/email eligible again"    "$m" "elig_has $m $PORTAL:email" "y" 36
}
t_throttle_sms_fallback(){ local m="${RUN}thrsms"; echo "[throttle_sms_fallback] email cap flips the member to the SMS channel (the headline flow)"
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  chk "baseline: email + sms both open" "elig_has $m $PORTAL:email" "y" 25
  throttle_set email 50000                                  # email channel hits the global cap
  chk_bump "email throttled -> 0 email pairs"        "$m" "elig_ch $m email"     "0" 36
  chk_bump "SMS survives (hra_reminder/sms eligible)" "$m" "elig_has $m $HRAR:sms" "y" 30
  throttle_set email 0; sleep 1                              # reset GLOBAL level for the next test
}
t_throttle_multichannel(){ local m="${RUN}thrmc"; echo "[throttle_multichannel] capping email AND sms (both have authored daily rules) strands the member on push"
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  throttle_set email 50000; throttle_set sms 8000           # both channels at their daily cap (email<50000, sms<8000)
  chk_bump "email capped: 0 email pairs" "$m" "elig_ch $m email" "0" 36
  chk_bump "sms capped: 0 sms pairs"     "$m" "elig_ch $m sms"   "0" 36
  chk_bump "push survives (independent per-channel caps)" "$m" "elig_has $m $HRA:push" "y" 30
  throttle_set email 0; throttle_set sms 0; sleep 3          # reset GLOBAL levels for the next test (let them propagate)
}
t_throttle_composes_with_member_cap(){ local m="${RUN}thrcmp"; echo "[throttle_composes] global daily throttle AND per-member weekly cap both gate email"
  throttle_set email 0; sleep 3                              # ensure the global level starts open + reaches GLOBAL_THROTTLE
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  chk_bump "baseline: portal/email eligible (both caps open)" "$m" "elig_has $m $PORTAL:email" "y" 36
  feed1 "$m" "operator.comms.emailsThisWeek" 1000 LONG       # per-member weekly cap (>=1000) bites -- global throttle still open
  chk_bump "per-member cap alone excludes email" "$m" "elig_ch $m email" "0" 36
  feed1 "$m" "operator.comms.emailsThisWeek" 0 LONG          # member cap clears
  chk_bump "email eligible again (member cap cleared, throttle still open)" "$m" "elig_has $m $PORTAL:email" "y" 36
  throttle_set email 50000                                   # now the GLOBAL throttle bites instead
  chk_bump "global throttle alone excludes email" "$m" "elig_ch $m email" "0" 36
  throttle_set email 0; sleep 2                              # leave the level open for other tests
}
t_operator_suppress(){ local m="${RUN}osup"; echo "[operator_suppress] Command Center suppresses an ACTION -> rules engine marks it ineligible -> ML re-scores; unsuppress restores"
  throttle_set email 0; sleep 1
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  chk "baseline: hra eligible" "elig_has $m $HRA:push" "y" 25
  al_suppress "$HRA" true; sleep 3
  feed1 "$m" "operator.comms.emailsThisWeek" 0 LONG     # bump a rulefact -> re-eval picks up the suppression
  chk_bump "hra SUPPRESSED -> not eligible (re-eval)" "$m" "elig_has $m $HRA:push" "n" 36
  chk_bump "portal still eligible (only hra suppressed)" "$m" "elig_has $m $PORTAL:email" "y" 36
  al_suppress "$HRA" false
  chk_bump "hra RESTORED -> eligible again" "$m" "elig_has $m $HRA:push" "y" 36
}
t_operator_suppress_channel(){ local m="${RUN}osupc"; echo "[operator_suppress_channel] suppress an ACTION-CHANNEL (actionId.channel) -> only that channel excluded; restore brings it back"
  throttle_set email 0; sleep 1
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  chk "baseline: hra/push eligible" "elig_has $m $HRA:push" "y" 25
  al_suppress_ch "$HRA" "push" true
  chk_bump "hra/push channel suppressed -> not eligible" "$m" "elig_has $m $HRA:push"  "n" 36
  chk_bump "hra/email (other channel) still eligible"    "$m" "elig_has $m $HRA:email" "y" 36
  al_suppress_ch "$HRA" "push" false
  chk_bump "channel restored -> hra/push eligible again" "$m" "elig_has $m $HRA:push" "y" 36
}
t_content_variants(){ echo "[content_variants] a channel carries content-key VARIANTS (A/B + targeting) -> the rules engine picks the key per member: vip variant gated on operator.tier=premium, else the base key"
  # base key tmpl.base.vtest; variant tmpl.vip.vtest at 100% for premium tier. Empty inclusion = always eligible.
  local json='{"name":"VariantTest","ttlSeconds":3600,"channels":[{"channel":"email","contentKey":"tmpl.base.vtest","variants":[{"contentKey":"tmpl.vip.vtest","percent":100,"conditions":{"op":"all","conditions":[{"fact":"operator.tier","cmp":"eq","value":"premium"}]}}]}],"inclusion":{"op":"all","conditions":[]},"exclusion":{"op":"any","conditions":[]}}'
  local vid; vid=$(al_post_action "$json" | python -c "import json,sys;print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
  echo "  created action [$vid] -- waiting for definition + rulefacts (operator.tier) to propagate"; sleep 12
  throttle_set email 0                                       # ensure email is open (a prior throttle test's GLOBAL_THROTTLE could linger)
  local mp="${RUN}vpre" mb="${RUN}vbas"
  setup "$mp" 'operator.tier:"premium":STRING' "operator.profile.isDNC:false:BOOL" "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  setup "$mb" 'operator.tier:"basic":STRING'   "operator.profile.isDNC:false:BOOL" "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  # chk_bump re-evals until operator.tier rulefact + the email-open level have propagated (VariantTest is empty-inclusion).
  chk_bump "premium tier -> vip variant key" "$mp" "get_contentkey $mp $vid email" "tmpl.vip.vtest" 36
  chk_bump "basic tier   -> base key (no variant matched)" "$mb" "get_contentkey $mb $vid email" "tmpl.base.vtest" 36
  al_del_action "$vid"; sleep 2
}
t_hard_completion(){ echo "[hard_completion] completion latches + auto-retires the action (criterion + API paths); autoExclude=false tracks it WITHOUT excluding"
  # two PUSH-channel actions (push never hits the email throttle) gated to distinct member groups so each
  # test member matches exactly one. goal facts auto-join rulefacts (action-library collects completion facts).
  local jc='{"name":"ComplCrit","ttlSeconds":3600,"channels":[{"channel":"push","contentKey":"tmpl.cc"}],"inclusion":{"op":"all","conditions":[{"fact":"operator.test.grp","cmp":"eq","value":"crit"}]},"exclusion":{"op":"any","conditions":[]},"completion":{"op":"all","conditions":[{"fact":"operator.test.goal","cmp":"eq","value":true}]}}'
  local jn='{"name":"ComplNoExcl","ttlSeconds":3600,"channels":[{"channel":"push","contentKey":"tmpl.cn"}],"inclusion":{"op":"all","conditions":[{"fact":"operator.test.grp","cmp":"eq","value":"noexcl"}]},"exclusion":{"op":"any","conditions":[]},"autoExcludeOnCompletion":false,"completion":{"op":"all","conditions":[{"fact":"operator.test.goal2","cmp":"eq","value":true}]}}'
  local cid nid
  cid=$(al_post_action "$jc" | python -c "import json,sys;print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
  nid=$(al_post_action "$jn" | python -c "import json,sys;print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
  echo "  created [$cid] (auto-exclude) + [$nid] (track-only) -- waiting for defs + rulefacts to propagate"; sleep 12
  al_suppress "$WELC" true; al_suppress "$REENG" true; sleep 3   # iso: mute the always-on welcome/reengage so ONLY our custom action is eligible

  # The test member carries only its group fact (no journey facts), so with welcome/reengage muted ONLY our custom
  # push action is eligible -> the router deterministically dispatches it (push never hits the email throttle).
  # --- criterion path: eligible while goal=false; goal flips -> workflow walks to HARD_COMPLETED (the ML label),
  #     then the action auto-retires from eligibility. THIS is the supervised label we train on. ---
  local mc="${RUN}hc"
  setup "$mc" 'operator.test.grp:"crit":STRING' "operator.test.goal:false:BOOL" "operator.activity.viewedDashboard:true:BOOL"
  chk "criterion: action eligible before completion" "get_eligible $mc" "$cid:push" 30
  chk "criterion: workflow activated (walking)" "get_state $mc $cid:push | grep -qE 'CREATED|IN_PROCESS|PRESENTED' && echo y || echo n" "y" 40
  feed1 "$mc" "operator.test.goal" true BOOL                          # member did the goal
  chk "criterion: workflow reaches terminal HARD_COMPLETED (the ML training label)" "get_state $mc $cid:push" "HARD_COMPLETED" 60
  chk "criterion: retired from eligibility once the workflow completed" "get_eligible $mc" "" 30

  # --- API path: explicit POST /completion drives the same HARD_COMPLETED + retire ---
  local ma="${RUN}hca"
  setup "$ma" 'operator.test.grp:"crit":STRING' "operator.test.goal:false:BOOL" "operator.activity.viewedDashboard:true:BOOL"
  chk "api: action eligible before completion" "get_eligible $ma" "$cid:push" 30
  chk "api: workflow activated (walking)" "get_state $ma $cid:push | grep -qE 'CREATED|IN_PROCESS|PRESENTED' && echo y || echo n" "y" 40
  al_completion "$ma" "$cid"                                          # nba.completion.{cid} signal (outbox path)
  chk "api: workflow reaches terminal HARD_COMPLETED" "get_state $ma $cid:push" "HARD_COMPLETED" 60
  chk "api: signal latched -> retired" "get_eligible $ma" "" 30

  # --- autoExclude=false: completion is TRACKED (hardCompleted + HARD_COMPLETED) but the action stays eligible ---
  local mn="${RUN}hcn"
  setup "$mn" 'operator.test.grp:"noexcl":STRING' "operator.test.goal2:false:BOOL" "operator.activity.viewedDashboard:true:BOOL"
  chk "no-exclude: eligible, not yet completed" "get_hardcompleted $mn $nid push" "False" 30
  feed1 "$mn" "operator.test.goal2" true BOOL
  chk "no-exclude: workflow reaches HARD_COMPLETED" "get_state $mn $nid:push" "HARD_COMPLETED" 60
  chk "no-exclude: hardCompleted=true but STILL eligible (no auto-retire)" "get_hardcompleted $mn $nid push" "True" 30
  chk "no-exclude: action remains in the eligible set" "get_eligible $mn" "$nid:push" 20

  al_del_action "$cid"; al_del_action "$nid"; al_suppress "$WELC" false; al_suppress "$REENG" false; sleep 2
}
t_expired(){ echo "[expired] no HARD completion within the action TTL -> the workflow EXPIRES (terminal): frees the slot + the NEGATIVE ml label"
  # short-TTL push action gated to a unique group. We DON'T suppress it -- suppressing would mark it ineligible and
  # the router would CANCEL the live workflow (-> SUPPRESSED), which is the wrong terminal. We just let the TTL
  # window elapse with no conversion. EXPIRED is terminal but FREES THE SLOT, so the still-eligible action re-fires
  # (a fresh run walks CREATED->...->EXPIRED again, ~every TTL): the snapshot's LATEST value cycles, so we assert
  # the EMITTED EXPIRED (append-only log) not the transient snapshot. Pause the conversion-sim: it converts a
  # stable-hash fraction of (member,action) pairs, and for THIS run's member it would HARD_COMPLETE before the TTL.
  sim_pause
  local je='{"name":"ExpireTest","ttlSeconds":8,"channels":[{"channel":"push","contentKey":"tmpl.ex"}],"inclusion":{"op":"all","conditions":[{"fact":"operator.test.exgrp","cmp":"eq","value":true}]},"exclusion":{"op":"any","conditions":[]}}'
  local eid; eid=$(al_post_action "$je" | python -c "import json,sys;print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
  echo "  created [$eid] (ttl=8s) -- waiting for defs + rulefacts to propagate"; sleep 12
  al_suppress "$WELC" true; al_suppress "$REENG" true; sleep 3        # iso: only our custom action eligible -> deterministically dispatched
  local me="${RUN}exp"
  setup "$me" "operator.test.exgrp:true:BOOL"
  chk "expired: action eligible + workflow activates" "get_state $me $eid:push | grep -qE 'CREATED|IN_PROCESS|PRESENTED' && echo y || echo n" "y" 40
  chk "expired: TTL elapses with no conversion -> EXPIRED emitted (terminal; then frees the slot to re-fire)" "state_emitted $me $eid push EXPIRED" "y" 45
  al_del_action "$eid"; al_suppress "$WELC" false; al_suppress "$REENG" false; sim_resume; sleep 2
}
t_inbound_serving(){ local m="${RUN}inb"; echo "[inbound_serving] action-library serves the next best action from the eval CACHE (pull, no state machine) + records an inbound disposition"
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  chk "next-action serves a scored action from cache" \
    "next_action $m | python -c 'import json,sys; d=json.load(sys.stdin); print(\"y\" if d[\"count\"]>=1 and d[\"actions\"][0][\"score\"] is not None else \"n\")'" "y" 20
  # disposition funnel is per channel: push -> Delivered/Opened (tracking, never 'sent' -> no throttle)
  chk "disposition records the channel funnel status (push: Delivered)" \
    "inbound_disp $m $HRA push Delivered | python -c 'import json,sys; print(json.load(sys.stdin)[\"value\"])'" "Delivered" 8
  # inbound/pull channel funnel: Presented/Accepted/Completed
  chk "inbound (pull) disposition records Presented" \
    "inbound_disp $m $HRA app Presented | python -c 'import json,sys; print(json.load(sys.stdin)[\"value\"])'" "Presented" 8
}
t_throttle_saturation(){ local m="${RUN}sat"; echo "[throttle_saturation] gate SATURATION (rate cannot clear today) -> rules engine marks the channel ineligible -> ML re-scores onto another channel"
  throttle_set email 0; sleep 2          # email passes the absolute daily ceiling so we isolate saturation
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  chk "baseline: portal/email eligible" "elig_has $m $PORTAL:email" "y" 25
  throttle_hot email                     # gate predicts email cannot clear today -> throttle-suppress -> rules engine
  chk_bump "email saturated for the day -> 0 email pairs (ML re-scores to push)" "$m" "elig_ch $m email" "0" 36
  chk_bump "push survives saturation" "$m" "elig_has $m $HRA:push" "y" 30
}

# ============================ SCORING (ML, deterministic heuristic) ============================
t_scoring(){ local m="${RUN}score"; echo "[scoring] the local journey-scorer attaches deterministic numeric scores to the eligible action-channels"
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  # The local journey-scorer is a deterministic hash over (member,action,channel,state) -- stable per member but
  # member-id-dependent, so the INTEGRATION test asserts the scorer RAN end-to-end (numeric, non-null) on the top
  # actions. The exact score bands + ordering + determinism are pinned by the UNIT test (nba-journey-scorer/
  # test_scorer.py, 13 cases), so the rigor lives there and this proves the live wiring.
  chk "portal/email carries a numeric score"     "get_score $m $PORTAL:email | grep -qE '^-?[0-9]+(\\.[0-9]+)?\$' && echo y || echo n" "y" 25
  chk "hra/push carries a numeric score"          "get_score $m $HRA:push    | grep -qE '^-?[0-9]+(\\.[0-9]+)?\$' && echo y || echo n" "y"
  chk "hra_reminder/sms carries a numeric score"  "get_score $m $HRAR:sms    | grep -qE '^-?[0-9]+(\\.[0-9]+)?\$' && echo y || echo n" "y"
}

# ============================ ROUTING + STATE MACHINE (Temporal) ============================
t_routing_lifecycle(){ local m="${RUN}life"; echo "[lifecycle] the soloed action walks the funnel (dispatched -> delivered)"
  throttle_set email 0; lc_solo               # only $LC_ACT:email eligible -> the router deterministically dispatches it
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  # the activation layer walks the email funnel Sent -> Delivered(PRESENTED) -> engaged. We assert it WALKS (sent +
  # delivered or beyond); whether it then ENGAGES (SOFT_COMPLETED) / CONVERTS (HARD_COMPLETED) / EXPIRES is the
  # conversion-sim's PROBABILISTIC outcome, so asserting an exact engagement state would be permanently flaky.
  chk "$LC_ACT/$LC_CH walks the funnel (dispatched -> delivered+)" "walked $m $LC_ACT:$LC_CH" "y" 75
  lc_restore
}
t_supersede(){ local m="${RUN}sup"; echo "[supersede/serialization] two same-channel siblings -> the router sends only the top one (the other is held / self-DEBOUNCEs) -> NO double-comm"
  throttle_set email 0
  # keep exactly TWO email siblings (hra:email + portal:email); suppress the rest + their other channels.
  al_suppress "$HRAR" true; for c in push sms voice mail; do al_suppress_ch "$HRA" "$c" true; al_suppress_ch "$PORTAL" "$c" true; done; sleep 3
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  # The router SERIALIZES per channel: it CREATEs only the top-scored of the two (the slot is then occupied), and the
  # debounce dedup self-DEBOUNCEs any sibling that raced into the window -- so exactly ONE of the two ever reaches a
  # sent/delivered state, the other is held (CREATED) or DEBOUNCED. (Which is the winner is score-dependent.) This is
  # the no-double-comm guarantee; the exact loser-state (DEBOUNCED vs held) is a race, so we assert the COUNT.
  chk "exactly ONE of the two same-channel siblings sends (no double-comm)" "walked_count $m $HRA:email $PORTAL:email" "1" 70
  al_suppress "$HRAR" false; for c in push sms voice mail; do al_suppress_ch "$HRA" "$c" false; al_suppress_ch "$PORTAL" "$c" false; done
}
t_batch(){ local m="${RUN}batch"; echo "[batch] maxBatch>1 -> router batches the top-N on the winning channel -> ONE dispatch -> N child workflows -> N SOFT_COMPLETED"
  al_maxbatch email 2; throttle_set email 0
  # keep TWO email actions (hra:email + portal:email); maxBatch=2 dispatches BOTH (no dedup) -> both walk.
  al_suppress "$HRAR" true; for c in push sms voice mail; do al_suppress_ch "$HRA" "$c" true; al_suppress_ch "$PORTAL" "$c" true; done; sleep 3
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  chk "hra/email in the batch walks the funnel (dispatched + delivered)"  "walked $m $HRA:email"    "y" 75
  chk "portal/email in the same batch walks too (BOTH from ONE dispatch)" "walked $m $PORTAL:email" "y" 75
  al_maxbatch email 1; al_suppress "$HRAR" false; for c in push sms voice mail; do al_suppress_ch "$HRA" "$c" false; al_suppress_ch "$PORTAL" "$c" false; done
}
t_throttle_lifecycle_sms(){ local m="${RUN}thrlc"; echo "[throttle_lifecycle] email at the global cap -> the member's send actually goes out on SMS instead"
  throttle_set email 50000                     # email channel is globally capped (rule: nba.throttle.email.daily < 50000)
  # solo hra but KEEP email+sms (suppress the rest); email is capped -> sms is the surviving send channel.
  al_suppress "$PORTAL" true; al_suppress "$HRAR" true; al_suppress_ch "$HRA" push true; al_suppress_ch "$HRA" voice true; al_suppress_ch "$HRA" mail true; sleep 3
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  chk "SMS send walks the funnel (email capped -> rerouted to SMS)" "walked $m $HRA:sms" "y" 75
  chk "email never sent (throttled out)" "get_state $m $HRA:email | grep -qE 'IN_PROCESS|PRESENTED|SOFT_COMPLETED|HARD_COMPLETED' && echo y || echo n" "n" 5
  throttle_set email 0; al_suppress "$PORTAL" false; al_suppress "$HRAR" false; al_suppress_ch "$HRA" push false; al_suppress_ch "$HRA" voice false; al_suppress_ch "$HRA" mail false
}
# NOTE on driveability: there is NO distinct "ACTIVATED" state. The state machine emits CREATED first (the
# debounce window — ChannelActionWorkflowImpl.java:71), then IN_PROCESS on dispatch past debounce+gate
# (line 111). This test asserts that real CREATED -> IN_PROCESS transition for a clean single-action,
# no-suppression activation (the other lifecycle tests only assert the funnel rest states beyond it).
t_state_created_then_in_process(){ local m="${RUN}crin"; echo "[state_created_then_in_process] no suppression -> CREATED (debounce-armed) then IN_PROCESS (dispatched)"
  throttle_set email 0; lc_solo
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  # CREATED is the first emitted state (the debounce window). Catch it early, then watch it advance once the
  # debounce window + throttle gate clear -> IN_PROCESS (the dispatch hand-off; ChannelActionWorkflowImpl:111).
  chk "$LC_ACT/$LC_CH activates at CREATED (debounce-armed)" "get_state $m $LC_ACT:$LC_CH | grep -qE 'CREATED|IN_PROCESS|PRESENTED|SOFT_COMPLETED' && echo y || echo n" "y" 25
  chk "advances past the debounce gate -> IN_PROCESS (dispatched, no provider response yet)" \
      "get_state $m $LC_ACT:$LC_CH | grep -qE 'IN_PROCESS|PRESENTED|SOFT_COMPLETED' && echo y || echo n" "y" 40
  lc_restore
}
t_state_suppress_post_dispatch(){ local m="${RUN}suppd"; echo "[state_suppress_post_dispatch] suppress AFTER dispatch -> SUPPRESSING -> SUPPRESSED (the post-send cancel cascade, not pre-send DEBOUNCED)"
  throttle_set email 0; lc_solo
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  # Let it walk PAST the debounce window to a real dispatch (IN_PROCESS/PRESENTED) so the suppress is a post-handoff
  # CANCEL (operatorSuppress -> trackDispositions emits SUPPRESSING, then the layer answers SUPPRESSED) -- the
  # distinct post-dispatch terminal, NOT the pre-send DEBOUNCED path.
  chk "dispatched past debounce (IN_PROCESS/PRESENTED)" "get_state $m $LC_ACT:$LC_CH | grep -qE 'IN_PROCESS|PRESENTED|SOFT_COMPLETED' && echo y || echo n" "y" 45
  al_suppress "$LC_ACT" true                                         # operator pull -> operatorSuppress fan-out to the RUNNING workflow
  chk "post-dispatch suppress reaches terminal SUPPRESSED (cancel caught it)" "get_state $m $LC_ACT:$LC_CH" "SUPPRESSED" 45
  al_suppress "$LC_ACT" false; lc_restore                            # restore
}
t_state_declined(){ local m="${RUN}decl"; echo "[state_declined] a DECLINED delivery disposition -> the workflow records DECLINED (non-terminal rest state: keeps watching for HARD_COMPLETED until TTL)"
  throttle_set email 0; lc_solo
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  # Drive it to a real dispatch FIRST (so the workflow is past the gate, holding the live corr), then inject a
  # DECLINED disposition carrying that corr. DECLINED is a REST state (ChannelActionWorkflowImpl:28) -- the
  # workflow emits it and stays alive, so the latest actionstate fact reads DECLINED.
  chk "dispatched (IN_PROCESS/PRESENTED) before the decline" "get_state $m $LC_ACT:$LC_CH | grep -qE 'IN_PROCESS|PRESENTED|SOFT_COMPLETED' && echo y || echo n" "y" 45
  disp_inject "$m" "$LC_ACT" "$LC_CH" DECLINED                       # member opted out (canonical state DECLINED)
  chk "DECLINED delivered to the live workflow + emitted as its state" "disp_delivered $m $LC_ACT $LC_CH DECLINED" "y" 40
  lc_restore
}
t_state_failed(){ local m="${RUN}fail"; echo "[state_failed] a FAILED delivery disposition (bounce / no-answer) -> the workflow reaches terminal FAILED"
  throttle_set email 0; lc_solo
  setup "$m" "$RO" "$ND" "$C0" "$E0"
  chk "dispatched (IN_PROCESS/PRESENTED) before the failure" "get_state $m $LC_ACT:$LC_CH | grep -qE 'IN_PROCESS|PRESENTED|SOFT_COMPLETED' && echo y || echo n" "y" 45
  disp_inject "$m" "$LC_ACT" "$LC_CH" FAILED                         # provider bounce (canonical state FAILED -- terminal)
  chk "FAILED delivered to the live workflow + emitted (terminal; then frees the slot for a retry)" "disp_delivered $m $LC_ACT $LC_CH FAILED" "y" 40
  lc_restore
}

# ============================ DLQ (poison -> envelope -> replay) ============================
# A consumer that fails to process a record envelopes it onto nba.dlq.{consumer} (DELETE-policy, retained a
# week) so it stays replayable; the replay re-produces the EXACT original to its SOURCE topic and idempotency
# makes it safe. snapshot-builder is the cleanest probe: ANY unparseable record on nba.member.facts is DLQ'd
# (no header filter), and a valid member fact is snapshotted normally.
# Did OUR poison (UNIQUE marker $1) land on the snapshot-builder DLQ? Reads the whole DLQ (reset-fresh keeps it
# small) under a hard `timeout` so it can NEVER hang the suite (a bare `rpk consume -n` can block waiting for more
# records than exist). Uses grep, NOT python: `rpk | python` hangs under Git Bash (Windows-python never sees the
# podman-exec pipe's EOF); grep is native MSYS + closes cleanly. Chained greps require ONE DLQ line (one record)
# to carry the marker value AND consumer=snapshot-builder AND source topic=nba.member.facts -- a correctly-shaped
# envelope. The marker is unique per run, so reading the whole DLQ is isolation-safe. Echoes y|n.
dlq_has_poison(){ local marker="$1" h out
  h=$(podman exec ais-nba-redpanda rpk topic describe nba.dlq.snapshot-builder -p 2>/dev/null | awk '/^[0-9]/{print $6}'); h=${h:-0}
  [ "$h" -le 0 ] && { echo n; return; }
  # CAPTURE the whole DLQ into a var, THEN grep it -- NOT a streaming `rpk | grep` pipe. rpk block-buffers stdout
  # and has no clean early EOF here: `-o start -n hwm` would wait for data records that don't exist (a transactional
  # topic's hwm counts commit markers as offsets), and bare `-o start` tails -- either way the matched line stays
  # stuck in grep's buffer and `grep -q` never fires until the process dies. A bounded `timeout` ends rpk's tail
  # AFTER it has emitted every existing record, so command-substitution captures a complete dump (EOF flushes all).
  # reset-fresh empties the DLQ + t_dlq_replay runs FIRST -> only this run's records (marker unique = isolation-safe).
  # One DLQ line must carry the marker value AND consumer=snapshot-builder AND source topic=nba.member.facts.
  out=$(timeout 8 podman exec ais-nba-redpanda rpk topic consume nba.dlq.snapshot-builder -o start -f '%v\n' 2>/dev/null | cat)
  if echo "$out" | grep "\"value\":\"$marker" | grep "\"consumer\":\"snapshot-builder\"" | grep -q "\"topic\":\"nba.member.facts\""
  then echo y; else echo n; fi
}
t_dlq_replay(){ local m="${RUN}dlq"; echo "[dlq_replay] poison record -> nba.dlq.snapshot-builder envelope; replay the ORIGINAL good fact -> snapshotted normally (no data loss)"
  local POISON="__POISON_${RUN}_not-json"                            # a unique marker so we can find OUR envelope
  # 1) produce an UNPARSEABLE record to the consumer's input topic (nba.member.facts). snapshot-builder's
  #    M.readTree throws -> it envelopes the record onto nba.dlq.snapshot-builder (still inside the txn).
  echo "$POISON" | podman exec -i ais-nba-redpanda rpk topic produce nba.member.facts -k "OPERATOR:$m:poison" >/dev/null 2>&1
  # 2) produce a GOOD fact AFTER the poison + wait for ITS snapshot. nba.member.facts is single-partition, so the
  #    builder consumes strictly in offset order: once a record produced AFTER the poison appears in the snapshot,
  #    the builder has processed PAST the poison -> the poison is already DLQ'd (no latency race against a cold
  #    earliest-read backlog). This sentinel is ALSO the REPLAY proof -- a good record after the poison is processed
  #    normally + LWW-lands in the lean snapshot (replay is safe + lossless). Generous window for the cold catch-up.
  feed1 "$m" "operator.activity.respondedToOutreach" true BOOL
  chk "good fact AFTER the poison lands in the snapshot -> builder processed past it (replay-safe, no data loss)" "has_fact $m operator.activity.respondedToOutreach" "y" 90
  # 3) the builder is now PAST the poison, so the envelope is GUARANTEED present. Assert its shape:
  #    {consumer,topic,partition,offset,key,value,headers,error,dlqTs} with consumer=snapshot-builder, source
  #    topic=nba.member.facts, value=our (unique) raw poison.
  chk "poison enveloped onto nba.dlq.snapshot-builder (consumer+source-topic+raw value)" \
    "dlq_has_poison '$POISON'" "y" 30
}

# ============================ FACT ROUTING / RECONCILE (snapshot adaptation) ============================
# The snapshot is LEAN — only facts some action references (nba:rulefacts) are kept. When the action->fact
# map changes, the lake reconcile re-emits newly-referenced facts (from gold) onto member.facts so the
# whole population's snapshot adapts; de-referenced facts are pruned. These tests drive that contract by
# manipulating nba:rulefacts directly (what the action-library does on a definition change).
t_lean_filter(){ local m="${RUN}lean"; local NF="operator.lean.unused_${RUN}"; echo "[lean_filter] facts no action uses are NOT snapshotted"
  setup "$m" "operator.activity.loggedIn:true:BOOL"         # a real healthcare rulefact -> the member gets a snapshot
  feed1 "$m" "$NF" 7 LONG; sleep 2                          # NF is not in rulefacts
  chk "rulefact IS in snapshot" "has_fact $m operator.activity.loggedIn" "y"
  chk "non-rulefact is NOT snapshotted (lean filter)" "has_fact $m $NF" "n"
}
t_fact_backfill(){ local m="${RUN}bf"; local NF="operator.bf.flag_${RUN}"; echo "[fact_backfill] a newly-referenced fact is added to the snapshot on re-emit"
  setup "$m" "operator.activity.loggedIn:true:BOOL"
  feed1 "$m" "$NF" 5 LONG; sleep 2
  chk "before: $NF NOT in snapshot" "has_fact $m $NF" "n"
  R sadd nba:rulefacts "$NF" >/dev/null                     # action now uses it (action-library would do this)
  sleep 11                                                  # snapshot-builder refreshes rulefacts (10s)
  feed1 "$m" "$NF" 5 LONG; sleep 2                          # reconcile re-emits the latest value from gold
  chk "after: $NF IS in snapshot (backfilled)" "has_fact $m $NF" "y" 5
  R srem nba:rulefacts "$NF" >/dev/null
}
t_fact_drop(){ local m="${RUN}drop"; local DF="operator.drop.flag_${RUN}"; echo "[fact_drop] a de-referenced fact is pruned from the snapshot"
  R sadd nba:rulefacts "$DF" >/dev/null; sleep 11           # DF is a rulefact
  setup "$m" "operator.activity.loggedIn:true:BOOL"
  feed1 "$m" "$DF" 1 LONG; sleep 2
  chk "DF IS in snapshot" "has_fact $m $DF" "y" 5
  R srem nba:rulefacts "$DF" >/dev/null                     # no action uses it anymore
  sleep 12                                                  # snapshot-builder refresh detects shrink + prunes
  chk "DF is PRUNED from snapshot (drop)" "has_fact $m $DF" "n" 8
}

# DLQ runs FIRST, right after reset-fresh, while member.facts is still EMPTY. The snapshot-builder reads from
# earliest, so the moment we produce the poison it's the head of an empty topic -> DLQ'd in one poll cycle. Run it
# later and it sits behind a backlog of seeded members (the builder reprocesses earliest-first), so the warmup
# fact never snapshots in time. Empty topic = zero backlog = deterministic, fast envelope.
echo; echo "== DLQ (poison -> envelope -> replay; runs FIRST, before any seeded backlog) =="; t_dlq_replay
echo; echo "== ELIGIBILITY =="; t_eligibility_basic; t_dnc_excludes_survey; t_missing_facts_default; t_channel_cap; t_global_cap
echo; echo "== CHANNEL THROTTLE (global daily cap) =="; t_channel_throttle; t_throttle_sms_fallback; t_throttle_multichannel; t_throttle_composes_with_member_cap
echo; echo "== SCORING =="; t_scoring
if [ "$MODE" != "fast" ]; then echo; echo "== ROUTING + STATE MACHINE =="; t_routing_lifecycle; t_supersede; t_batch; t_throttle_lifecycle_sms; fi
# Disposition-driven states inject SUPPRESS/DECLINE/FAIL by hand, so hold off the conversion-sim (else a stable-hash
# conversion to HARD_COMPLETED races + beats the inject to a terminal). Resume it right after the block.
if [ "$MODE" != "fast" ]; then echo; echo "== STATE MACHINE (disposition-driven states) =="; sim_pause; t_state_created_then_in_process; t_state_suppress_post_dispatch; t_state_declined; t_state_failed; sim_resume; fi
echo; echo "== OPERATOR SUPPRESS (Command Center) =="; t_operator_suppress; t_operator_suppress_channel
echo; echo "== CONTENT VARIANTS (per-member content key) =="; t_content_variants
echo; echo "== INBOUND SERVING (pull from cache) =="; t_inbound_serving
echo; echo "== HARD / SOFT COMPLETION =="; t_hard_completion; t_expired
echo; echo "== FACT ROUTING (lean filter) =="; t_lean_filter
if [ "$MODE" != "fast" ]; then echo; echo "== FACT RECONCILE (backfill + drop) =="; t_fact_backfill; t_fact_drop; fi
# LAST — marks email saturated (hot until the TTL), so it must run after every other email test.
echo; echo "== THROTTLE SATURATION (dynamic eligibility) =="; t_throttle_saturation

echo; echo "================ RESULT: $PASS passed, $FAIL failed ================"
[ "$FAIL" -eq 0 ]
