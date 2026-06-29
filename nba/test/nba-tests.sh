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
# Lifecycle tests assert the disposition-driven states (happy path: CREATED -> IN_PROCESS -> PRESENTED ->
# SOFT_COMPLETED). The supersede test proves DEBOUNCED via the sibling dedup: two workflows for the same
# member end up in the debounce window, each queries its siblings at the timer, the lower self-DEBOUNCEs.
# Sibling discovery is Temporal visibility (eventually consistent ~1-2s), so run the worker with a window
# comfortably above that (prod = 60s):  nba/services/nba-temporal/run.ps1 -DebounceSeconds 10
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

# ---- fixtures: resolve the seeded action ids by name ----
action_id(){ # name -> actionId
  local k
  for k in $(R --scan --pattern 'nba:action:*'); do
    R get "$k" | python -c "import json,sys;d=json.load(sys.stdin);print(d['id']) if d.get('name')=='$1' else None" 2>/dev/null
  done | grep -v '^None$' | grep . | head -1
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

# ---- readers (operate on entity, resolve nbaid each call so it works before/after mint) ----
# (Historically the catalog carried hash-id 'demo arms' eligible for EVERY member — now PURGED; the catalog is the 15
# authored healthcare actions only. This hook stays empty so eligibility assertions self-heal if a stray test action
# is ever re-seeded.)
DEMO_ARMS=""
get_eligible(){ R get "nba:evaluation:$(nbaid "$1")" 2>/dev/null | DEMO="$DEMO_ARMS" python -c "import json,sys,os
try: d=json.load(sys.stdin)
except: print('?'); raise SystemExit
demo=set(os.environ.get('DEMO','').split())
print(','.join(sorted(c['actionId']+':'+c['channel'] for c in d.get('channelActions',[]) if c.get('eligible') and c['actionId'] not in demo)))" 2>/dev/null; }
# what INBOUND actually serves (action-library /next-action) — this is where operator suppression is
# enforced LIVE (the raw eval still lists suppressed actions; the serve strips them). Sorted slugs.
served_actions(){ next_action "$1" | python -c "import json,sys
try: d=json.load(sys.stdin)
except: print('?'); raise SystemExit
print(','.join(sorted(a['actionId']+':'+a['channel'] for a in d.get('actions',[]))))" 2>/dev/null; }
get_score(){ R get "nba:evaluation:$(nbaid "$1")" 2>/dev/null | python -c "import json,sys
d=json.load(sys.stdin)
print(next((str(c.get('score')) for c in d.get('channelActions',[]) if c['actionId']+':'+c['channel']=='$2'),'none'))" 2>/dev/null; }
# HARD-completed action ids on the eval (channelActions where hardCompleted; sorted unique csv); and a ChannelAction's hardCompleted flag.
get_completed(){ R get "nba:evaluation:$(nbaid "$1")" 2>/dev/null | python -c "import json,sys
try: d=json.load(sys.stdin)
except: print('?'); raise SystemExit
print(','.join(sorted(set(c['actionId'] for c in d.get('channelActions',[]) if c.get('hardCompleted')))))" 2>/dev/null; }
get_hardcompleted(){ R get "nba:evaluation:$(nbaid "$1")" 2>/dev/null | python -c "import json,sys
try: d=json.load(sys.stdin)
except: print('?'); raise SystemExit
print(next((str(c.get('hardCompleted')) for c in d.get('channelActions',[]) if c['actionId']=='$2' and c['channel']=='$3'),'none'))" 2>/dev/null; }
# the CONTENT KEY the rules engine selected for a member's action-channel (variant-aware) — read off the eval
get_contentkey(){ R get "nba:evaluation:$(nbaid "$1")" 2>/dev/null | python -c "import json,sys
try: d=json.load(sys.stdin)
except: print('?'); raise SystemExit
print(next((c.get('contentKey') for c in d.get('channelActions',[]) if c['actionId']=='$2' and c['channel']=='$3'),'none'))" 2>/dev/null; }
get_state(){ R hget "nba:snapshot:$(nbaid "$1")" "fact:nba.actionstate.${2/:/.}" 2>/dev/null | python -c "import json,sys;print(json.load(sys.stdin)['value'])" 2>/dev/null; }
get_count(){ R get "nba:dl:$(nbaid "$1"):$2" 2>/dev/null; }
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

echo "== resolving fixtures =="
REE="$(action_id 'Reengage Email'):email"
SUR="$(action_id 'Survey Prompt'):push"
DSH="$(action_id 'Dashboard Tip'):email"
SMSA="$(action_id 'Re-engage SMS'):sms"   # smsConsent-gated SMS fallback (throttle lifecycle)
echo "  Reengage=$REE  Survey=$SUR  Dashboard=$DSH  SMS=$SMSA"
[ -z "${REE%%:*}" ] && { echo "FIXTURES MISSING — seed actions first"; exit 1; }

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
echo "== resetting throttle/rate/saturation to a known clean baseline (poll until channels open) =="
for ch in email sms push; do throttle_set "$ch" 0; throttle_set "$ch" 0 rate; done
__p="${RUN}base"
setup "$__p" "operator.activity.usedChat:true:BOOL" "operator.activity.viewedDashboard:true:BOOL" \
      "operator.activity.daysSinceLogin:20:LONG" "operator.activity.completedTasks:8:LONG" \
      "operator.profile.isDNC:false:BOOL" "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
__i=0
while [ $__i -lt 30 ]; do
  case "$(get_eligible "$__p")" in *"${REE%%:*}:email"*) echo "  baseline clean — email + push open after ${__i}s"; break;; esac
  throttle_set email 0; sleep 2; __i=$((__i+2))                 # re-open each loop in case a stale count re-capped during warmup
done
[ $__i -ge 30 ] && echo "  WARN: baseline never opened email — a live-lake count may be capping it; tests may be noisy"

# ============================ ELIGIBILITY (rules engine) ============================
t_eligibility_basic(){ local m="${RUN}elig"; echo "[eligibility_basic] qualifying member"
  setup "$m" "operator.activity.usedChat:true:BOOL" "operator.activity.viewedDashboard:true:BOOL" \
        "operator.activity.daysSinceLogin:20:LONG" "operator.activity.completedTasks:8:LONG" \
        "operator.profile.isDNC:false:BOOL" "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  chk "eligible = Reengage/email + Survey/push (Dashboard excluded: viewedDashboard=true)" \
      "get_eligible $m" "$(slugs "$REE" "$SUR")"
}
t_dnc_excludes_survey(){ local m="${RUN}dnc"; echo "[dnc] isDNC=true excludes Survey (its exclusion)"
  setup "$m" "operator.activity.completedTasks:8:LONG" "operator.activity.viewedDashboard:true:BOOL" \
        "operator.activity.daysSinceLogin:5:LONG" "operator.profile.isDNC:true:BOOL" \
        "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  chk "Survey NOT eligible under DNC" "get_eligible $m" ""
}
t_missing_facts_default(){ local m="${RUN}miss"; echo "[missing_facts] absent facts -> type default"
  # ONLY completedTasks=7: missing isDNC->false (not excluded), missing counts->0 (under caps),
  # missing viewedDashboard->false (Dashboard eligible), missing daysSinceLogin->0 (Reengage not).
  setup "$m" "operator.activity.completedTasks:7:LONG"
  chk "eligible = Survey/push + Dashboard/email (defaults applied)" "get_eligible $m" "$(slugs "$SUR" "$DSH")"
}
t_channel_cap(){ local m="${RUN}chcap"; echo "[channel_cap] emailsThisWeek=3 drops email actions, push survives"
  setup "$m" "operator.activity.usedChat:true:BOOL" "operator.activity.daysSinceLogin:20:LONG" \
        "operator.activity.completedTasks:8:LONG" "operator.activity.viewedDashboard:false:BOOL" \
        "operator.profile.isDNC:false:BOOL" "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:3:LONG"
  chk "only Survey/push eligible (email cap hit)" "get_eligible $m" "$SUR"
}
t_global_cap(){ local m="${RUN}glcap"; echo "[global_cap] totalThisWeek=3 drops everything"
  setup "$m" "operator.activity.usedChat:true:BOOL" "operator.activity.daysSinceLogin:20:LONG" \
        "operator.activity.completedTasks:8:LONG" "operator.activity.viewedDashboard:false:BOOL" \
        "operator.profile.isDNC:false:BOOL" "operator.comms.totalThisWeek:3:LONG" "operator.comms.emailsThisWeek:0:LONG"
  chk "nothing eligible (global cap hit)" "get_eligible $m" ""
}
t_channel_throttle(){ local m="${RUN}thr"; echo "[channel_throttle] GLOBAL daily email cap (lake -> definitions broadcast) excludes email for everyone; push survives; lifts"
  # qualifies for Reengage/email (daysSinceLogin>=14) AND Survey/push (completedTasks>=5); viewedDashboard=true drops Dashboard
  setup "$m" "operator.activity.usedChat:true:BOOL" "operator.activity.viewedDashboard:true:BOOL" \
        "operator.activity.daysSinceLogin:20:LONG" "operator.activity.completedTasks:8:LONG" \
        "operator.profile.isDNC:false:BOOL" "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  chk "baseline: Reengage/email + Survey/push eligible (throttle open)" "get_eligible $m" "$(slugs "$REE" "$SUR")"
  throttle_set email 5; sleep 3                              # global email level hits the cap (rule: nba.throttle.email.daily < 5)
  feed1 "$m" "operator.comms.emailsThisWeek" 0 LONG          # bump a rulefact -> re-eval against the new GLOBAL level (no per-member change)
  chk "email THROTTLED globally: only Survey/push eligible" "get_eligible $m" "$SUR" 25
  throttle_set email 0; sleep 3                              # cap lifts (e.g. next day / sends roll off)
  feed1 "$m" "operator.comms.emailsThisWeek" 0 LONG          # bump -> re-eval
  chk "throttle lifts: Reengage/email eligible again" "get_eligible $m" "$(slugs "$REE" "$SUR")" 25
}
t_throttle_sms_fallback(){ local m="${RUN}thrsms"; echo "[throttle_sms_fallback] email cap flips the member to the SMS channel (the headline flow)"
  # smsConsent=true -> Re-engage SMS eligible; completedTasks=0 -> no push; viewedDashboard=true -> no Dashboard
  setup "$m" "operator.activity.daysSinceLogin:20:LONG" "operator.profile.smsConsent:true:BOOL" \
        "operator.activity.viewedDashboard:true:BOOL" "operator.activity.completedTasks:0:LONG" \
        "operator.profile.isDNC:false:BOOL" "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  chk "baseline: Reengage/email + Re-engage/sms both eligible" "get_eligible $m" "$(slugs "$REE" "$SMSA")"
  throttle_set email 5; sleep 3                              # email channel hits the global cap
  feed1 "$m" "operator.comms.emailsThisWeek" 0 LONG          # bump -> re-eval
  chk "email throttled -> ONLY the SMS action survives (flip to SMS)" "get_eligible $m" "$SMSA" 25
  throttle_set email 0; sleep 1                              # reset GLOBAL level for the next test
}
t_throttle_multichannel(){ local m="${RUN}thrmc"; echo "[throttle_multichannel] capping email AND sms strands the member on neither (independent per-channel caps)"
  setup "$m" "operator.activity.daysSinceLogin:20:LONG" "operator.profile.smsConsent:true:BOOL" \
        "operator.activity.viewedDashboard:true:BOOL" "operator.activity.completedTasks:0:LONG" \
        "operator.profile.isDNC:false:BOOL" "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  throttle_set email 5; throttle_set sms 5; sleep 3          # both channels capped (sms has NO authored cap rule -> still open!)
  feed1 "$m" "operator.comms.emailsThisWeek" 0 LONG
  # NOTE: only email has a channel rule referencing nba.throttle.email.daily; sms has none, so the sms
  # level is broadcast but unenforced. This asserts caps are PER-CHANNEL and only bite where a rule exists.
  chk "email capped (rule) excludes email; sms has no rule so SMS still eligible" "get_eligible $m" "$SMSA" 25
  throttle_set email 0; throttle_set sms 0; sleep 3          # reset GLOBAL levels for the next test (let them propagate)
}
t_throttle_composes_with_member_cap(){ local m="${RUN}thrcmp"; echo "[throttle_composes] global daily throttle AND per-member weekly cap both gate email"
  throttle_set email 0; sleep 3                              # ensure the global level starts open + reaches GLOBAL_THROTTLE
  setup "$m" "operator.activity.daysSinceLogin:20:LONG" "operator.activity.viewedDashboard:true:BOOL" \
        "operator.activity.completedTasks:0:LONG" "operator.profile.isDNC:false:BOOL" \
        "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  # A global throttle change does NOT re-snapshot a member (re-eval fires only on a new snapshot), so bump a
  # rulefact to force a fresh eval against the now-open level before asserting the baseline. Deterministic.
  feed1 "$m" "operator.comms.emailsThisWeek" 0 LONG
  chk "baseline: Reengage/email eligible (both caps open)" "get_eligible $m" "$REE" 30
  feed1 "$m" "operator.comms.emailsThisWeek" 3 LONG          # per-member weekly cap (>=3) bites — global throttle still open
  chk "per-member cap alone excludes email" "get_eligible $m" "" 25
  feed1 "$m" "operator.comms.emailsThisWeek" 0 LONG          # member cap clears
  chk "email eligible again (member cap cleared, throttle still open)" "get_eligible $m" "$REE" 30
  throttle_set email 5; sleep 3                              # now the GLOBAL throttle bites instead
  feed1 "$m" "operator.comms.emailsThisWeek" 0 LONG
  chk "global throttle alone excludes email" "get_eligible $m" "" 25
  throttle_set email 0; sleep 2                              # leave the level open for other tests
}
t_operator_suppress(){ local m="${RUN}osup"; echo "[operator_suppress] Command Center suppresses an ACTION -> rules engine marks it ineligible -> ML re-scores; unsuppress restores"
  throttle_set email 0; sleep 1
  # completedTasks>=5 -> Survey/push; viewedDashboard=false + daysSinceLogin<14 -> Dashboard/email (Reengage needs >=14)
  setup "$m" "operator.activity.completedTasks:8:LONG" "operator.activity.viewedDashboard:false:BOOL" \
        "operator.activity.daysSinceLogin:5:LONG" "operator.profile.isDNC:false:BOOL" \
        "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  chk "baseline: Survey/push + Dashboard/email eligible" "get_eligible $m" "$(slugs "$SUR" "$DSH")"
  # Suppression is enforced at the READ edges, not by re-evaluating every member (you can't rely on a new
  # fact arriving in prod). Inbound serving strips suppressed actions LIVE from the nba:suppressed set, so it
  # takes effect immediately with NO new fact. (Outbound self-corrects: the next eval excludes it via the
  # rules engine and the router never activates it; in-flight is pulled by the cancel cascade.)
  al_suppress "${SUR%%:*}" true; sleep 3
  chk "Survey SUPPRESSED -> inbound no longer serves it (only Dashboard/email)" "served_actions $m" "$DSH" 20
  al_suppress "${SUR%%:*}" false; sleep 3
  chk "Survey RESTORED -> inbound serves it again" "served_actions $m" "$(slugs "$SUR" "$DSH")" 20
}
t_operator_suppress_channel(){ local m="${RUN}osupc"; echo "[operator_suppress_channel] suppress an ACTION-CHANNEL (actionId.channel) -> only that channel excluded; restore brings it back"
  throttle_set email 0; sleep 1
  setup "$m" "operator.activity.completedTasks:8:LONG" "operator.activity.viewedDashboard:false:BOOL" \
        "operator.activity.daysSinceLogin:5:LONG" "operator.profile.isDNC:false:BOOL" \
        "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  chk "baseline: Survey/push + Dashboard/email eligible" "get_eligible $m" "$(slugs "$SUR" "$DSH")"
  # Enforced live at the inbound serve edge off nba:suppressed (no re-eval, no new fact needed).
  al_suppress_ch "${SUR%%:*}" "push" true; sleep 3
  chk "Survey/push channel suppressed -> inbound serves only Dashboard/email" "served_actions $m" "$DSH" 20
  al_suppress_ch "${SUR%%:*}" "push" false; sleep 3
  chk "channel restored -> inbound serves Survey/push again" "served_actions $m" "$(slugs "$SUR" "$DSH")" 20
}
t_content_variants(){ echo "[content_variants] a channel carries content-key VARIANTS (A/B + targeting) -> the rules engine picks the key per member: vip variant gated on operator.tier=premium, else the base key"
  # base key tmpl.base.vtest; variant tmpl.vip.vtest at 100% for premium tier. Empty inclusion = always eligible.
  local json='{"name":"VariantTest","ttlSeconds":3600,"channels":[{"channel":"email","contentKey":"tmpl.base.vtest","variants":[{"contentKey":"tmpl.vip.vtest","percent":100,"conditions":{"op":"all","conditions":[{"fact":"operator.tier","cmp":"eq","value":"premium"}]}}]}],"inclusion":{"op":"all","conditions":[]},"exclusion":{"op":"any","conditions":[]}}'
  local vid; vid=$(al_post_action "$json" | python -c "import json,sys;print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
  echo "  created action [$vid] — waiting for definition + rulefacts (operator.tier) to propagate"; sleep 12
  local mp="${RUN}vpre" mb="${RUN}vbas"
  setup "$mp" 'operator.tier:"premium":STRING' "operator.activity.daysSinceLogin:5:LONG" "operator.profile.isDNC:false:BOOL" "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  setup "$mb" 'operator.tier:"basic":STRING'   "operator.activity.daysSinceLogin:5:LONG" "operator.profile.isDNC:false:BOOL" "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  chk "premium tier -> vip variant key" "get_contentkey $mp $vid email" "tmpl.vip.vtest" 30
  chk "basic tier   -> base key (no variant matched)" "get_contentkey $mb $vid email" "tmpl.base.vtest" 30
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
  echo "  created [$cid] (auto-exclude) + [$nid] (track-only) — waiting for defs + rulefacts to propagate"; sleep 12

  # viewedDashboard=true excludes the seeded Dashboard action (its inclusion is viewedDashboard==false), and
  # the absent daysSinceLogin/completedTasks keep the other seeded actions out — so only OUR action is eligible.
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

  al_del_action "$cid"; al_del_action "$nid"; sleep 2
}
t_expired(){ echo "[expired] no HARD completion within the action TTL -> the workflow EXPIRES (terminal): frees the slot + the NEGATIVE ml label"
  # short-TTL push action gated to a unique group. The member activates it; we operator-suppress it (so it
  # can't RE-fire after expiry — without cancelling the live workflow, since the eval stays eligible:true and
  # the router only SUPPRESSes !eligible+cancellable), then the TTL elapses with no conversion -> EXPIRED.
  local je='{"name":"ExpireTest","ttlSeconds":8,"channels":[{"channel":"push","contentKey":"tmpl.ex"}],"inclusion":{"op":"all","conditions":[{"fact":"operator.test.exgrp","cmp":"eq","value":true}]},"exclusion":{"op":"any","conditions":[]}}'
  local eid; eid=$(al_post_action "$je" | python -c "import json,sys;print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
  echo "  created [$eid] (ttl=8s) — waiting for defs + rulefacts to propagate"; sleep 12
  local me="${RUN}exp"
  setup "$me" "operator.test.exgrp:true:BOOL" "operator.activity.viewedDashboard:true:BOOL"
  chk "expired: action eligible + workflow activates" "get_state $me $eid:push | grep -qE 'CREATED|IN_PROCESS|PRESENTED' && echo y || echo n" "y" 40
  al_suppress "$eid" true                                             # operator-pull: blocks re-fire after expiry, leaves the live workflow running
  chk "expired: TTL elapses with no conversion -> EXPIRED (terminal, slot freed)" "get_state $me $eid:push" "EXPIRED" 45
  al_suppress "$eid" false; al_del_action "$eid"; sleep 2
}
t_inbound_serving(){ local m="${RUN}inb"; echo "[inbound_serving] action-library serves the next best action from the eval CACHE (pull, no state machine) + records an inbound disposition"
  setup "$m" "operator.activity.daysSinceLogin:20:LONG" "operator.activity.completedTasks:8:LONG" \
        "operator.activity.viewedDashboard:true:BOOL" "operator.profile.isDNC:false:BOOL" \
        "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  chk "next-action serves a scored action from cache" \
    "next_action $m | python -c 'import json,sys; d=json.load(sys.stdin); print(\"y\" if d[\"count\"]>=1 and d[\"actions\"][0][\"score\"] is not None else \"n\")'" "y" 20
  # disposition funnel is per channel: push -> Delivered/Opened (tracking, never 'sent' -> no throttle)
  chk "disposition records the channel funnel status (push: Delivered)" \
    "inbound_disp $m ${SUR%%:*} push Delivered | python -c 'import json,sys; print(json.load(sys.stdin)[\"value\"])'" "Delivered" 8
  # inbound/pull channel funnel: Presented/Accepted/Completed
  chk "inbound (pull) disposition records Presented" \
    "inbound_disp $m ${SUR%%:*} app Presented | python -c 'import json,sys; print(json.load(sys.stdin)[\"value\"])'" "Presented" 8
}
t_throttle_saturation(){ local m="${RUN}sat"; echo "[throttle_saturation] gate SATURATION (rate can't clear today) -> rules engine marks the channel ineligible -> ML re-scores onto another channel"
  throttle_set email 0; sleep 2          # email passes the absolute daily ceiling so we isolate saturation
  setup "$m" "operator.activity.daysSinceLogin:20:LONG" "operator.activity.completedTasks:8:LONG" \
        "operator.activity.viewedDashboard:true:BOOL" "operator.profile.isDNC:false:BOOL" \
        "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  chk "baseline: Reengage/email + Survey/push eligible" "get_eligible $m" "$(slugs "$REE" "$SUR")"
  throttle_hot email; sleep 3            # gate predicts email can't clear today -> throttle-suppress -> rules engine
  feed1 "$m" "operator.comms.emailsThisWeek" 0 LONG   # bump -> re-eval against the new (saturated) eligibility
  chk "email saturated for the day -> excluded; ML re-scores to Survey/push" "get_eligible $m" "$SUR" 25
}

# ============================ SCORING (ML, deterministic heuristic) ============================
t_scoring(){ local m="${RUN}score"; echo "[scoring] deterministic propensity + channel prior"
  setup "$m" "operator.activity.usedChat:true:BOOL" "operator.activity.viewedDashboard:true:BOOL" \
        "operator.activity.daysSinceLogin:20:LONG" "operator.activity.completedTasks:8:LONG" \
        "operator.profile.isDNC:false:BOOL" "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  # 0.5 -0.20(days) +0.16(tasks) +0.10(chat) +0.05(viewed) = 0.61; email +0.05 = 0.66, push +0.
  chk "Reengage/email scores 0.66 (email prior)" "get_score $m $REE" "0.66"
  chk "Survey/push scores 0.61 (no email prior)" "get_score $m $SUR" "0.61"
}

# ============================ ROUTING + STATE MACHINE (Temporal) ============================
t_routing_lifecycle(){ local m="${RUN}life"; echo "[lifecycle] top-scored -> workflow walks the funnel -> rests at SOFT_COMPLETED"
  setup "$m" "operator.activity.usedChat:true:BOOL" "operator.activity.viewedDashboard:true:BOOL" \
        "operator.activity.daysSinceLogin:20:LONG" "operator.activity.completedTasks:3:LONG" \
        "operator.profile.isDNC:false:BOOL" "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  # the activation layer walks the email funnel Sent->Delivered->Opened->LinkClicked; LinkClicked is email's
  # soft bar, so the workflow rests at SOFT_COMPLETED (then watches for HARD_COMPLETED until TTL -> EXPIRED).
  chk "top (Reengage/email) walks the funnel to SOFT_COMPLETED" "get_state $m $REE" "SOFT_COMPLETED" 60
}
t_supersede(){ local m="${RUN}sup"; echo "[supersede/debounce] lower activates; a higher action appears in its debounce window -> the loser self-DEBOUNCEs (sibling dedup), the winner walks to SOFT_COMPLETED"
  setup "$m" "operator.activity.usedChat:true:BOOL" "operator.activity.viewedDashboard:true:BOOL" \
        "operator.activity.daysSinceLogin:5:LONG" "operator.activity.completedTasks:8:LONG" \
        "operator.profile.isDNC:false:BOOL" "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  chk "Survey/push activates first (CREATED, in the debounce window)" "get_state $m $SUR" "CREATED" 25
  # promote Reengage (0.66 > Survey 0.61) AND drop Survey out of eligibility, so the loser stays DEBOUNCED
  # (a still-eligible loser would re-fire and eventually send as the next action once the winner engages).
  feed1 "$m" "operator.activity.daysSinceLogin" 20 LONG
  feed1 "$m" "operator.activity.completedTasks" 0 LONG
  chk "Survey/push loses the dedup -> DEBOUNCED (nothing sent; frees the slot)" "get_state $m $SUR" "DEBOUNCED" 45
  chk "Reengage/email wins -> walks to SOFT_COMPLETED" "get_state $m $REE" "SOFT_COMPLETED" 60
}
t_batch(){ local m="${RUN}batch"; echo "[batch] maxBatch>1 -> router batches the top-N on the winning channel -> ONE dispatch -> N child workflows -> N SOFT_COMPLETED"
  al_maxbatch email 2; throttle_set email 0; sleep 2
  # eligible for BOTH email actions: Reengage (daysSinceLogin>=14) + Dashboard (viewedDashboard=false); completedTasks=0 -> no push
  setup "$m" "operator.activity.daysSinceLogin:20:LONG" "operator.activity.viewedDashboard:false:BOOL" \
        "operator.activity.completedTasks:0:LONG" "operator.profile.isDNC:false:BOOL" \
        "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  chk "Reengage/email in the batch walks to SOFT_COMPLETED" "get_state $m $REE" "SOFT_COMPLETED" 60
  chk "Dashboard/email in the same batch walks to SOFT_COMPLETED" "get_state $m $DSH" "SOFT_COMPLETED" 60
  al_maxbatch email 1; sleep 1          # back to single-action for the other tests
}
t_throttle_lifecycle_sms(){ local m="${RUN}thrlc"; echo "[throttle_lifecycle] email at the global cap -> the member's send actually goes out on SMS instead"
  throttle_set email 5; sleep 3                              # email channel is globally capped
  setup "$m" "operator.activity.daysSinceLogin:20:LONG" "operator.profile.smsConsent:true:BOOL" \
        "operator.activity.viewedDashboard:true:BOOL" "operator.activity.completedTasks:0:LONG" \
        "operator.profile.isDNC:false:BOOL" "operator.comms.totalThisWeek:0:LONG" "operator.comms.emailsThisWeek:0:LONG"
  chk "SMS send walks to SOFT_COMPLETED (email capped -> rerouted to SMS)" "get_state $m $SMSA" "SOFT_COMPLETED" 60
  chk "email never sent (it was throttled out)" "get_state $m $REE | grep -qE 'IN_PROCESS|PRESENTED|SOFT_COMPLETED' && echo y || echo n" "n" 5
  throttle_set email 0; sleep 1                              # reset GLOBAL level for the next test
}

# ============================ FACT ROUTING / RECONCILE (snapshot adaptation) ============================
# The snapshot is LEAN — only facts some action references (nba:rulefacts) are kept. When the action->fact
# map changes, the lake reconcile re-emits newly-referenced facts (from gold) onto member.facts so the
# whole population's snapshot adapts; de-referenced facts are pruned. These tests drive that contract by
# manipulating nba:rulefacts directly (what the action-library does on a definition change).
t_lean_filter(){ local m="${RUN}lean"; local NF="operator.lean.unused_${RUN}"; echo "[lean_filter] facts no action uses are NOT snapshotted"
  setup "$m" "operator.activity.daysSinceLogin:20:LONG"     # a real rulefact -> the member gets a snapshot
  feed1 "$m" "$NF" 7 LONG; sleep 2                          # NF is not in rulefacts
  chk "rulefact IS in snapshot" "has_fact $m operator.activity.daysSinceLogin" "y"
  chk "non-rulefact is NOT snapshotted (lean filter)" "has_fact $m $NF" "n"
}
t_fact_backfill(){ local m="${RUN}bf"; local NF="operator.bf.flag_${RUN}"; echo "[fact_backfill] a newly-referenced fact is added to the snapshot on re-emit"
  setup "$m" "operator.activity.daysSinceLogin:20:LONG"
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
  setup "$m" "operator.activity.daysSinceLogin:20:LONG"
  feed1 "$m" "$DF" 1 LONG; sleep 2
  chk "DF IS in snapshot" "has_fact $m $DF" "y" 5
  R srem nba:rulefacts "$DF" >/dev/null                     # no action uses it anymore
  sleep 12                                                  # snapshot-builder refresh detects shrink + prunes
  chk "DF is PRUNED from snapshot (drop)" "has_fact $m $DF" "n" 8
}

echo; echo "== ELIGIBILITY =="; t_eligibility_basic; t_dnc_excludes_survey; t_missing_facts_default; t_channel_cap; t_global_cap
echo; echo "== CHANNEL THROTTLE (global daily cap) =="; t_channel_throttle; t_throttle_sms_fallback; t_throttle_multichannel; t_throttle_composes_with_member_cap
echo; echo "== SCORING =="; t_scoring
if [ "$MODE" != "fast" ]; then echo; echo "== ROUTING + STATE MACHINE =="; t_routing_lifecycle; t_supersede; t_batch; t_throttle_lifecycle_sms; fi
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
