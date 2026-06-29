# The member snapshot — what it is, how it changes over time

The snapshot-builder consumes `nba.member.facts` and maintains **one Redis hash per member** at
`nba:snapshot:{nbaId}`. It is the member's *current* state — the single object the rules engine evaluates and
the synchronous hot path reads. The KStreams engine and the Flink job build the **same shape** (only the store
differs — RocksDB / Flink state instead of Redis).

```
nba:snapshot:{nbaId}  (a hash)
  __entityType  OPERATOR
  __entityId    jdemo1
  __nbaId       nba_af77df084981
  __updatedTs   1782749560524
  fact:{key}    {"value":…,"valueType":…,"eventTs":…,"source":…}    ← one field per distinct fact key
  …
```

## What AGGREGATES vs what FALLS OFF (the core question)

**Distinct fact KEYS accumulate; within a key it is event-time last-writer-wins — facts do NOT pile up per key.**
Each fact key is exactly one hash field. A new fact for the same key **replaces** the old one *iff its `eventTs`
is newer* (`SnapshotLogic.applyLww`). So the snapshot grows by the **number of distinct keys** the member
touches, and each key holds only its **latest** value.

The fact families that accumulate (each its own keyspace):

| family | key | cardinality | update rule |
|---|---|---|---|
| inbound attributes | `operator.*` (e.g. `operator.profile.diabetic`) | one per attribute | latest wins (LWW by eventTs) |
| ML scores | `nba.score.{action}.{channel}` | one per action×channel | latest wins (LWW) — **scores do NOT pile up** |
| lifecycle state | `nba.actionstate.{action}.{channel}` | one per action×channel | latest state wins (LWW) |
| delivery disposition | `nba.disposition.{action}.{channel}` | one per action×channel | latest wins (LWW) |
| goal completion | `nba.completion.{action}` | one per action | **durable latch** (set once, rides forever) |
| milestone | `nba.milestone.{id}` | one per milestone | **durable latch** |

### Do scores pile up? No.
There is exactly **one** `nba.score.{action}.{channel}` field per action-channel. A fresh score from the scorer
**overwrites** the prior one (LWW). The number of score fields = the action×channel combinations the member has
been scored on — it grows as new actions/channels enter scope, but each combination is single-valued. (In the
journey below, the score count stays 7 across all three stages even as states/completions accumulate.)

### Do scores fall off once applied in eligibility? By default no (carried); with a TTL, yes.
By default the score stays on the snapshot as the *current* score and is read into **every** evaluation
(`RulesEngine.evaluate` copies `nba.score.{a}.{c}` onto the channelAction), replaced only when a newer score
arrives. **Set `NBA_SCORE_TTL_SECONDS` (rules-engine) to expire them:** at eval time a score whose `eventTs` is
older than the TTL is treated as **absent** — the channelAction emits `score=null`, so the router won't act on
it *and* the now eligible-but-unscored action re-triggers the scorer. Only `nba.score.*` expires; completions /
states / dispositions / milestones are durable and never do. The daily bulk re-scores everyone, so a TTL of
~1.5–2× the bulk cadence makes this a **safety net** for scores the bulk missed (and against acting on a score
from a now-superseded model). Default `0` = disabled (carry-forever).

### How is a STALE score prevented from being re-applied? Three mechanisms.
1. **Event-time LWW.** `applyLww` drops any incoming fact whose `eventTs <= ` the current field's `eventTs`. An
   out-of-order / replayed (older) score **cannot** overwrite a newer one. Each score is stamped with the
   scorer's `eventTs`; the snapshot keeps the freshest.
2. **Change-detect convergence.** The rules engine emits an eval only when its signature moved
   (`fullHash`/`eligHash`; `eligibilityChanged`). Scores are deterministic for a given state, so the
   score→snapshot→eval→score loop **converges**: once a score is written, the next eval recomputes the same
   score → the signature is unchanged → no re-emit → no re-score. This is what stops an infinite re-scoring loop.
3. **Wall-clock TTL (opt-in).** LWW only guards *out-of-order* staleness, not a score that is simply OLD in real
   time — a quiet member whose facts stopped changing, a missed bulk-score run, or a score from a superseded
   model. `NBA_SCORE_TTL_SECONDS` drops `nba.score.*` facts older than the TTL from the evaluation, so a stale
   score can never drive an action; the eligible-but-unscored eval re-triggers the scorer. `0` = disabled.

### What actually FALLS OFF
- **The lean-filter (never added).** A fact whose key is **not** in `nba:rulefacts` *and* is not an `nba.*`
  journey fact (score/state/disposition/completion/milestone) is dropped at classify time — it never enters the
  snapshot. *Observed below:* the member was accreted with 8 inbound attributes; **7 were kept and
  `operator.activity.daysSinceLogin` was dropped** because no rule references it. (When `nba:rulefacts` is empty
  the filter fails **open** = snapshot everything.)
- **De-referenced facts (pruning).** When a key is later removed from `nba:rulefacts` (no rule references it any
  more), the prune punctuator (`pruneFacts` / `pruneDereferenced`) sweeps it out of every snapshot.
- Everything else **persists**: terminal states (`HARD_COMPLETED`/`EXPIRED`/`DEBOUNCED`) stay as the
  action-channel's last state; completions/milestones are permanent latches; an opt-out disposition
  (`STOP`/`Unsubscribe`) is terminal and permanent — a dead channel never re-fires, so the fact never
  disappears and opt-out can never silently re-open.

## The snapshot across a member journey

Real captures of one member (`jdemo1` / `nba_af77df084981`) stepped through the journey on a clean flywheel —
the snapshot-builder folds the same facts a live journey emits (scores from the live scorer; the lifecycle
states/dispositions/completions/milestones the state machine emits). Watch the field count grow as distinct
keys accumulate while each key stays single-valued.

### Stage 1 — newly accreted, scored, ready for the forced onboarding email  (14 fields)
The member's inbound attributes are in (7 kept; `daysSinceLogin` dropped by the lean-filter), and the engine has
scored the candidate actions. `action_plan_welcome` (the forced onboarding action) is top-scored across its
channels. **No `nba.actionstate.*` yet** — it sits here until the router CREATEs the top candidate.
```
operator (7):    diabetic=true isDNC=false respondedToOutreach=true hraCompleted=false
                 pcpSelected=false registeredForPortal=false totalThisWeek=0      (daysSinceLogin DROPPED)
score (7):       nba.score.action_plan_welcome.{email=19.74, sms=19.06, push=18.06, voice=14.43}   ← onboarding, top
                 nba.score.action_reengage.{email=17.34, push=15.02, sms=14.62}
```

### Stage 2 — onboarding completed, second action in flight  (19 fields, +5)
The onboarding action walked its funnel and latched its goal; a second action is now in flight. Nothing from
Stage 1 was removed — five new keys appeared:
```
operator (7) + score (7)   ← unchanged (scores did NOT pile up — still 7)
actionstate (2): nba.actionstate.action_plan_welcome.email = HARD_COMPLETED
                 nba.actionstate.action_reengage.push       = IN_PROCESS        ← second action in flight
disposition (1): nba.disposition.action_plan_welcome.email  = LinkClicked
completion (1):  nba.completion.action_plan_welcome          = true             ← durable latch
milestone (1):   nba.milestone.milestone_reached             = <ts>             ← durable latch
```

### Stage 3 — whole set completed  (22 fields, +3)
Both actions reached a terminal state; both goals + a second milestone latched. Still 7 scores (one per
action×channel — no pile-up); the terminal states + latches are permanent. The full journey is reconstructable
from this one object: each action's high-water mark (its terminal state), how it was engaged (the dispositions),
what it scored, and which goals + milestones it reached.
```
operator (7) + score (7)   ← still unchanged
actionstate (2): action_plan_welcome.email = HARD_COMPLETED   action_reengage.push = HARD_COMPLETED
disposition (2): action_plan_welcome.email = LinkClicked      action_reengage.push = Delivered
milestone (2):   milestone_reached   milestone_assessed       (durable latches)
completion (2):  action_plan_welcome=true   action_reengage=true
```
