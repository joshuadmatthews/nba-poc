# NBA — Corner Cases & Consistency

> **Audience:** architects and engineers reviewing the Next-Best-Action pipeline.
> **Purpose:** the subtle, non-obvious behaviors the system is *deliberately* engineered for. Each section
> states the **scenario**, why it's **tricky**, **how the system handles it** (with the exact code), and the
> **integration test** that proves it (`nba/test/nba-tests.sh`).

The pipeline is a chain of single-purpose Kafka consumers over Redpanda + Redis + Temporal:

```
member.facts ─▶ snapshot-builder ─▶ nba.snapshots ─▶ rules-engine ─▶ nba.evaluations ─┬▶ scorer ─▶ (score facts) ─▶ member.facts ↺
                     │  (Redis snapshot, LWW)            (Drools eligibility)           └▶ action-router ─▶ (CREATE/SUPPRESS, kind=router) ─▶ member.facts
                     ▼                                                                                              │
              nba.definitions (broadcast: throttle, suppress, HOT)                                                 ▼
                     ▲                                                                            nba-temporal (debounce, ThrottleGate, state machine)
                     └──────────────────────────────────── nba.facts firehose (ML feature store) ───────── DISPATCH/CANCEL ─▶ action-layer ─▶ disposition ─▶ member.facts ↺
```

Almost every "corner case" below is a consequence of three design commitments:

| Commitment | Consequence |
|---|---|
| **Event-time everywhere** (`eventTs` on every fact, never wall-clock for ordering) | Out-of-order delivery and replay are *automatically* safe via last-writer-wins. |
| **Snapshot-driven re-evaluation** (the rules engine only fires when a *new snapshot* arrives) | Population-wide changes (throttle, suppress) **cannot** re-fire 8M members — so they're handled lazily or at read edges. |
| **The eval is authoritative + suppression-agnostic** | The router/inbound serve enforce policy at the *read* edge; the eval just describes reality. |

---

## 1. Event-time Last-Writer-Wins (LWW)

**Scenario.** Two facts for the same `(member, factKey)` arrive out of order — a network retry, a partition
re-balance, a DLQ replay — and the *older* one lands second. A naive "latest write" would clobber newer state.

**Why it's tricky.** "Latest" must mean *latest by the event's own clock*, not by arrival order or wall-clock.
A score computed at decision time T, replayed an hour later, must NOT overwrite a score that was computed at T+30m.

**How it's handled.** Every fact carries `eventTs`. Three independent stores each enforce LWW on it:

| Store | Code | Rule |
|---|---|---|
| Snapshot (Redis hash) | `SnapshotBuilder.selectWinners` | `if (sf.eventTs() <= cur) continue; // stale -> LWW drop` |
| ML feature store (in-mem) | `MlScorer.applyFact` | `if (cur != null && eventTs <= cur.eventTs()) return;` |
| Global throttle level | `RulesEngine.applyThrottle` | `if (cur == null || ts >= cur[1]) { ... }` |

The **score discipline** is the load-bearing subtlety. The scorer stamps each score with the *evaluation's
decision time*, not "now":

```java
// MlScorer.scoreEval
long evalTs = eval.path("evaluatedAt").asLong(System.currentTimeMillis());
...
fact.put("eventTs", evalTs);   // eval decision time (replay-safe LWW), not wall-clock now
```

So if an old eval is replayed off the DLQ, its re-emitted score carries the **old** `eventTs`, and the
snapshot-builder's LWW simply drops it under any newer score. Stamping with `now` would let stale replays win.

> **Talking point:** ordering is a property of the *data*, not the *transport*. We never need ordered delivery
> or a global clock — `eventTs` is the total order, applied independently at every sink.

**Test discipline.** `mkts()` increments a monotonic counter in the *parent shell* (a `$()` subshell would lose
the increment) so a re-written fact is strictly newer and not LWW-dropped. The base sits *below* wall-clock so
the live datalake's real-time count facts always win over test seeds.

---

## 2. Idempotency & Exactly-Once

**Scenario.** The snapshot-builder crashes mid-batch, or a Kafka produce fails after Redis already applied
some writes. On restart it re-processes the same records. Members must not see duplicated or lost snapshots.

**Why it's tricky.** Two stores (Redis + Kafka) must move together. A classic solution is a transactional
**outbox**; this pipeline avoids one entirely.

**How it's handled — two transactions, whole-batch retry:**

1. **One Redis transaction** (`MULTI/EXEC`) applies *all* winning fact writes for the batch.
2. **One Kafka transaction** (read-process-write EOS) produces *all* routes + firehose + DLQ + per-member
   snapshots **and** commits the consumer offsets:

```java
producer.sendOffsetsToTransaction(offsetsAfter(recs), consumer.groupMetadata());
producer.commitTransaction();
```

3. **On any failure:** abort the Kafka txn, do **not** commit offsets, `seek` back to the batch start, and
   **retry the whole batch** (`SnapshotBuilder.main`).

The reason whole-batch retry is safe **without an outbox** is that the re-emit is **additive**, and crucially it
is *independent of whether Redis changed this attempt*:

```java
// Re-emit a snapshot for EVERY member the batch touched — independent of LWW outcome, so a
// retry (where Redis already holds the values) still re-emits. This is what removes the outbox.
for (String nbaId : plan.emitMembers) { ... producer.send(... buildSnapshotJson ...); }
```

> Note `selectWinners` is intentionally decoupled from `emitMembers`: on a retry, `winners` is *empty*
> (Redis already holds the values) yet every touched member is **still re-emitted**. That decoupling is the
> whole trick.

| Property | Guaranteed by |
|---|---|
| No cross-replica Redis race | Producers key member facts by `memberId` (`entityType:entityId`) → one partition → one replica owns a member. |
| Stable txn identity | `TRANSACTIONAL_ID_CONFIG = "snapshot-builder-" + hostname()` — stable per instance, unique across instances. |
| Downstream reads only committed data | Consumers run `ISOLATION_LEVEL = read_committed`. |

**Replaying a stale DLQ message on top of newer state.** A DLQ envelope replays the *exact* original record to
its source topic. If newer state already exists, **§1 LWW drops it** — the replay is a no-op on state, harmless.
This is why the DLQ → Replay path is unconditionally safe: idempotency is inherited from event-time LWW.

---

## 3. Loop Prevention (score → snapshot → eval → score)

**Scenario.** The ML scorer writes a score *as a member fact*. That fact flows back through the
snapshot-builder → a new snapshot → the rules engine → a fresh eval → the scorer again. Left unchecked this is
an **infinite token-burning loop** — score begets eval begets score.

**Why it's tricky.** The router genuinely *needs* the eval re-emitted on every score change (so it always sees
fresh scores). So we can't just stop re-emitting on score changes — we have to break the loop **one hop later**,
at the scorer, without a body deserialize on the hot path.

**How it's handled.** The rules engine computes two signatures and emits a **type header**:

```java
// RulesEngine.evaluate
String eligHash = ...;  // eligibility identity (slug + content + ttl)
String fullHash = ...;  // eligibility + scores + states + completions
if (fullHash.equals(redis.get(fullKey))) return;            // nothing at all changed -> skip emit
boolean eligibilityChanged = !eligHash.equals(redis.get(eligKey));
rec.headers().add("type", (eligibilityChanged ? "eligibility" : "score") ...);
```

- `fullSig` includes scores/states/completions → an eval re-emits whenever **anything** relevant changes.
- `eligSig` is eligibility identity only → `eligibilityChanged=false` when *only scores* moved.

The scorer then discards score-only evals **by header, with no deserialization**:

```java
// MlScorer.runScorer
if (h != null && "score".equals(new String(h.value(), UTF_8))) continue;
// "re-scoring those is exactly what would loop, so we drop them here."
```

> **The loop is cut at exactly one place:** the scorer re-scores only on `type=eligibility`. The router still
> consumes the `type=score` evals (it wants fresh scores), so functionality is preserved while the cycle is broken.

---

## 4. Eventual Consistency of Throttle — *the key talking point*

**Scenario.** An operator (or the lake) drops a population-wide channel cap, e.g. `nba.throttle.email.daily = 5`.
8M members are now over-cap on email. What re-evaluates them?

**Why it's tricky.** **Nothing actively does — and that's correct.** The rules engine is *snapshot-driven*: it
only fires when a **new snapshot arrives for a member**. There is no "re-evaluate the population" sweep. A
throttle change produces **no member fact**, so it would never re-fire any member on its own.

**How it's handled — three converging mechanisms:**

| Layer | Mechanism | Code |
|---|---|---|
| **Eval** | The throttle level rides in on whatever snapshot the builder happens to be batching, broadcast on `nba.definitions`, retained per key with LWW, and applied to **every** member's next eval. | `GLOBAL_THROTTLE` + `for (e : GLOBAL_THROTTLE.entrySet()) f.put(e.getKey(), e.getValue()[0]);` |
| **Outbound** | The *binding* enforcement at send time is the Temporal **`ThrottleGate`**, evaluated right before handoff — **not** the eval. | `ChannelActionWorkflowImpl.activate` → `activities.throttleGate(act)` |
| **Inbound** | Serves the **eval cache** (`nba:evaluation:{nbaId}`) — last computed view, pull-only. | action-library `/next-action` |

The consequence everyone gets wrong: a member whose cached eval is **stale** (computed before the cap changed)
is **harmless**. If they never get a new snapshot, they're never re-evaluated — but they also never *send*,
because:
- **outbound** is gated at send-time by the `ThrottleGate` (which reads the *current* level), and
- **inbound** is a pull that, when the member *is* touched again, recomputes against the current level.

So the population **reconverges lazily** as members are naturally touched; the lake's gold snapshot catches the
rest. We never pay a 8M-member re-eval for a single toggle.

```java
// RulesEngine — comment that states the doctrine outright:
// authoritative for the channel cap for EVERY member, regardless of which member's traffic last advanced it.
```

**Tests.**
- `t_channel_throttle` — global `nba.throttle.email.daily=5` excludes email for everyone; push survives; cap
  lifts and email returns. Note the test must `feed1` a rulefact bump to *force a fresh snapshot* — proving the
  "no auto re-eval on a throttle change" semantics (the comment in `t_throttle_composes_with_member_cap` spells
  this out).
- `t_throttle_composes_with_member_cap` — the **global daily throttle** and the **per-member weekly cap**
  compose: either alone gates email, exercising them independently.
- `t_throttle_multichannel` — caps are **per-channel and only bite where an authored rule exists**: `sms` has
  no rule referencing `nba.throttle.sms.daily`, so the broadcast level is retained but **unenforced**.

---

## 5. Race: DISPATCH vs CANCEL (the action-layer)

**Scenario.** The state machine hands off a send (DISPATCH), then a supersede/operator-suppress arrives and it
emits a CANCEL. Did the cancel beat the actual send, or not?

**Why it's tricky.** Real sends have latency. A CANCEL is only meaningful if we can crisply decide *cancel won*
vs *too late*, with no double-send and no lost cancel under concurrency.

**How it's handled.** The action-layer holds each in-flight send in a `WALKS` `ConcurrentHashMap`; a background
**walker thread** advances each send one funnel step at a time (`w.step`, which starts below `0` = not-yet-sent).
A `CANCEL` is answered by inspecting how far the walk has progressed:

```java
// ActionLayer — CANCEL handler:
Walk w = WALKS.get(slug);
if (w == null || w.step < 0) {                 // walker hasn't fired the first disposition -> cancel wins
    WALKS.remove(slug);
    disposition(producer, ..., SUPPRESSED, "Cancelled");
} else {                                       // already walking (sent) -> too late
    disposition(producer, ..., SUPPRESS_FAILED, "AlreadySent");
}
```

The verdict turns on `w.step`: the walker bumps it to `0` when it emits the first delivery disposition, so the
CANCEL crisply observes pre-send vs sent:

| `w.step` at CANCEL | Disposition (canonical / raw) | Meaning |
|---|---|---|
| `< 0` (or no walk) | `SUPPRESSED` / `Cancelled` | cancel won — nothing was sent |
| `>= 0` | `SUPPRESS_FAILED` / `AlreadySent` | the send already fired — cancel too late |

**This is exactly what makes the workflow's `SUPPRESSING` state meaningful** (`ChannelActionWorkflowImpl`): a
suppress that arrives *before* the workflow hands off (during the debounce window, no `DISPATCH` yet) needs no
layer round-trip — the workflow just goes `DEBOUNCED` (nothing was ever dispatched). A suppress *after* handoff
is only a **request** — the workflow goes `SUPPRESSING`, emits a `CANCEL` activation, and **waits for the
layer's disposition** to learn whether the cancel won (`SUPPRESSED`) or the send already fired
(`SUPPRESS_FAILED` → resume to `IN_PROCESS`). Pre- vs post-handoff is the split.

> **Stale-verdict guard:** the workflow ignores a disposition whose `correlationId` doesn't match this run's
> (`disposition()` checks `myCorr`) — a verdict for a previous activation of the same workflow id can't settle
> the current one.

---

## 6. Supersede / Wait-Gate (the action-router)

**Scenario.** A lower-scored action is already in flight for a member. A higher-scored action becomes eligible.
We must promote the winner, retract the loser, and **never** end up sending both.

**Why it's tricky.** The router is stateless and purely fact-driven off the eval. A CANCEL it just emitted is
still settling (the layer's race in §5). If the router activated the new winner *now*, it could double-comm.

**How it's handled.** The router never inspects raw disposition/workflow states. The rules engine folds two
coarse flags onto each `ChannelAction` — `active` (a workflow is in flight, `workflowState ∈ ACTIVE_STATES`) and
`cancellable` (`workflowState == CREATED`, i.e. not sent yet) — and the router decides off those flags plus
`eligible` / `score` / `hardCompleted` (`ActionRouter.activate`). It emits exactly four ops: `CREATE`,
`SUPPRESS`, `SOFT_COMPLETE`, `HARD_COMPLETE`.

1. **Bridge completions** — for each ChannelAction, `hardCompleted` → `HARD_COMPLETE`, else `softCompleted` →
   `SOFT_COMPLETE` (idempotent; the workflow dedups).
2. **SUPPRESS the in-flight no-longer-eligibles** — every ChannelAction where `!eligible && cancellable` (its
   workflow still occupies the slot but it dropped out of eligibility).
3. **`cur`** = the slot occupant = the top-scored action that is `eligible` **and** `active`.
4. **`cand`** = the top-scored **free** candidate = `eligible`, not `active`, not `hardCompleted`, scored
   (`pickCandidate`; ties break on the smallest `slug`).
5. **Slot occupied (`cur != null`)?** Supersede **only** if the candidate out-scores the occupant *and* the
   occupant hasn't sent yet, then **return**:

```java
if (candScore > curScore && cur.path("cancellable").asBoolean(false))
    emit(... "SUPPRESS" ..., cur);
return;
```

   A *sent* occupant (`active` but not `cancellable`) owns the slot until its lifecycle closes, so the router
   **holds** — it does not CREATE over it.
6. **Slot free?** `CREATE` the candidate — single, or the top-N batch on the winning channel (`maxBatch > 1`).

When the router supersedes, it **returns** without creating the replacement in the same pass; the replacement is
`CREATE`d on a later eval, once `cur`'s workflow has left the active set (the SUPPRESS resolved). Because the
router only ever supersedes a **cancellable** (not-yet-sent) occupant and the state machine resolves the race —
`SUPPRESSED` if the cancel caught it, `SUPPRESS_FAILED` (the original send proceeds) if not — two comms can never
both go out.

> **The `::` vs `.` separator gotcha.** The router uses **two different separators** for two different purposes,
> and confusing them silently breaks suppression:
> - `slug(ca)` = `actionId + "::" + channel` — the eligibility/slot identity (matches the rules engine's `hits`
>   format `"actionId::channel"`).
> - `isSuppressed(...)` checks `suppressed.contains(aid)` **or** `suppressed.contains(aid + "." + channel)` —
>   a **single dot**, because operator suppression targets are authored as `actionId` (whole action) or
>   `actionId.channel` (one channel). The suppression set is keyed with `.`, the slot identity with `::`.

**Test.** `t_supersede` — Survey/push (0.61) activates first; a fact bump makes Reengage/email (0.66) eligible;
Survey ends `suppressed` (superseded) and Reengage reaches `sent`.

---

## 7. Batch (maxBatch > 1)

**Scenario.** A channel is configured `maxBatch = N`. Instead of N separate comms, the top-N free eligible
actions on the winning channel should go out as **one** send but track **N** independent outcomes.

**Why it's tricky.** One dispatch, N dispositions, N independent lifecycles — and the per-action trackers must
**outlive** the orchestrator that spawned them.

**How it's handled.**

| Step | Where | Detail |
|---|---|---|
| Select | `ActionRouter.selectBatch` | top-N FREE eligible on the winning channel, score-desc, capped at `maxBatch`. |
| One CREATE | `ActionRouter.emitBatch` | a single `op=CREATE` carrying `actions[]`, keyed `nbaId:channel:batch`. |
| Orchestrate | `BatchOrchestratorWorkflowImpl.orchestrate` | `signalWithStart` (debounce-dedupe to the latest top-N), then **start one tracking child per action FIRST**, then `emitBatchDispatch` (ONE activation). |
| Track | `ChannelActionWorkflowImpl.activate` (`preDispatched=true`) | each child **skips** the debounce/dedup/throttle/dispatch path and tracks only its own dispositions (the orchestrator already dispatched). |
| Fan-out dispositions | `ActionLayer.disposition` | a batch send fans out to **one disposition per action** (each with its own `trackingId`). |

`readMaxBatch` reads `nba:channel:maxbatch` (default 1). `maxBatch <= 1` → the single-action path (unchanged).

**Test.** `t_batch` — `al_maxbatch email 2`; both email actions (Reengage + Dashboard) reach `sent` from one
batch dispatch.

---

## 8. Missing Facts — type-zero defaults

**Scenario.** A member has never logged a `commsThisWeek` count, or has no `isDNC` flag. A rule references it.
What value does the missing fact take?

**Why it's tricky.** A missing fact must behave **deterministically** and **identically** in two code paths —
the compiled DRL (eligibility) and the plain-Java `treePass` (variant gating + milestones + completion) — or a
member could be eligible for the action but get the wrong content variant.

**How it's handled.** A missing fact reads as **its type's default**, inferred from the *comparison value*:

```java
// RulesEngine.defaultFor — number→0, boolean→false, string→""
static String defaultFor(JsonNode v) {
    if (v == null || v.isNull()) return "null";
    if (v.isBoolean()) return "false";
    if (v.isNumber())  return "0";
    return "\"\"";
}
// DRL: getOr("operator.comms.totalThisWeek", 0) > 3
```

The Java `condPass` mirrors it exactly: `actual == null ? 0.0 : ...` for numbers, `Boolean.parseBoolean` over a
null-coalesced string for booleans, `actual == null ? "" : ...` for strings. **`exists` is special-cased** in
*both* paths — it asks about presence and is never coalesced (`"exists".equals(cmp) → get(fact) != null`).

**Test.** `t_missing_facts_default` — a member with **only** `completedTasks=7`:
- missing `isDNC` → `false` → not excluded,
- missing counts → `0` → under all caps,
- missing `viewedDashboard` → `false` → Dashboard eligible,
- missing `daysSinceLogin` → `0` → Reengage (needs ≥14) **not** eligible.

Result asserted: `Survey/push + Dashboard/email`. (`t_dnc_excludes_survey` proves the inverse: an explicit
`isDNC=true` fires Survey's exclusion.)

---

## 9. Latch Permanence (milestones + hard completion)

**Scenario.** A member completes a milestone (or an action's goal). Later the underlying facts change so the
condition no longer holds. The completion must **stay** completed.

**Why it's tricky.** Completion is a *historical* fact ("they did the thing"), not a *current* predicate.
Re-deriving it each pass would let it flicker off when facts move.

**How it's handled — detect in the engine, publish in the router, carry on the snapshot.** The rules engine writes
**no latch key**. It DETECTS the transition over the snapshot and builds a per-eval transition array; the
action-router PUBLISHES the durable fact; the snapshot-builder folds that fact back so the engine reads it as a
perpetual fact on every later eval:

```java
// RulesEngine.evaluate — DETECT only; the durable fact (published by the router) rides the snapshot
// milestones: done = the nba.milestone.* facts already on the snapshot UNION any whose logic passes now
if (logic != null && treePass(logic, f) && !doneMs.containsKey(id)) newMs.add(id);     // transition (no fact yet)
// hard completion (per action): by criterion this eval OR the durable nba.completion.{actionId} fact present
boolean signalled = isTruthy(f.get("nba.completion." + actionId));
if (byCriterion && !signalled) newCompleted.add(actionId);                              // transition (no fact yet)

// ActionRouter.activate (~:116) — PUBLISH the durable facts straight off the transition arrays
for (JsonNode aid : eval.path("newCompleted"))
    emitFact(producer, outTopic, ..., "nba.completion." + aid.asText(), "true", "BOOL", "completion");
for (JsonNode m : eval.path("newMilestones"))
    emitFact(producer, outTopic, ..., "nba.milestone." + m.path("id").asText(), ..., "LONG", "milestone");
```

Once the router publishes the fact, the completed set is **carried perpetually as a snapshot fact** and rides
every subsequent eval — it is never re-derived from live facts, and (because the disposition/completion fact is
terminal on the snapshot) it can never flicker off. Milestones ride on `fullSig` (a fresh completion emits) but
**not** `eligSig` (a milestone isn't an eligibility change), so completing one doesn't trigger a needless re-score.

**The `autoExcludeOnCompletion` mode (default ON).** Hard completion drives eligibility:

```java
boolean completed = doneCompleted.containsKey(p[0]);
if (completed && a.path("autoExcludeOnCompletion").asBoolean(true)) continue;   // retired, every channel
```

- **Default (`true`):** a completed member **drops the action from eligibility entirely** — every channel,
  retired.
- **`false`:** completion is still **tracked/latched** (`hardCompleted: true` on the ChannelAction; the durable
  record is the terminal `HARD_COMPLETED` workflow state) but eligibility is handed back to the operator's own
  rules — the entry stays `eligible: true` permanently, no auto-retire.

**Test.** `t_hard_completion` — three sub-cases: criterion path auto-retires when the goal flips; the API path
(`POST /completion` → `nba.completion.{cid}`) auto-retires too; and `autoExcludeOnCompletion=false` flips
`hardCompleted` to true **while the action stays in the eligible set**.

---

## 10. Throttle Saturation Reroute (channel HOT until midnight)

**Scenario.** A channel's daily *absolute* ceiling is fine, but the gate predicts the channel **can't clear its
backlog before midnight** at its rate cap. The member's action on that channel should be rerouted *now* — not
left waiting all day.

**Why it's tricky.** This is a *predictive* eligibility change ("won't make it out today"), distinct from the
hard daily ceiling (§4). It originates in the **Temporal worker** at send time and must flow *back* into
eligibility so the ML layer re-scores onto another channel — and it must **self-expire** without an operator.

**How it's handled — the full loop:**

1. **Gate predicts saturation** (`ThrottleGate.admit` → `SUPPRESS`): window full **and**
   `backlog >= rateCap × windowsLeftToday`.
2. **Workflow emits a throttle-reason suppression** (`ChannelActionWorkflowImpl`):
   `emitReason(act, "SUPPRESSED", "throttle")` → `activities.emitStateReason(act, "SUPPRESSED", "throttle")`.
3. **Snapshot-builder routes it by header** to `nba.definitions` as `THROTTLE_HOT:{channel}` (the channel is
   parsed from the fact key, *not* the kafka key — robust to memberId re-keying):
   ```java
   if ("throttle-suppress".equals(kind)) {
       String channel = fkey.substring(fkey.lastIndexOf('.') + 1);
       plan.forwards.add(new Forward(defsTopic, "THROTTLE_HOT:" + channel, p.raw(), null, null));
   }
   ```
4. **Rules engine marks the channel HOT until midnight** (`applyThrottleHot` → `CHANNEL_HOT_UNTIL`) and **drops
   HOT channels from eligibility** so ML re-scores onto another channel:
   ```java
   Long hotUntil = CHANNEL_HOT_UNTIL.get(p[1]);
   if (hotUntil != null && System.currentTimeMillis() < hotUntil) continue;   // self-expires at midnight
   ```

**Two subtle guards in `applyThrottleHot`** (because `THROTTLE_HOT` is a transient event landing on a
**compacted** topic, so it *replays on restart*):
- A future `eventTs` (clock skew / bad data) is ignored: `if (eventTs > now + 60_000) return;`
- A HOT from a previous day is ignored so a restart doesn't resurrect old saturation:
  `if (eventTs / 86_400_000L < now / 86_400_000L) return;`

**Idempotent reopen.** Explicitly opening a channel (`daily → 0`, operator/test source, **not** the routine
`throttle-lake` telemetry) also **clears any saturation HOT** so a stale HOT can't linger and bleed into later
evals (`applyThrottle` → `CHANNEL_HOT_UNTIL.remove(ch)`). The `throttle-lake` source is gated out so routine
count telemetry can't race-clear a live saturation.

**Tests.**
- `t_throttle_saturation` — `throttle_hot email` → email excluded, ML re-scores to Survey/push. (Runs **last**
  in the suite because it marks email HOT.)
- `t_throttle_sms_fallback` / `t_throttle_lifecycle_sms` — the headline flow: email capped → the send actually
  goes out on **SMS** instead.

---

## 11. Operator Suppression — enforced at the READ edges

**Scenario.** An operator suppresses an action (or one action-channel) from the Command Center. It must take
effect **immediately**, including for members who won't get a new fact for days.

**Why it's tricky.** Suppression produces **no member fact**, and the rules engine is snapshot-driven — so it
**cannot** be enforced in the eval without re-evaluating the whole population (§4). You can't rely on a fresh
fact arriving in prod.

**How it's handled — enforced at the two READ edges, never in the eval:**

| Edge | Code | Behavior |
|---|---|---|
| **Outbound** (router) | `ActionRouter.activate` → `isSuppressed(SUPPRESSED, ca)` | never *activates* a suppressed action; CANCELs it if already in flight. In-memory `SUPPRESSED` mirror fed by the compacted `nba.definitions` broadcast (`ACTION_SUPPRESS:{target}`) — a pure HashSet lookup, **no Redis round-trip** on the hot path. |
| **Inbound** (serve) | action-library `/next-action` | strips suppressed actions live off the `nba:suppressed` Redis set. |

The eval itself stays **suppression-agnostic** — a suppressed action is "still sitting there." The rules engine
explicitly does **not** filter it (`RulesEngine.evaluate`):

```java
// NOTE: operator suppression is NOT filtered here. ... enforcement happens at the READ edges ...
// That avoids re-evaluating 8M members on a toggle; the lake's gold snapshot reconverges lazily.
```

`ACTION_SUPPRESS` is even a **no-op in the rules engine's `applyDef`** (`return false` — no DRL rebuild) — it's
purely a read-edge concern.

> **The `::` vs `.` key gotcha (again, and it bites here).** The rules engine's Drools `hits` are
> `actionId::channel` (double-colon). But the suppression set is keyed with a **single dot** —
> `isSuppressed` checks `suppressed.contains(aid)` (whole action) or `suppressed.contains(aid + "." + channel)`
> (one channel). Whole-action targets are bare `actionId`; channel targets are `actionId.channel`. If the
> suppression cache were ever keyed with `::` to "match the slug," **channel-level suppression would silently
> never match** and the action would keep firing. The separators are deliberately different because they index
> different things: `::` = the eligibility slot identity, `.` = the operator's suppression target.

**Tests** — note these assert on `served_actions` (the **inbound serve**), *not* `get_eligible` (the raw eval),
precisely because the raw eval still lists suppressed actions and the **serve** strips them:
- `t_operator_suppress` — suppress a whole action → inbound stops serving it **immediately** with no new fact;
  unsuppress restores it.
- `t_operator_suppress_channel` — suppress `actionId.channel` → only that channel is stripped; restore brings
  it back.

---

## 12. Hot-Path Optimistic Write-Through — the API is *just* the hot path

**Scenario.** A real hot path — presented facts on `GET /next-action`, or an inbound `POST /disposition` — must
make the very *next* read fresh (even a no-facts serve a moment later) without waiting a full flywheel cycle.
The action-library API best-effort writes Redis directly. Two such writes can race each other, or race the
authoritative writers (snapshot-builder, action-router).

**Why it's tricky.** The single-writer model (§2) says the snapshot's authoritative writer is the
**snapshot-builder** and eligibility's is the **action-router**. A hot-path write that touches `nba:snapshot` /
`nba:eligibility` directly is a *second* writer — exactly the kind of thing that corrupts state if it wins or
loses a race uncontrolled.

**How it's handled.** The hot path is framed as an **optimistic accelerator**, never the source of truth:

| Write | Store | Mechanism |
|---|---|---|
| LWW-merge presented facts | `nba:snapshot:{nbaId}` (the `fact:` fields) | plain Jedis, back-to-back (no Lua, no `MULTI`) |
| Refresh channelAction scores | `nba:eligibility:{nbaId}` | read-modify-write |
| Emit presented facts (DURABLE) | `nba.member.facts` (member-fact shape, `source=hotpath`) | the bus path |

The bus emit is the **self-heal**: the snapshot-builder re-applies those facts under **event-time LWW** (§1).
So if the optimistic Redis write **loses a race or fails**, the bus reconciles it — and because every writer
is LWW on `eventTs`, the writers are **commutative** (order of arrival doesn't matter). The whole write-through
is best-effort, wrapped in try/catch — it **never fails the decision**. No Lua, no distributed transaction.

> **Design framing:** the API is *just* the hot path. Kafka + the snapshot-builder remain the source of truth;
> the hot path's direct writes are best-effort and bus-reconciled. The single-writer model **holds** because
> event-time LWW makes the second writer's writes idempotent against the authoritative writer's.

**Bounded staleness on the no-facts path.** The write-through is gated on facts being presented. A **no-facts
serve** reads the **cached eligibility** (the router's last eval) — never a stale-write hazard, just bounded
staleness that refreshes each flywheel cycle.

---

## 13. Per-Channel Touch-Template Escalation — only the *settled* dispatch escalates

**Scenario.** A channel carries a sequence of touch templates (`touchKeys = [firstTouch, secondTouch,
thirdTouch]`). The Nth actual send on that channel must pick template N — but debounced, suppressed, and
throttled *attempts* must **not** advance the sequence, and concurrent batch sends must not double-count.

**Why it's tricky.** A naive per-(member, action, channel) workflow counter can't see a member's *other*
actions' sends on the same channel — yet the touch number is per-CHANNEL (a prior email of ANY action counts
toward the next action's touch number). And an attempt that the funnel later *debounces away* must leave the
counter untouched, or escalation drifts ahead of reality.

**How it's handled.** A **monotonic per-(member, channel) counter** lives in the actionlib Postgres, **not** in
the workflow:

- Table `channel_touch (nba_id, channel, n)`, created by the temporal-worker on startup.
- Bumped at the **single DISPATCH point** — `emitActivation` (single) and `emitBatchDispatch` (batch) — **after**
  debounce / suppress / throttle have all settled. So debounced / suppressed / throttled attempts **NEVER
  escalate**. The counter **never resets**.
- Per-CHANNEL regardless of action — every send on a touch-configured channel bumps the same counter. That is
  precisely why it lives in the send activity, not the workflow (a per-(member,action,channel) workflow can't
  track cross-action sends).
- Template selection: `touchKeys[min(n, len) - 1]` — caps at the last template. Absent `touchKeys` → the
  variant-selected `contentKey` is used unchanged.

**Concurrency.** The bump is an atomic `INSERT ... ON CONFLICT ... RETURNING n` — **race-free for concurrent
batch sends** (two sends on the same `(nba_id, channel)` get distinct, monotonically increasing `n`).

---

## 14. Inbound Serve → Disposition Tracking — fire-and-forget, no outbox

**Scenario.** An inbound serve (`GET /next-action`) and its later `POST /disposition` must be **linkable** as one
journey in the lake + the Command Center member timeline. But the serve/disposition tracking is *telemetry*, not
state — losing one must never corrupt member state.

**Why it's tricky.** The rest of the pipeline emits via the transactional **outbox** (§2) so DB-write and
event-emit are atomic. Tracking events have no business DB write to be atomic *with*, and putting them on the
outbox would couple telemetry loss to state correctness.

**How it's handled.** The serve stamps a `correlationId` and emits `INBOUND_SERVE`; `POST /disposition` accepts
that correlationId, emits `INBOUND_DISPOSITION` linked to it, then stamps a **new** correlationId on the
next-served set and emits its own `INBOUND_SERVE`. Both are **DIRECT-to-Kafka** on `nba.activations`
(`op = INBOUND_SERVE` / `INBOUND_DISPOSITION`, `source=inbound`) — **fire-and-forget, NO outbox, no distributed
transaction**.

> A **lost tracking event doesn't corrupt state** — it's pure observability. The serve→disposition journey is
> linkable in the lake, but member state lives entirely on the durable (outbox / `nba.member.facts`) path.

Outbound dispatches now also carry `nbaId` / `entityId` attribution (single-action `emitActivation`), so
outbound sends are countable too (an action can re-dispatch after `EXPIRED`).

---

## Appendix — the consistency model in one table

| Mechanism | Scope | Re-fires the population? | Self-heals by |
|---|---|---|---|
| Event-time LWW (§1) | per `(member, factKey)` | n/a | newer `eventTs` always wins |
| Snapshot EOS + whole-batch retry (§2) | per batch | no | additive re-emit; LWW absorbs replays |
| `eligibilityChanged` + type header (§3) | per eval | no | scorer ignores `type=score` |
| Global throttle (§4) | population (broadcast) | **no** | members re-eval on their next snapshot; gate binds at send-time |
| DISPATCH/CANCEL claim race (§5) | per activation | no | walker `step` gate (`SUPPRESSED`/`SUPPRESS_FAILED`); corr-id guard |
| Supersede + hold (§6) | per member | no | holds while a sent action owns the slot; supersedes only a `cancellable` occupant |
| Hard/soft completion (§9) | per `(member, action)` | no | durable `nba.completion.*` fact (router-published) carried perpetually on the snapshot |
| Saturation HOT (§10) | per channel (broadcast) | **no** | expires at midnight; explicit reopen clears it |
| Operator suppression (§11) | population (broadcast) | **no** | enforced at read edges; gold snapshot reconverges lazily |
| Hot-path write-through (§12) | per `(member)` | no | best-effort Redis warm; bus re-applies (event-time LWW, commutative writers) |
| Touch-template escalation (§13) | per `(member, channel)` | no | monotonic counter bumped only at settled DISPATCH; atomic `INSERT…ON CONFLICT…RETURNING` |
| Inbound serve/disposition tracking (§14) | per serve→disposition | no | fire-and-forget direct-to-Kafka; lost event = telemetry gap, never state corruption |

The recurring theme: **population-wide changes are never push-evaluated.** They broadcast on `nba.definitions`,
are applied at each member's *next natural touch*, and are *bound* at the read/send edges in the meantime — so a
"stale" eval is always harmless.
