# The Rules Engine — dynamic Drools from the definitions topic

How the NBA rules engine turns operator-authored JSON into live Drools rules, and how it evaluates a member
snapshot into the unified `channelActions[]` evaluation. This is the deep-dive companion to the per-service
summary in [06-component-specs.md](06-component-specs.md).

Source: `nba/services/rules-engine/src/main/java/ai/das/nba/rules/` — `RulesEngine.java` (everything) and
`Snap.java` (the Drools working-memory fact). Drools `8.44.0.Final` (`drools-compiler` + `drools-mvel`),
embedded — there are **no `.drl` files in the repo**; every rule is synthesized at runtime.

---

## 1. What it is

The rules engine is the **eligibility brain**. It answers, per member: *which (action, channel) pairs is this
member allowed to receive right now?* It does **not** rank them (that's the ml-scorer) or send anything (that's
the router + state machine). Its only output is `nba.evaluations` — a unified `channelActions[]` list where each
entry self-describes its eligibility, live workflow state, score (carried over), and completion flags.

The defining trait: **rules are data, compiled at runtime.** Operators author actions and rules as structured
JSON **condition trees** in the Command Center; the action-library writes them (via the outbox) to the compacted
`nba.definitions` topic; the rules engine consumes that topic, **synthesizes Drools DRL** from the JSON, and
rebuilds its `KieBase` on every structural change. An operator edit is live within one Kafka round-trip — no
redeploy, no `.drl` authoring.

---

## 2. Two threads, two topics

`main` (`RulesEngine.java:79`) starts two independent consumers:

| Thread | Consumes | Group | Job |
|--------|----------|-------|-----|
| `defs-consumer` (`runDefsConsumer`, :99) | `nba.definitions` | `rules-engine-defs-{random}` (fresh, `earliest`) | Keep the in-memory definition stores + the compiled `KieBase` current. |
| `snapshots-consumer` (main, `runSnapshotsConsumer`, :115) | `nba.snapshots` | `rules-engine-snapshots` (stable, auto-commit) | Evaluate each snapshot against the current `KieBase` → emit `nba.evaluations`. |

The snapshots consumer **blocks until the first `KieBase` is built** (`:118`) so it never evaluates against an
empty pack. The definitions group is a **fresh random group reading from `earliest`** every boot — the compacted
`nba.definitions` topic replays the entire current definition set into each new pod, so every instance
self-hydrates its rule pack on startup with no shared state.

In-memory definition stores (all `ConcurrentHashMap`, `:46-51`):

- `actions` — `ACTION:{id}` docs (inclusion/exclusion/completion trees, channels, ttl, variants).
- `globalRules` — `GLOBAL_RULE:{id}` (apply to every action-channel).
- `channelRules` — `CHANNEL_RULE:{id}` (apply to one channel).
- `milestones` — `MILESTONE:{id}` (evaluated in Java, not Drools — see §6).

Plus two population-wide state maps fed off the same topic: `GLOBAL_THROTTLE` (`:58`) and `CHANNEL_HOT_UNTIL`
(`:64`) — see §7.

---

## 3. The definitions topic — keys, types, and what triggers a rebuild

Every record on `nba.definitions` is keyed `TYPE:id`. `applyDef(key, value)` (`:485`) routes by the `TYPE`
prefix and returns a **boolean: did this change the rule structure?** Only a `true` triggers a `KieBase`
rebuild — and the defs-consumer rebuilds **once per poll batch**, not per record (`:111`).

| Key prefix | Stored / action | Rebuilds DRL? |
|------------|-----------------|:---:|
| `ACTION:{id}` | `actions.put/remove` | ✅ yes |
| `GLOBAL_RULE:{id}` | `globalRules.put/remove` | ✅ yes |
| `CHANNEL_RULE:{id}` | `channelRules.put/remove` | ✅ yes |
| `MILESTONE:{id}` | `milestones.put/remove` (Java-evaluated) | ❌ no |
| `THROTTLE:{ch}.{metric}` | `applyThrottle` → `GLOBAL_THROTTLE` (LWW per key) | ❌ no (eval-time data) |
| `THROTTLE_HOT:{channel}` | `applyThrottleHot` → `CHANNEL_HOT_UNTIL` | ❌ no |
| `ACTION_SUPPRESS:{target}` | **no-op** (`return false`) | ❌ no — enforced at the read edges |

A `null` value is a compaction **tombstone**: for the store types it removes the entry (`:504`). The
distinction is deliberate — a throttle-*level* update or an operator suppression is **eval-time data or a
read-edge concern**, not rule structure, so it must not pay for a DRL recompile.

`ACTION_SUPPRESS` being a literal no-op (`:492`) is the engine's statement that it is **not** the place to
enforce operator suppression: the engine is snapshot-driven, so a bare suppress (which produces no member fact)
would never re-fire a member. Suppression is enforced at the router (won't activate) and the inbound serve
(strips it) — see §10.

---

## 4. Dynamic DRL generation (the heart)

On any structural change, `rebuild()` (`:562`) calls `buildDrl()`, compiles it with a `KieHelper`, and
**hot-swaps** the result into the `volatile KieBase kieBase` field (`:71`) — in-flight evaluations keep using the
old pack until they finish their `newKieSession()`; the next eval picks up the new one. A compile failure logs
the offending DRL and **keeps the last good `KieBase`** (`:574`) rather than going dark.

### 4.1 The fact: `Snap`

Each snapshot becomes one Drools working-memory fact, `Snap` (`Snap.java`): a `nbaId` plus a
`Map<String,Object> f` of `factKey → typed value` (Boolean / Long / Double / String, coerced in `typedValue`,
`:359`). The generated rules never reference Java fields — they call two accessors:

- `get("key")` — the raw value or `null` (`Snap.java:18`).
- `getOr("key", default)` — value, or `default` when the fact is **absent** (`Snap.java:24`).

`getOr` is what makes a **missing fact behave as its type's default** (0 / false / ""), so a brand-new member
with almost no facts isn't accidentally excluded or included.

### 4.2 One rule per action-channel

`buildDrl()` (`:580`) emits a preamble (`import ai.das.nba.rules.Snap; global java.util.List results;`) then,
for **every (action, channel) pair**, one rule:

```drl
rule "elig::{actionId}::{channel}"
dialect "mvel"
when
  Snap( {inclusion} && {globalRules} && {channelRules} )
  not Snap( {exclusion} )
then
  results.add("{actionId}::{channel}");
end
```

- **inclusion** (`exprForTree(a.inclusion)`) — the action's own gate.
- **globalRules** (`andRules(globalRules)`) — every global rule ANDed; applies to all actions.
- **channelRules** (`andRules(channelRulesFor(ch))`) — every channel rule for *this* channel, ANDed.
- The three are joined with `&&`; if all are empty, `Snap()` matches any member (`:600`).
- **exclusion** becomes a `not Snap(...)` negative pattern (`:604`) — a hit on the exclusion tree removes the
  member.

Firing the session collects the passing slugs into `results` as `"actionId::channel"` strings — the **hit list**.
The `::` double-colon is the eligibility/slot identity used throughout the pipeline (distinct from the single
dot the operator-suppression target uses; confusing them silently breaks channel-level suppression — see
[05-corner-cases.md](05-corner-cases.md#11-operator-suppression--enforced-at-the-read-edges)).

### 4.3 Condition tree → MVEL

A **condition node** is either a branch `{ op: "all"|"any", conditions: [...] }` or a leaf
`{ fact, cmp, value }`. `exprForTree` (`:612`) joins child expressions with `&&` (`all`) or `||` (`any`),
wrapped in parens; `exprForCond` (`:625`) renders a leaf:

| `cmp` | Rendered MVEL | Note |
|-------|---------------|------|
| `exists` | `get("fact") != null` | presence test — **never** coalesced to a default |
| `eq` / `ne` | `getOr("fact", <default>) == / != <val>` | |
| `gt` `gte` `lt` `lte` | `getOr("fact", <default>) > …` | numeric |
| `in` | (Java path only — comma-separated allow-list, see §6) | |

The left side is always `getOr("fact", defaultFor(value))` — `defaultFor` (`:647`) infers the type default from
the **comparison value's** type: number→`0`, boolean→`false`, string→`""`, null→`null`. `renderVal` (`:654`)
emits the right side (quoting + escaping strings). So `daysSinceLogin gte 14` compiles to
`getOr("operator.activity.daysSinceLogin", 0) >= 14` — a member who never logged that fact reads as `0` and is
(correctly) ineligible for a 14-day reengagement.

`channelRulesFor(ch)` (`:670`) deliberately **skips `.rate` rules** (`referencesRate`, `:681`): a rate cap is
gate-only pacing (the Temporal `ThrottleGate` trickles sends against it); enforcing it as *eligibility* would
reroute the action the instant the rate is hit instead of letting it wait. Only `.daily` (the hard ceiling) is
eligibility.

---

## 5. Evaluating a snapshot → the unified `channelActions[]`

`evaluate(snapJson, …)` (`:147`) is the per-snapshot pipeline. In order:

1. **Build the fact map** `f` from `snap.facts` (`:150`), coercing each to a typed value.
2. **Inject the global throttle level** into `f` (`:162`): for every key in `GLOBAL_THROTTLE`, overwrite the
   member's value with the population level. This is the "make it global" step — a population-wide cap throttles
   *everyone*, regardless of whose snapshot last carried the level in.
3. **Latch milestones** (`:168`): for each milestone whose `logic` tree passes `treePass(logic, f)`,
   `HSETNX nba:milestones:{nbaId}` — permanent, never overwritten even if facts later move. Each milestone that
   passes **and was fact-absent** this eval (`treePass && fact-absent`) is also collected into the per-eval
   transition array **`newMilestones[]`** — the just-transitioned milestones (see §5.2).
4. **Latch hard completions** (`:178`): for each action, if its `completion` tree passes **or** an explicit
   `nba.completion.{actionId}` signal fact is truthy (`isTruthy`, `:444`), `HSETNX nba:completed:{nbaId}`.
   Then read the latched completed set back (`:185`). Each action whose completion criterion **just** became true
   (`byCriterion && !already-signalled`) is collected into the per-eval transition array **`newCompleted[]`** — the
   just-completed actions (see §5.2).
5. **Latch channel opt-outs** (`:190`): scan `nba.disposition.*` facts; if a channel's latest disposition equals
   that channel's opt-out raw status (`OPTOUT_RAW`: email `Unsubscribe`, sms `STOP`, `:467`),
   `HSETNX nba:optout:{nbaId}` for that channel. This is a **built-in, always-on compliance channel rule** — only
   the hard, legally-binding opt-outs latch; a voice `Declined` / push `Dismissed` is a negative disposition the
   model learns from, not a permanent removal.
6. **Run eligibility** (`:205`): either inline (`session.fireAllRules()` over a fresh `KieSession`, default) or
   offloaded to the standalone KIE server (`NBA_RULES_MODE=kie`, see §9). Both yield the same hit-slug list.
7. **Build the candidate set** (`:242`): every Drools hit, **plus** every action-channel that still has a live
   workflow (`nba.actionstate.*` ∈ `ACTIVE_STATES`, `:76`) even if it fell out of eligibility — so an in-flight
   action, or one walking to `HARD_COMPLETED`, stays on the eval for the router to manage.
8. **Enrich each candidate** into a `ChannelAction` (`:259`):
   - `eligible` = Drools hit **and** not throttle-HOT **and** not auto-excluded-on-completion **and** not
     channel-opted-out (`:275`). A candidate that's neither eligible nor in-flight is dropped (`:276`).
   - `score` carried over from the `nba.score.{aid}.{ch}` fact (null until the ml-scorer scores it, `:287`).
   - `workflowState` (the live state fact), `active` (`state ∈ ACTIVE_STATES`), `cancellable` (`state == CREATED`
     → router can SUPPRESS to replace) (`:290-292`).
   - `hardCompleted` (from the latch), `softCompleted` (recomputed each eval: `softCompleted(a, ch, disp)`, the
     channel's disposition vs the soft bar, `:296`), and `optedOut` surfaced for observability (`:299`).
   - `contentKey` chosen per member by **variant selection** (`contentKeyFor`, §8).
9. **Ride the latched milestones** on the eval (`:309`) from `nba:milestones:{nbaId}` — the perpetual
   `milestones[]`, permanent, read from Redis, never re-derived. The perpetual `completed[]` rides the same way
   (from the latched completed set). Alongside them the eval carries the two **transient** transition arrays
   `newCompleted[]` / `newMilestones[]` from steps 3–4 — see §5.2.
10. **Decide whether to emit** (§5.1) and write the eval + headers + cache.

### 5.1 Change detection, the emit decision, and the loop break

Two signatures are computed over the eval (`:236`, `:301`):

- **`eligSig`** — the *eligibility identity*: the sorted set of `aid::ch::contentKey::ttl` for eligible entries.
- **`fullSig`** — `eligSig` plus each entry's score, state, soft/hard flags, plus completed milestones.

The engine:

- **Skips the emit entirely** if `fullSig` is unchanged from `nba:eval:fullsig:{nbaId}` (`:334`) — nothing
  relevant moved.
- Otherwise sets **`eligibilityChanged`** = `eligSig` differs from `nba:eval:eligsig:{nbaId}` (`:342`), stores
  both signatures, and emits the eval to `nba.evaluations` (keyed `nbaId`) with a **`type` header**:
  `eligibility` when the eligible set moved, `score` when only scores did (`:351`).
- Caches the full eval at `nba:evaluation:{nbaId}` (`:354`) for the action-library inbound pull-serve.

That `type` header is the **loop-breaker**. The ml-scorer writes scores *as member facts*, which flow back into
a new snapshot → a fresh eval. If the scorer re-scored on every eval it would spin forever (score → eval →
score). Instead the scorer drops `type=score` evals by header without deserializing, re-scoring only on
`type=eligibility`. The router consumes both (it always wants fresh scores). See
[05-corner-cases.md](05-corner-cases.md#3-loop-prevention-score--snapshot--eval--score).

### 5.2 Transition arrays — the rules-engine computes, the action-router publishes

The engine no longer **emits** the durable `nba.completion.{actionId}` / `nba.milestone.{id}` facts itself. It only
**detects the transitions** and carries them on the eval as two transient arrays:

- **`newCompleted[]`** — actions whose completion criterion *just* became true this eval (`byCriterion &&
  !already-signalled`), from step 4.
- **`newMilestones[]`** — milestone trees that *just* passed this eval (`treePass && fact-absent`), from step 3.

The **action-router** consumes the eval and **publishes** the `nba.completion.{actionId}` / `nba.milestone.{id}`
facts FROM these arrays — it owns the publish, off the eval message, with no diffing and no redundant
eligibility-cache read (the transition is already computed). Before the router persists the single eligibility
object (`nba:eligibility:{nbaId}`), it **strips** the transient `newCompleted` / `newMilestones`; only the
perpetual `completed[]` / `milestones[]` stay on the persisted object. So the `new*` arrays are an eval-only
signal — present on the wire, absent from the durable eligibility state.

(Previously the rules-engine emitted these completion/milestone facts directly during the latch steps; that
emission has moved to the router, which already reads every eval and now publishes off the precomputed
transition arrays rather than re-deriving them.)

---

## 6. Two evaluators, same grammar: compiled DRL vs Java `treePass`

The exact same condition-tree grammar is evaluated in **two** places, and they are kept semantically identical:

| Evaluator | Used for | Code |
|-----------|----------|------|
| **Compiled DRL** (MVEL, in the `KieBase`) | action-channel **eligibility** (the hit list) | `buildDrl` / `exprForCond` |
| **Java `treePass`** (interpreted, off the fact map) | **milestones**, **hard-completion criteria**, and content **variant** gates | `treePass` (`:404`) / `condPass` (`:417`) |

`treePass` mirrors the DRL semantics exactly — missing fact = type default, `exists` special-cased and never
coalesced (`:422`), `in` as a comma-separated allow-list (`:436`), numeric/boolean/string comparisons matched to
the value's type. Variant/milestone/completion gating runs **post-eligibility off the eval's fact map** (no
Drools session needed), so it doesn't pay for a rule firing — but it must agree with the DRL bit-for-bit, or a
member could be eligible for an action yet get the wrong content variant.

---

## 7. Throttle: three distinct mechanisms

| Mechanism | Source | Effect | Code |
|-----------|--------|--------|------|
| **Global daily cap** (`nba.throttle.{ch}.daily`) | lake `throttle-emit` | LWW-retained in `GLOBAL_THROTTLE`, injected into every eval, read by an authored `.daily` channel rule → **eligibility** | `applyThrottle` (`:539`) |
| **Rate cap** (`nba.throttle.{ch}.rate`) | lake | **Not** eligibility — gate-only; the Temporal `ThrottleGate` trickles sends against it. Skipped in DRL by `referencesRate` | `channelRulesFor` (`:670`) |
| **Saturation HOT** (`THROTTLE_HOT:{ch}`) | Temporal gate predicting "can't clear the backlog before midnight" | Marks the channel ineligible in `CHANNEL_HOT_UNTIL` **until midnight** so ML reroutes; self-expires | `applyThrottleHot` (`:518`) |

The saturation HOT is a transient *event* that lands on a **compacted** topic, so it **replays on restart**.
`applyThrottleHot` guards against that: a future `eventTs` (clock skew) is ignored (`> now + 60_000`), and a HOT
from a previous day is ignored (`eventTs / 86_400_000L < now / 86_400_000L`, `:531`) so a restart can't
resurrect yesterday's saturation; the default window runs until the next midnight boundary. Explicitly
**reopening** a channel (`daily → 0` from a non-`throttle-lake` source) also clears any lingering HOT
(`applyThrottle`, `:555`) so routine count telemetry can't race-clear a live saturation.

---

## 8. Content variant selection (A/B + targeting)

A channel can carry **content variants** — each with its own `contentKey`, an optional `percent` split, and an
optional `conditions` fact gate. `contentKeyFor` (`:373`) tries variants in order and returns the **first** whose
`conditions` pass (`treePass`) and whose member-bucket falls under `percent`; otherwise the base `contentKey`.
The bucket is `Math.floorMod(hash(memberKey + ":" + ch + ":" + idx), 100)` (`:390`) — **deterministic and stable
per member**, so the same member always lands in the same variant (no flapping), while the population splits at
the authored percentage.

---

## 9. KIE-server mode (horizontal scale-out)

By default the Drools session runs **inline** in the rules-engine pod. Setting `NBA_RULES_MODE=kie` (`:688`)
offloads just the `fireAllRules` step to the standalone **`nba-kie-server`** (Javalin on `:7010`,
`services/kie-server`): the engine POSTs `{nbaId, facts}` to `KIE_URL/evaluate` (`kieServerEval`, `:694`) and gets
back the same hit slugs. Everything else (enrichment, latches, throttle, signatures, emit) stays in the
rules-engine. The KIE server consumes `nba.definitions` and builds the **identical DRL** from the same condition
trees (its `Snap` and `buildDrl` are copies), so the same rules fire. If the KIE server is unreachable the eval
is skipped (`:208`) and the snapshot re-flows later — fail-safe, not fail-open. This is purely a load-test /
scale knob; the decision logic is unchanged.

---

## 10. What the engine deliberately does *not* do

- **It doesn't rank.** Scores are carried over from `nba.score.*` facts written by the ml-scorer; the engine
  only attaches them.
- **It no longer publishes completion/milestone facts.** It computes the `newCompleted[]` / `newMilestones[]`
  transition arrays (§5.2) and rides them on the eval; the **action-router** publishes the durable
  `nba.completion.{actionId}` / `nba.milestone.{id}` facts off those arrays. The engine still latches the
  perpetual `nba:completed:*` / `nba:milestones:*` sets in Redis (steps 3–4).
- **It doesn't enforce operator suppression.** `ACTION_SUPPRESS` is a no-op (§3). Enforcement is at the read
  edges (router won't activate, inbound serve strips) because a snapshot-driven engine can't re-fire a member on
  a bare suppress that carries no member fact.
- **It doesn't re-evaluate the population on a population-wide change.** A throttle/suppress/HOT toggle produces
  no member fact, so it re-fires nobody on its own; members reconverge on their **next natural snapshot**, and
  the change is *bound* at the send/read edges in the meantime. This is the core consistency doctrine — see the
  [appendix table in 05](05-corner-cases.md#appendix--the-consistency-model-in-one-table).

---

## 11. Configuration

| Env | Default | Purpose |
|-----|---------|---------|
| `NBA_BOOTSTRAP` | `nba-redpanda:9092` | Kafka. |
| `NBA_DEFINITIONS_TOPIC` | `nba.definitions` | Definitions in. |
| `NBA_SNAPSHOTS_TOPIC` | `nba.snapshots` | Snapshots in. |
| `NBA_EVALUATIONS_TOPIC` | `nba.evaluations` | Evaluations out. |
| `NBA_REDIS_HOST` | `nba-redis` | Latches (`nba:milestones:*`, `nba:completed:*`, `nba:optout:*`), eval cache (`nba:evaluation:*`), change-detection signatures (`nba:eval:fullsig:*`, `nba:eval:eligsig:*`). |
| `NBA_RULES_MODE` | `embedded` | `kie` offloads Drools to `nba-kie-server`. |
| `NBA_KIE_URL` | `http://nba-kie-server:7010` | KIE server endpoint (kie mode). |
| `NBA_THROTTLE_HOT_TTL_SECONDS` | `0` | Saturation HOT window; `0` = until midnight (positive = test override). |

---

## At a glance

```
nba.definitions ──▶ defs-consumer ──▶ applyDef ──▶ {actions, globalRules, channelRules, milestones,
   (compacted)                          │            GLOBAL_THROTTLE, CHANNEL_HOT_UNTIL}
                                        └──structural change──▶ buildDrl() ──▶ KieHelper ──▶ volatile KieBase (hot-swap)

nba.snapshots ──▶ evaluate() ──▶ build fact map ──▶ inject global throttle ──▶ latch milestones/completions/opt-outs
                                     │
                                     ├─▶ fireAllRules (or KIE server) ──▶ hit slugs "aid::ch"
                                     ├─▶ + live-workflow slugs (ACTIVE_STATES)
                                     ├─▶ enrich each ──▶ ChannelAction{eligible, score, state, active, cancellable,
                                     │                                  soft/hardCompleted, contentKey (variant)}
                                     ├─▶ ride completed[]/milestones[] (perpetual) + newCompleted[]/newMilestones[] (transient)
                                     └─▶ eligSig/fullSig change-detect ──▶ emit nba.evaluations (type=eligibility|score)
                                                                            + cache nba:evaluation:{nbaId}
                                                  (action-router publishes nba.completion.{aid}/nba.milestone.{id}
                                                   FROM new*[], then strips new*[] before persisting nba:eligibility:{nbaId})
```
