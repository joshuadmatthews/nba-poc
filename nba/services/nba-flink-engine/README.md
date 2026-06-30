# nba-flink-engine — the whole NBA app as one Apache Flink job

This is the **third reference implementation** of the NBA spine, alongside:
1. **classic + Redis** — the five bespoke services (snapshot-builder, rules-engine, journey-scorer,
   action-router) + Temporal for the per-action lifecycle, Redis as the snapshot/eligibility store.
2. **nba-decision-engine** — a Kafka Streams port of the *stateful compute* (snapshot + eligibility), RocksDB
   state + Interactive Queries instead of the Redis snapshot.
3. **this** — the **entire app as a single Flink DataStream job**, replacing the five services **and Temporal**
   (the lifecycle becomes a `KeyedProcessFunction` with timers — Flink durable timers replace Temporal's).

Runs **embedded** (a local MiniCluster via `env.execute()`), so it deploys as one container like the other
services — no separate JobManager/TaskManager cluster.

## Layers (each a Flink operator; Kafka topics are the connective tissue, exactly as the live system)

| # | Stage | Operator | Replaces | In → Out |
|---|-------|----------|----------|----------|
| 1 | Snapshot | `ClassifyResolveFn` (ProcessFunction) → `SnapshotLwwFn` (KeyedProcessFunction) | snapshot-builder | `nba.member.facts` → classify (defs/firehose/dlq side-outputs) → event-time LWW keyed by nbaId → `nba.snapshots` |
| 2 | Rules / eligibility | `RulesFn` (KeyedBroadcastProcessFunction) | rules-engine (Drools) | `nba.snapshots` (keyed) + broadcast `nba.definitions` → `nba.evaluations` |
| 3 | Score | `ScoreStage.ScoreFn` (flatMap) | nba-journey-scorer | `nba.evaluations` → `nba.member.facts` (kind=score) |
| 4 | Route | `RouterFn` (RichFlatMap) | action-router | `nba.evaluations` → per-member slot/dedup/suppress + completion bridge → `nba.member.facts` (kind=router/completion/milestone) |
| 5 | **State machine** | `StateMachineFn` (KeyedProcessFunction + timers) | **Temporal `ChannelActionWorkflow`** | `nba.member.facts` (kind=router\|disposition) keyed by (member,action,channel) → `nba.actionstate.*` (kind=state) + `nba.activations` (DISPATCH/CANCEL) |
| 6 | Action layer | `ActionLayerFn` (KeyedProcessFunction + timer) | action-layer (delivery sim) | `nba.activations` → simulated delivery → `nba.member.facts` (kind=disposition) |

The forward path (snapshot→rules→score→route) and the loops (score→facts→snapshot;
router→statemachine→actionlayer→disposition→statemachine) all flow through the same Kafka topics the live
system uses, so this is a drop-in-shaped, feature-complete port **on the Kafka topics** — same topics, same
flow, one runtime. It is **not yet wired for the durable / Databricks path** (it writes Kafka directly, bypassing
the Postgres outbox → Debezium → lake) — see [Production / Databricks integration gaps](#production--databricks-integration-gaps).

## The state machine (Temporal replacement) in detail
`StateMachineFn` is one keyed instance per `(member,action,channel)`. Temporal's durable execution + signals +
timers map to Flink keyed state + the keyed event stream + processing-time timers:

```
CREATE → CREATED --[debounce timer]--> (suppressed? DEBOUNCED/SUPPRESSED) : DISPATCH → IN_PROCESS
  IN_PROCESS --[disposition signals]--> PRESENTED → (SOFT_COMPLETED) → HARD_COMPLETED | DECLINED | FAILED
  --[TTL timer, no hard completion]--> EXPIRED ; post-dispatch SUPPRESS → SUPPRESSING → SUPPRESSED | resume
```
- `Workflow.await(debounce)` → a processing-time timer; `trackDispositions` TTL window → a second timer.
- The `disposition`/`suppress`/`softComplete`/`hardComplete` signals arrive as one keyed `StateEvent` stream
  (router activations on kind=router, deliveries on kind=disposition), routed by the `nba-ca:{nbaId}:{aid}:{ch}`
  tracking id — see `StateEventMapper`.

## Modes (additive, like the KStreams engine)
- `NBA_FLINK_MODE=shadow` (default) — sinks write `.shadow` topics, no Redis write-through, drives nothing. The
  snapshot entry unions `nba.member.facts` (live external) with `nba.member.facts.shadow` (its own loop-back) so
  the shadow pipeline is self-contained and fed by live facts, with zero blast radius on the classic system.
- `NBA_FLINK_MODE=authoritative` — writes the real topics + the Redis mirrors (`nba:snapshot`, `nba:eligibility`)
  so the synchronous hot path (action-library) and downstream stay byte-compatible; the Flink job becomes the writer.

## Faithful where it counts; simplifications (noted, not hidden)
- **Eligibility is evaluated in Java** (`RulesLogic.treePass`/`condPass`) — the *same condition-tree semantics*
  the classic rules-engine uses for milestones/completion/variants. The classic engine compiles those same trees
  to MVEL/Drools (`inclusion && global && channel && !exclusion`); evaluating them in Java yields the identical
  hit list without embedding Drools in the operator. (Drools deps are present for parity if a future variant
  wants the KieBase path.)
- **Reads** stay on Redis (write-through in authoritative) — the load test (../infra/loadtest-results.md) showed
  Redis reads are ~10× faster than Flink/RocksDB IQ and stay up independently, so the hot path keeps reading Redis.
- **Sibling debounce-dedup** (Temporal `debounceLost`) is omitted — the router's per-member single-active-slot
  upstream already prevents the sibling race. A full port would add a member-keyed coordinator.
- **Throttle gate** is SEND (saturated channels are already gated ineligible by the rules engine's THROTTLE_HOT);
  the trickle/backlog bucket is a follow-up.

## Production / Databricks integration — what's actually needed
This engine reaches **output parity on the Kafka topics** (snapshots, evaluations, scores, router/completion/
milestone facts, the 11 state facts, activations, dispositions). Importantly, the **Databricks lake and the RL
scorer read the Kafka topics directly** (`nba_datalake_stream.py` subscribes to `nba.evaluations`/`nba.snapshots`/
`nba.activations`; `nba_ml_score_rl.py` subscribes to `nba.evaluations`) — they do **not** read the Postgres
outbox. So in **authoritative** mode the lake and scorer **do see Flink's facts**; the outbox→Debezium is the
*classic path's transactional emit* mechanism, not a lake dependency. The genuine items before Flink is a true
prod drop-in:
- **One writer at a time.** Don't run Temporal **and** authoritative-Flink together — both write the same real
  topics (dual-write). Pick one as the authoritative writer.
- **Durability.** The classic path's outbox transactionally couples the business-state write + the Kafka emit.
  Flink gets the equivalent from its **checkpointed exactly-once Kafka sink** (EXACTLY_ONCE_V2) — so direct writes
  are durable; the outbox isn't required. (If you specifically want the outbox audit row in Postgres, that'd be
  an add to `StateMachineFn`/`ActionLayerFn`.)
- **Scoring — gate the local stage off in prod.** Flink's score stage ports the local deterministic
  `nba-journey-scorer`. The prod RL scorer (`nba_ml_score_rl`) already subscribes to `nba.evaluations` and emits
  `nba.score.*`, so for prod you **disable Flink's internal `ScoreStage`** and let the Databricks RL scorer score
  Flink's evaluations (avoids double-scoring). Config, not a rewrite.
- **`channel_touch` counter (the one real code gap).** Temporal atomically UPSERTs the per-(member,channel) send
  counter in Postgres to pick the escalating touch template (`touchKeys[min(n,len-1)]`); `ActionLayerFn` has no
  Postgres connection, so today every send uses the base `contentKey`. Add a Postgres UPSERT in `ActionLayerFn`
  (or keep the touch counter in Flink keyed state) to close it.

## Build / run
```
podman build -t localhost/nba-flink-engine:latest -f Containerfile .   # gradle test shadowJar gates the image
pwsh run.ps1                                                           # deploy (shadow by default)
```
Tests: `SnapshotLogicTest` (classify + LWW), `RulesLogicTest` (condPass/treePass/eligibleHits/evaluate +
change-detect), `ScoreFnTest` (scoring), `StateMachineFnTest` (the lifecycle via the Flink test harness:
debounce→dispatch→hard-complete, TTL→expire, pre-send suppress→debounced).
