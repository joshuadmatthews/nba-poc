# 06 · Component Specs

Deep per-service reference. Each service is a single-purpose Java 21 fat-jar (Gradle Shadow, `eclipse-temurin:21-jre`) unless noted. Common deps: `kafka-clients:3.7.1`, `jedis:5.1.5`, `jackson-databind:2.17.2`.

Common env defaults: `NBA_BOOTSTRAP=nba-redpanda:9092`, `NBA_REDIS_HOST=nba-redis`, `NBA_REDIS_PORT=6379`, `NBA_FAULT_INJECT=""` (test hook: any record whose value contains the substring throws → DLQ).

---

## Snapshot Builder

`nba/services/snapshot-builder/.../SnapshotBuilder.java` · container `ais-nba-snapshot-builder` · boot wave 16

**Purpose.** Maintain the last-write-wins snapshot of every member's facts, re-emitting on every change. A batched, transactional, exactly-once read-process-write loop.

**I/O.** In: `nba.member.facts` (group `snapshot-builder`, `read_committed`). Out: `nba.snapshots` (re-emitted snapshots), `nba.facts` (firehose of internal `kind`-tagged facts), `nba.definitions` (forwarded throttle/suppress), `nba.dlq.snapshot-builder`.

**Classification** (per record): `nba.throttle.*` → forward to `nba.definitions` as `THROTTLE:*`; `nba.actionsuppress.*` → forward as `ACTION_SUPPRESS:*`; `kind` header present → firehose to `nba.facts` (and `kind=throttle-suppress` → also `THROTTLE_HOT:{channel}`; `kind=router` → **skip**); else → lean filter.

**Lean filter** (`toSnapFact`). Always-attach: `nba.score.*`, `nba.actionstate.*`, `nba.disposition.*`, `nba.completion.*`. Otherwise keep only if key ∈ `nba:rulefacts`. If `nba:rulefacts` is empty (no rules loaded) → snapshot ALL (fail-open for dev). `nba.score.*` value is flattened to its `.score` double, `valueType=DOUBLE`.

**nbaId.** Redis `nba:idmap:{entityType}:{entityId}`; `SETNX` mint of `nba_`+12hex; first-writer-wins.

**LWW.** Within-batch: highest `eventTs` per `(nbaId, key)`. Cross-batch: pipelined `HGET` of stored `eventTs`; drop any batch fact with `eventTs <= stored`.

**Transaction.** Phase 1: all Redis writes in one `MULTI/EXEC`. Phase 2: one Kafka transaction = forwards + DLQ envelopes + re-emitted snapshots for every touched member + `sendOffsetsToTransaction`. Invariant: snapshots re-emit for every touched member even if the LWW phase wrote nothing (retry-idempotent). On failure: `abortTransaction` + `seek` back + exponential backoff (1s→30s).

**Config.** `NBA_TOPIC_IN=nba.member.facts`, `NBA_TOPIC_OUT=nba.snapshots`, `NBA_FACTS_TOPIC=nba.facts`, `NBA_DEFINITIONS_TOPIC=nba.definitions`, `NBA_GROUP=snapshot-builder`, `NBA_TXN_ID=snapshot-builder-{HOSTNAME}`. Producer: `acks=all`, `enable.idempotence=true`, transactional.

**Background.** `nba:rulefacts` refreshed every 10s; on shrink, `pruneFacts` SCANs all `nba:snapshot:*`, `HDEL`s de-referenced fields, re-emits via a separate non-transactional producer.

**Failure.** Kafka txn fail → abort+seek+retry. Redis fail → no Kafka txn started yet, offset not committed, batch retried. Unparseable JSON → DLQ, batch continues.

---

## Rules Engine

`nba/services/rules-engine/.../RulesEngine.java` · container `ais-nba-rules-engine` · boot wave 17 · Drools 8.44.0.Final

**Purpose.** Compile authored rules into Drools at runtime; evaluate each snapshot into a unified `channelActions[]` evaluation. **There are no `.drl` files** — all DRL is generated from JSON condition trees. Full deep-dive (DRL synthesis, definitions routing, eval enrichment): [rules-engine.md](rules-engine.md).

**Threads.** (A) `defs-consumer`: tails `nba.definitions`, maintains `actions`/`globalRules`/`channelRules`/`milestones`/`GLOBAL_THROTTLE`/`CHANNEL_HOT_UNTIL`, rebuilds the `KieBase` (volatile) on structural change. (B) main: consumes `nba.snapshots` (auto-commit, at-least-once — idempotent via fullsig dedup), evaluates, emits `nba.evaluations`.

**DRL generation.** Per action-channel: `rule "elig::{action}::{channel}" when Snap({inclusion} && {global} && {channel}) [not Snap({exclusion})] then results.add("{action}::{channel}")`. Comparators map to MVEL with type-defaulted `getOr` (absent number→0, boolean→false, string→""), so missing facts never wrongly suppress new members. `.rate` channel rules are excluded from eligibility DRL (they are Temporal-gate pacing, not eligibility).

**Evaluation steps.** (1) build typed fact map from snapshot; (2) inject `GLOBAL_THROTTLE` levels as facts; (3) milestone latch (`HSETNX nba:milestones`); (4) hard-completion latch (`HSETNX nba:completed` when `completion` tree passes or `nba.completion.{action}` truthy); (5) fire Drools (embedded `KieSession` or HTTP to KIE server); (6) candidate set = Drools hits ∪ in-flight ACTIVE_STATES; (7) assemble each ChannelAction with flags + content-variant selection.

**Completion/milestone TRANSITIONS.** The engine no longer publishes completion/milestone facts. Instead each eval carries two transient TRANSITION arrays the engine computes once off the snapshot: `newCompleted[]` (action whose completion criterion just became true — `byCriterion && !already-signalled`) and `newMilestones[]` (milestone tree that just passed — `treePass && fact-absent`). The **action-router** publishes the durable `nba.completion.{action}` / `nba.milestone.{id}` facts off these arrays (see Action Router). The perpetual `completed[]`/`milestones[]` on the eval are unchanged.

**ACTIVE_STATES** = `{CREATED, IN_PROCESS, SUPPRESSING, PRESENTED, SOFT_COMPLETED, DECLINED}`. **Soft completion** (rule-based, per eval): disposition funnel index ≥ channel soft bar (`CHANNEL_FUNNEL` terminal by default, overridable per action-channel). **Content variants**: deterministic A/B via `floorMod(hash(member:channel:idx), 100) < percent` + optional condition tree; first match wins.

**Dedup.** `nba:eval:eligsig` (eligible identity) + `nba:eval:fullsig` (everything incl. scores/states). Skip emit if fullsig unchanged. Header `type=eligibility` (eligible set moved) or `type=score` (only scores changed). Change detection reads the single eligibility object at `nba:eligibility:{nbaId}` (written by the action-router off the eval; serves the inbound pull).

**Suppression is NOT enforced here** (a snapshot-driven engine never re-fires on a bare suppress). Enforced at read edges (router + inbound serve).

**Config.** `NBA_RULES_MODE=embedded|kie`, `NBA_KIE_URL=http://nba-kie-server:7010`, `NBA_THROTTLE_HOT_TTL_SECONDS=0` (0 = saturation until midnight UTC).

---

## KIE Server

`nba/services/kie-server/.../KieServer.java` · container `ais-nba-kie-server:7010` · boot wave 17 (optional)

Standalone Drools decision service. Tails `nba.definitions` (own random group) to build an identical `KieBase`. `POST /evaluate {nbaId, facts}` → `{nbaId, hits:["action::channel"]}`. `GET /health` → `warming`/`ok`. Activated by `NBA_RULES_MODE=kie`; scales to N replicas behind the `nba-kie-server` DNS alias. The `buildDrl` logic is identical to the rules engine.

---

## ML Scorer — RETIRED (replaced by the Databricks CQL scoring stream)

> **The Java `ml-scorer` (heuristic stub) is removed** — service dir deleted, container `ais-nba-ml-scorer` retired.
> It was a placeholder propensity heuristic standing in for a real model. The **live scorer is now the Databricks
> CQL stream** (`nba_ml_score_rl`, job `nba-ml-score-rl`): it consumes `nba.evaluations`, scores each eligible
> ChannelAction by its **channel's** offline-RL Q-value (the disposition-aware CQL policy), and emits
> `nba.score.{action}.{channel}` to `nba.member.facts` (`kind=score`, **drops `type=score` by header** — the same
> loop-break). See [11-ml-pipeline.md](11-ml-pipeline.md). The `nba.dlq.ml-scorer-*` topics are dead with the service.

(Historical: the stub model was `0.5 − min(0.30, daysSinceLogin×0.01) + … + channelPrior`, clamped `[0.01, 0.99]` — a
transparent heuristic, never a learned model. Output keying/replay-safety semantics are unchanged in the CQL scorer.)

**Failure / cold start.** Scorer spins on `featuresReady` (never scores against an empty store). A bad eval → DLQ, batch still commits. If scores aren't ready when an eval arrives, the eval's `score` fields are absent and the router does nothing until the score fact round-trips.

---

## Action Router

`nba/services/action-router/.../ActionRouter.java` · container `ais-nba-action-router` · boot wave 19

**Purpose.** Pick the winning action per channel; suppress losers; bridge soft/hard completion. Reads only the eval's coarse flags (never Temporal/Redis except one `readMaxBatch`).

**Decision order** (`activate`): (1) for every non-suppressed ChannelAction: `hardCompleted` → `HARD_COMPLETE`, else `softCompleted` → `SOFT_COMPLETE`; (2) ineligible + `cancellable` → `SUPPRESS`; (3) find slot occupant `cur` = top-scored eligible active; (4) find candidate `cand` = top-scored eligible free (ties → smallest slug); (5) if `cur` exists: supersede only if `candScore > curScore && cur.cancellable` (else hold); (6) slot free → `CREATE` (or `CREATE_BATCH` of top-N if `nba:channel:maxbatch[channel] > 1`).

**Completion/milestone emission.** The router (not the rules engine) publishes the durable `nba.completion.{actionId}` (`kind=completion`, `BOOL` `true`) and `nba.milestone.{id}` (`kind=milestone`, `LONG` `completedAt`) facts. They ride the eval's TRANSITION lists — the rules engine computed them once off the snapshot and flags exactly the just-passed ones on `newCompleted[]` (completion criterion now true, not yet signalled) / `newMilestones[]` (milestone tree just passed, fact absent). The router emits straight off those arrays — no diffing, no eligibility-cache read (the diff already rode the message). Facts are LWW-idempotent, so an at-least-once re-publish during the round-trip is a snapshot no-op. Before persisting the single eligibility object (`nba:eligibility:{nbaId}`) the router STRIPS the transient `newCompleted`/`newMilestones`; the perpetual `completed[]`/`milestones[]` stay on the object.

**Suppression cache.** Daemon thread tails `nba.definitions` (`ACTION_SUPPRESS:*`) into an in-memory `SUPPRESSED` set (hydrated from `nba:suppressed` on start). Whole-action or `actionId.channel` scope. Hot-path is a pure set lookup.

**Output.** All ops on `nba.member.facts`, `kind=router`. Single key `nbaId:actionId:channel`; batch key `nbaId:channel:batch`. `memberId=entityId` rides every op. DLQ `nba.dlq.action-router`.

---

## State Machine (Temporal)

`nba/services/nba-temporal/...` · container `ais-nba-temporal-worker` · boot wave 20 · server `ais-nba-temporal:7233`

Full spec in [03-state-machine.md](03-state-machine.md). Summary: one `ChannelActionWorkflow` per `nba-ca:{nbaId}:{actionId}:{channel}`; 11 states; debounce sibling-dedup (tristate LOSE/WAIT/PROCEED); in-process `ThrottleGate` (SEND/WAIT/SUPPRESS); emits via Postgres **outbox** (`outbox_member_facts`→`nba.member.facts`, `outbox_activations`→`nba.activations`) tailed by Debezium. Three Kafka threads: bridge (`kind=router`), disposition consumer (`kind=disposition|completion`, routes by `trackingId`), throttle-feed (`nba.definitions`, broadcast). Workflow reuse policy `ALLOW_DUPLICATE` + conflict `USE_EXISTING` (repeated CREATE attaches to the running workflow). DLQs `nba.dlq.temporal-disposition`, `nba.dlq.temporal-bridge`.

**Per-channel touch-template escalation** (`ActionActivitiesImpl`). A channel in the catalog `channels[]` can carry `touchKeys = [firstTouch, secondTouch, thirdTouch]` template ids. A MONOTONIC per-`(member, channel)` counter — table `channel_touch (nba_id text, channel text, n bigint, PRIMARY KEY (nba_id, channel))` in the actionlib Postgres, created `IF NOT EXISTS` by the worker on startup — is bumped at the SINGLE dispatch send point (`emitActivation` single-action and `emitBatchDispatch` batch, on a real `DISPATCH` only), AFTER debounce/suppress/throttle have settled, so debounced/suppressed/throttled attempts NEVER escalate. The bump is an atomic `INSERT … ON CONFLICT (nba_id, channel) DO UPDATE SET n = channel_touch.n + 1 RETURNING n` (race-free for concurrent batch sends); it NEVER resets. The send overrides the variant-selected `contentKey` with `touchKeys[min(n, len) - 1]` (caps at the last configured); absent `touchKeys` → the `contentKey` is used unchanged. The count is per-CHANNEL regardless of action — any prior send on a touch-configured channel (of ANY action) counts toward the next action's touch number, which is exactly why the counter lives in the send activity, not a per-`(member, action, channel)` workflow. `emitActivation` also now carries `entityId`/`nbaId` on every single-action DISPATCH (not just batches), so single sends land attributed in `silver_activations` for journey reconstruction / RL training.

---

## Activation Layer

`nba/services/action-layer/.../ActionLayer.java` · container `ais-nba-action-layer` · boot wave 21

**Purpose.** The single send point and the "disposition brain" — the only component that knows what a provider's raw status means.

**I/O.** In: `nba.activations` (group `action-layer`, offset `latest` — live activations only). Out: `nba.member.facts` `kind=disposition`. DLQ `nba.dlq.action-layer`.

**Behavior.** On `DISPATCH`: create a `Walk` keyed `memberId:actionId:channel` (or `memberId:batch:channel` — the activation carries no `nbaId`, only `memberId`, so concurrent sends of the same arm to different members don't collide); a background walker advances it through `CHANNEL_FUNNEL`, emitting one canonical disposition per step (raw status → `value`, canonical state → `state`). On `CANCEL`: `SUPPRESSED` if the walk hadn't dished its first disposition yet (`step<0`), else `SUPPRESS_FAILED`. Batch DISPATCH fans out one disposition per action.

**Canonical states emitted.** `PRESENTED` (all happy-path delivery), `DECLINED`, `FAILED` (bounce/compliance/undelivered/no-answer), `SUPPRESSED`, `SUPPRESS_FAILED`. **Never** `SOFT_COMPLETED`/`HARD_COMPLETED` (rules engine owns those).

**Production note.** The simulator walker is replaced by real provider webhooks calling the same `emitDisposition(...)` — `CHANNEL_FUNNEL` stays the classifier. Config: `NBA_DISPOSITION_STEP_MS=1500` (delay between funnel steps), `NBA_SIM_FAIL_RATE` / `NBA_SIM_DECLINE_RATE` (default `0.0`, live-shiftable via Redis). The CANCEL race window is the brief interval before the walker dishes the first disposition (`Walk.step<0`) — there is no separate send-delay knob. Sim mode (`deterministic`/`stochastic`) and the conversion model are driven from Redis (`nba:sim:*`), not env.

---

## Action Library

`nba/services/action-library/.../ActionLibrary.java` · container `ais-nba-action-library:7001` · Javalin · boot wave 16

**Purpose.** Authoring + inbound pull-serve boundary + the synchronous HOT PATH. Stores definitions in Postgres; publishes via the transactional outbox; serves a member's actions for a requesting channel; and — when live context is presented — runs a self-contained decision (snapshot + gold features → eligibility → score) instead of reading the cached eval. Design framing: **the API is JUST the hot path** — an optimistic accelerator over Kafka + the snapshot-builder, which remain the source of truth.

**Consistency.** Writes nothing to Kafka directly — every emission is an INSERT into `outbox_defs` in the same Postgres transaction as the business write; Debezium routes by `aggregatetype` → topic, `aggregateid` → key, `kind` → header, `payload` → value (`null` = tombstone).

**Postgres.** Tables `action`, `global_rule`, `channel_rule`, `milestone` (JSONB docs), `outbox_defs`, `action_group` (adjacency tree), `experience`. `factsUsed` is auto-derived (recursive `collectFacts` over inclusion/exclusion/logic/variants/completion).

**Key endpoints.**

| Method · Path | Purpose |
|---|---|
| `GET /health` | liveness |
| `POST/PUT/GET/DELETE /actions[/{id}]` | author actions; `/actions/{id}/group`, `/actions/{id}/experience` |
| `POST/PUT/GET/DELETE /global-rules`, `/channel-rules`, `/milestones` | author rules/milestones |
| `GET /next-action/{entityId}?channel=&n=&includeCompleted=` | **inbound pull serve.** Returns ALL of the channel's actions (default `n=ALL`, was top-1), each with a `state` ∈ `eligible`/`active`/`completed`. No `{facts}` body → serves from the cached eligibility object (`nba:eligibility:{nbaId}`: eligible, not suppressed, by score). A `{facts}` body → runs the synchronous **HOT PATH** (`hotPathDecide`: merge snapshot + presented facts → eligibility → score) so the served NBAs reflect the just-given inbound topic. `includeCompleted=true` also surfaces the `completed[]` actions (which otherwise prune out of channelActions). Stamps one `correlationId` on the served set + emits an `INBOUND_SERVE` tracking event. |
| `POST /disposition` | **THE FAST PATH** (singular). `{entityId, actionId, channel, status?, facts?, mode?=kie\|inproc, n?, writeback?, correlationId?}` → `hotPathDecide` (merges the inbound disposition first) → ranked NBAs synchronously; durable write-back via the outbox AFTER responding (off the latency path). Links the prior serve's `correlationId` + emits `INBOUND_DISPOSITION`, then stamps a NEW `correlationId` on the next-served set + emits its `INBOUND_SERVE`. |
| `POST /dispositions` | inbound disposition → outbox `nba.member.facts` `kind=disposition` (the durable, async, no-decision variant). |
| `POST /completion` | hard-completion signal → outbox `kind=completion` (channel-agnostic): the durable `nba.completion.{actionId}` fact — the SAME fact the router emits on outbound completion. |
| `POST /suppress` · `GET /suppressed` | operator suppression → `ACTION_SUPPRESS:{target}` |
| `POST/GET /channel-config` | per-channel `maxBatch` (Redis `nba:channel:maxbatch`) |
| `GET/POST/DELETE /groups`, `/experiences` | taxonomy |

**Definitions cache.** A daemon (own random group, replays compacted `nba.definitions`) keeps `SUPPRESSED` + `nba:suppressed` current and triggers `recomputeRuleFacts` (union of all `factsUsed` from Postgres → `nba:rulefacts`). The topic events only *trigger* the recompute; Postgres is the source of truth.

**Hot-path decision core** (`hotPathDecide`). Shared by the `POST /disposition` fast path and the `GET /next-action` hot path (facts given). Steps: (1) read `nba:snapshot:{nbaId}` (loop state — dispositions/completions/milestones); (2) read the ~30 rich model features STRAIGHT from gold (`goldFeatures`, below); (3) LWW-merge features + snapshot facts + the inbound disposition + presented facts (event-time) into a structured node (for the model) and a flat map (for KIE); (4) eligibility (`mode=kie` via KIE server, or `inproc`), then strip operator-suppressed; (5) score via the local `nba-model` (`scorer=local`, default) or the Databricks Model Serving endpoint (`scorer=dbx`); (6) rank top-`n`. Returns `{nbaId, channel, mode, scorer, eligibleCount, featureSource, nbas[], timings{}}` — `timings` carries per-stage ms (snapshot/features/merge/elig/score/total). `channel` null/blank scores ALL eligible channels.

**Optimistic write-through.** Only on a REAL hot path (presented facts OR an inbound disposition) does `hotPathDecide` warm Redis so the very next read — even a no-facts serve — is fresh: it LWW-merges the presented facts into `nba:snapshot:{nbaId}` (`fact:` fields, eventTs-stamped) and read-modify-writes the `nba:eligibility:{nbaId}` channelAction SCORES (back-to-back, plain Jedis — no Lua, no MULTI); and emits each presented fact to `nba.member.facts` (member-fact shape, `source=hotpath`) as the DURABLE path. The snapshot-builder re-applies those facts (event-time LWW) as the SELF-HEAL — if the optimistic write loses a race or fails, the bus reconciles it; the whole block is best-effort (try/catch), never failing the decision. The single-writer model HOLDS: the snapshot's authoritative writer stays the snapshot-builder, eligibility's stays the action-router; event-time LWW makes the writers commutative. A no-facts serve reads the cached eligibility (bounded staleness, refreshed each flywheel cycle — no write-through).

**Inbound tracking — the serve→disposition journey.** `INBOUND_SERVE` / `INBOUND_DISPOSITION` are DIRECT-to-Kafka tracking events on `nba.activations` (`source=inbound`) — fire-and-forget, NO outbox / no distributed transaction (distinct from the durable `kind=disposition` write-back, which still goes via the outbox). The serve stamps a `correlationId`; `POST /disposition` emits `INBOUND_DISPOSITION` linked to it, then stamps a NEW `correlationId` on the next-served set + emits its `INBOUND_SERVE`. They land in `silver_activations` (a `correlationId` column), so the serve→disposition journey is linkable in the lake + on the Command Center member timeline.

**Feature store — read straight from gold.** The ~30 rich model features (riskScore, comorbidityCount, rxAdherencePDC, openCareGaps, the activity/clinical/profile block) live in gold (`{NBA_LAKE_NS}.gold_member_snapshot`). `goldFeatures(entityId)` reads them STRAIGHT from gold via the serverless SQL warehouse — NO Redis cache (`featureSource="gold"`, ~1s warm; the hot path wears the latency). The whole `nba:features` cache machinery is GONE — `warmFeatures`, the `/warm-features` prefetch endpoint + route, and `FEATURE_TTL` are removed. The Redis caches are now exactly THREE: **snapshot**, **eligibility**, **action→fact** (catalog/rules; `nba:rulefacts` + `nba:suppressed`).

**Lakebase (BLOCKED, dormant).** The IDEAL online feature store is Lakebase (managed Postgres, instance `nba-lakebase`, catalog `nba_pg`) fed by a CONTINUOUS synced table off gold (`gold_member_snapshot` has `delta.enableChangeDataFeed=true`, ready); the hot path would then point-read by `nbaId` (the synced table's PK) at ~ms via `lakebaseFeatures` — present in the code but DORMANT. BLOCKER: the synced-table API creates the resource and accepts `scheduling_policy=CONTINUOUS`, but its backing DLT pipeline FAILS in a retry loop with `UNITY_CATALOG_INITIALIZATION_FAILED` / "Metastore storage root URL does not exist" — this POC account's UC metastore has no storage root. FIX: an account admin sets the UC metastore storage root (an S3 bucket); then re-create the synced table + flip the hot path from `goldFeatures` back to `lakebaseFeatures`. The manual `load-lakebase.py` mirror (lowercase columns) remains the Command Center BFF's source — a TRIGGERED/manual mirror, not continuous.

**Config.** `run.ps1` wires `NBA_DBX_WAREHOUSE` (the serverless SQL warehouse `<warehouse-id>`) + `NBA_LAKE_NS` (default `workspace.nba_poc`) for the gold feature read, plus `NBA_SERVING_URL` for the `scorer=dbx` hot-path scorer.

---

## Inbound Member Simulator

`nba/services/nba-inbound-sim/inbound_sim.py` · container `nba-inbound-sim` · image `localhost/ais-nba-inbound-sim` · network `aiservices_default` · **Python** (not a Java jar)

**Purpose.** Model real inbound members as what they are — external CLIENTS — driving the action-library's REAL inbound APIs end-to-end, instead of the OLD Databricks shortcut that wrote a completion fact straight onto the bus. It is the local counterpart to the Databricks source-sim (which now models OUTBOUND response only). Warm members proactively come inbound and complete an action through serve → disposition → completion, so the lake carries the "soft-complete → inbound completion" transition and the CQL learns to surface that action when the member contacts us inbound.

**I/O.** In: scans `nba:snapshot:*` from `nba-redis` for warm members — those with a live `fact:nba.actionstate.{aid}.{ch} == SOFT_COMPLETED` not already hard-completed (`fact:nba.completion.{aid}` absent). Out: HTTP to `nba-action-library:7001` — `GET /next-action/{entity}?channel=&n=5` → `POST /disposition` (deepest funnel `status`, linked by `correlationId`) → `POST /completion {source:inbound}` (only on a hard-complete visit). No Kafka — completions reach the bus only through the action-library's outbox, the same proven path every inbound disposition takes.

**Flow per loop** (`LOOP_SECONDS≈45`). SCAN up to `SCAN_LIMIT≈8000` snapshots; collect warm `(entity, aid, ch)` (each kept with prob `INBOUND_RATE≈0.25`) and a COLD baseline of non-warm members (kept with prob `COLD_RATE`); cap to `INBOUND_CAP`/`COLD_CAP`; drive each in a 12-worker thread pool. Per visit: serve (some carry a "call topic" → hot path), act on the served list with a disposition (the soft engagement, hot-pathed + linked to the serve), and on a `HARD_FRACTION` share post the completion (the goal). Cold members have no warm signal and act on the model's #1 served recommendation.

**Behavior knobs (env).** `HARD_FRACTION≈0.4` — share of inbound visits that finish the goal (the rest soft-engage and may finish on a later visit). `COLD_RATE≈0.015` — a baseline of non-warm members who spontaneously show up, so warmth is a LIFT on inbound completion, not a gate. `TOPIC_RATE≈0.3` — share of visits that carry a "call topic" (`INBOUND_CTX`, e.g. `operator.activity.daysSinceLogin=0` = contacting us right now) → the serve HOT-PATHS (reads gold live); the rest serve from the cached eligibility, which bounds the gold reads. `NBA_SCORER=local` — in-network `nba-model` for the sim's disposition/serve scoring, so it stays robust under load (the inbound RANKING signal already comes from the serve's cached `@champion` scores); set `dbx` to stress-test the live hot path. Also `INBOUND_RATE`, `LOOP_SECONDS`, `INBOUND_CAP`, `SCAN_LIMIT`, `COLD_CAP`, `NBA_API_BASE`, `NBA_REDIS_HOST/PORT`.

---

## Redis data layout (shared `ais-nba-redis`)

256 MB, `noeviction`, AOF.

| Key | Type | Writer | Reader | Contents |
|-----|------|--------|--------|----------|
| `nba:idmap:{type}:{id}` | string | snapshot-builder, ml-scorer | action-library | nbaId (SETNX, permanent) |
| `nba:snapshot:{nbaId}` | hash | snapshot-builder (MULTI/EXEC, authoritative), action-library (hot-path write-through, best-effort) | snapshot-builder (LWW/prune/re-emit), action-library (hot path), action-layer (sim features) | `fact:{key}`→fv JSON + `__entityType/__entityId/__nbaId/__updatedTs`. (rules-engine reads snapshots off the `nba.snapshots` Kafka topic, not this key.) |
| `nba:eligibility:{nbaId}` | string | action-router (authoritative, off the eval), action-library (hot-path score write-through, best-effort) | action-library (cached serve + hot path), rules-engine (change detection) | the single eligibility object (`channelActions[]` + perpetual `completed[]`/`milestones[]`; transient `newCompleted`/`newMilestones` stripped before persist) |
| `nba:eval:eligsig/fullsig:{nbaId}` | string | rules-engine | rules-engine | change-dedup identities |
| `nba:milestones:{nbaId}` | hash | rules-engine (HSETNX) | rules-engine | permanent milestone latches |
| `nba:completed:{nbaId}` | hash | rules-engine (HSETNX) | rules-engine | permanent hard-completion latches |
| `nba:rulefacts` | set | action-library | snapshot-builder | fact keys any rule references (lean filter) |
| `nba:suppressed` | set | action-library | router, action-library | suppressed action/channel targets |
| `nba:channel:maxbatch` | hash | action-library | action-router | per-channel batch size |

## Per-consumer DLQ topics

| DLQ | Source topic | Consumer |
|-----|--------------|----------|
| `nba.dlq.snapshot-builder` | `nba.member.facts` | snapshot-builder |
| `nba.dlq.ml-scorer-features` | `nba.facts` | ml-scorer feature store |
| `nba.dlq.ml-scorer-scorer` | `nba.evaluations` | ml-scorer |
| `nba.dlq.action-router` | `nba.evaluations` | action-router |
| `nba.dlq.action-layer` | `nba.activations` | action-layer |
| `nba.dlq.temporal-disposition` | `nba.member.facts` | temporal disposition consumer |
| `nba.dlq.temporal-bridge` | `nba.member.facts` | temporal bridge |

The rules-engine has no DLQ (at-least-once + fullsig dedup; a failed eval is logged and the snapshot re-flows). Replay/flush is driven from the Command Center DLQ tab; replay re-produces the exact original record to its source topic and is idempotent everywhere.
