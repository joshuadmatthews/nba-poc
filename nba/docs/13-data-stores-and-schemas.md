# 13 · Data Stores & Schemas

Every place the NBA system keeps state, in one reference: what it is, whether it's **authoritative** (source of
truth) or **derived** (a cache/projection you can rebuild), its key/schema, retention, and who writes vs reads
it. Message-shape detail lives in [04-message-schemas.md](04-message-schemas.md); this doc is the store-by-store
map.

## At a glance

| Store | Tech | Holds | Authoritative? | Rebuildable from |
|---|---|---|---|---|
| **Kafka topics** | Redpanda | the event log + current state per key (compacted) | mixed (see below) | — / upstream |
| **Redis** | Redis | hot-path snapshot + eligibility cache, id-map, lean-filter set, sim params | derived (except id-map) | Kafka topics / Postgres |
| **Postgres** (`actionlib`) | Postgres | authored action/rule/milestone defs + the 3 transactional outboxes | **authoritative** (defs) | — |
| **Engine state** | RocksDB (KStreams) / Flink keyed state | the engines' own snapshot + eligibility state | derived | changelog / checkpoint |
| **Temporal** | Temporal server | one ChannelAction workflow's lifecycle state | derived | replay from facts |
| **Databricks medallion** | Delta (UC) | bronze→silver→gold history + the gold feature store | derived (audit/features) | Kafka + source replays |
| **`rl_qnet.json`** | JSON artifact | the trained CQL Q-net weights | **authoritative** (the champion) | retraining |

Authoritative vs derived matters operationally: a derived store can be wiped and rebuilt; an authoritative one
needs a backup. The only authoritative stores are **Postgres definitions**, the **Redis id-map** (the minted
`nbaId` ↔ entity binding), and the **model artifact**.

---

## 1. Kafka / Redpanda topics

All core topics are **`cleanup.policy=compact`**, 1 partition / 1 replica in the POC (keys are chosen so adding
partitions is the only change to scale out — see [10-scaling-throughput.md](10-scaling-throughput.md)).
Created by [`infra/create-topics.ps1`](../infra/create-topics.ps1).

| Topic | Key | Authoritative? | Value | Writers → Readers |
|---|---|---|---|---|
| **`nba.member.facts`** | `entityType:entityId` | authoritative (member attrs) | a **Fact envelope** (below); multiplexes 6 internal kinds via a `kind` header | lake / action-library (hotpath) / scorer / temporal / action-layer / action-router → snapshot-builder, the engines, temporal bridge |
| **`nba.snapshots`** | `nbaId` | derived | the **Snapshot** object (below) | snapshot-builder (or an engine, authoritative) → rules-engine |
| **`nba.evaluations`** | `nbaId` | derived | an **Evaluation** (`channelActions[]` + `milestones[]`) | rules-engine → action-router, scorer, decision-engine |
| **`nba.activations`** | `nbaId:actionId:channel:sm` | derived | an **Activation** (`op` = DISPATCH/CANCEL/INBOUND_*) | temporal (via outbox) / action-library (inbound) → action-layer |
| **`nba.definitions`** | `TYPE:id` | authoritative-projection | per-`TYPE` def JSON (ACTION / GLOBAL_RULE / CHANNEL_RULE / MILESTONE / THROTTLE / THROTTLE_HOT / ACTION_SUPPRESS) | action-library (via outbox) / temporal (THROTTLE_HOT) → rules-engine, kie-server, action-router |
| **`nba.dlq.{consumer}`** | original key | n/a (`delete`, 7d) | a **DLQ envelope** `{consumer,topic,partition,offset,key,value,headers,error,dlqTs}` | each consumer on poison → Command Center (browse/replay) |

**Fact envelope** (the unit on `nba.member.facts` / `nba.facts`):
```json
{ "entityType":"OPERATOR", "entityId":"op-sg-0", "nbaId":"nba_a1b2c3d4e5f6"(opt),
  "key":"operator.activity.daysSinceLogin", "value":20, "valueType":"LONG|DOUBLE|BOOLEAN|STRING|OBJECT",
  "eventTs":1749000001234, "source":"lake:crm-export|ml|temporal|action-layer|action-router|hotpath" }
```
`eventTs` is the **last-writer-wins ordering key** everywhere downstream.

**Snapshot** (on `nba.snapshots`, and mirrored to Redis): `{nbaId, entityType, entityId, correlationId,
updatedTs, facts:{ "<key>": {value, valueType, eventTs, source}, … }}`.

**`kind` header on `nba.member.facts`** routes the 6 internal fact families: `score` (scorer), `state`
(temporal — the 11 canonical states), `disposition` (action-layer), `completion`/`milestone` (action-router
publishes the durable latch facts), `router` (router decisions → temporal bridge; not snapshotted).

**`.shadow` siblings** — when a reference engine runs in shadow mode it writes `nba.member.facts.shadow`,
`nba.snapshots.shadow`, `nba.evaluations.shadow`, `nba.activations.shadow`, `nba.facts.shadow`, and its own
`nba.dlq.{engine}.shadow` — zero blast radius next to the live topics (used for the load-test study).

**Retired:** `nba.facts` (the all-facts ML firehose — ML reads features from gold now; not created) and its
`nba.dlq.ml-scorer-*` DLQs.

---

## 2. Redis keyspace (runtime)

The hot store. Only the **id-map** is authoritative; everything else is a cache/projection rebuildable from
Kafka or Postgres. (The legacy `nba:action:*` per-def mirror was removed — it had no readers.)

| Key | Type | Authoritative? | Schema | Writer → Reader |
|---|---|---|---|---|
| `nba:snapshot:{nbaId}` | hash | derived | `__entityType/__entityId/__nbaId/__updatedTs` + `fact:{key}` → fv JSON | snapshot-builder (+ flink write-through) → action-library `/snapshot`, action-layer, inbound-sim |
| `nba:idmap:{entityType}:{entityId}` | string | **authoritative** | `→ nbaId` (race-safe `SETNX` first-write-wins) | snapshot-builder / engines → action-library |
| `nba:eligibility:{nbaId}` | string | derived | the latest Evaluation JSON (minus the transient `newCompleted`) | action-router (+ flink) → action-library `/disposition`, rules-engine (change-detect) |
| `nba:rulefacts` | set | derived (from defs) | the union of every fact key any rule references — the **lean-filter** | action-library → snapshot-builder, both engines |
| `nba:suppressed` | set | derived (from defs) | operator-suppressed `{actionId}` / `{actionId}:{channel}` targets | action-library → action-router |
| `nba:channel:maxbatch` | hash | user config | `{channel → maxBatch}` | action-library (`/channel-config`) → action-router |
| `nba:facttype` | hash | derived (from defs) | `{factKey → valueType}` (rule-builder UI validation) | action-library → action-library |
| `nba:sim:*` | strings | sim config | `fstar / effect / fail_rate / decline_rate / mode / propensity / channel_affinity` (the local conversion/scoring sim params) | seed → conversion-sim, journey-scorer |

> Note: the snapshot/eligibility Redis copies are a **read cache** for the synchronous hot path; the durable
> truth is the compacted Kafka topic (and, in the engine variants, the engine's own state store — see §4). This
> is the read-surface tradeoff measured in [PERFORMANCE.md](../PERFORMANCE.md).

---

## 3. Postgres (`actionlib`)

`jdbc:postgresql://nba-postgres:5432/actionlib`. **The authoritative store for authored definitions**, plus the
**transactional outbox** that makes every emit durable + exactly-once.

**Definition tables** — `action`, `global_rule`, `channel_rule`, `milestone`, and the taxonomy tables
`action_group` / `experience`: each `{ id PK, doc jsonb, updated_at }`. The Command Center authors these via the
action-library API.

**Outbox tables** (`infra/outbox-tables.sql`) — `outbox_member_facts`, `outbox_activations`, `outbox_defs`, each
`{ id uuid, aggregatetype text, aggregateid text, kind text, payload text, created_at }`. Every business write
inserts an outbox row **in the same transaction**; Debezium CDC-tails the tables and the Outbox Event Router SMT
publishes to Kafka: `aggregatetype → topic`, `aggregateid → key`, `kind → header`, `payload → value`
(`payload=NULL` → a compaction tombstone, for def deletes). **This outbox→Debezium path is how the durable facts
reach Kafka and, in turn, the Databricks lake** — see the Flink caveat in §4.

**`channel_touch`** `{ nba_id, channel, n bigint, PK(nba_id,channel) }` — the monotonic per-(member,channel)
send counter the state machine increments atomically at DISPATCH to pick the touch-template (`touchKeys[min(n,
len-1)]`).

---

## 4. Engine state stores (the KStreams / Flink variants)

The reference engines hold the snapshot/eligibility state **locally** instead of in Redis (that's the whole
point — see [PERFORMANCE.md](../PERFORMANCE.md)).

- **KStreams (`nba-decision-engine`)** — two RocksDB stores: `nba-snapshot-store` (keyed by `nbaId`, event-time
  LWW; the changelog topic is the durable truth, replacing Redis-as-truth) and `nba-eligibility-store`
  (materializes `nba.evaluations`). Both are backed by compacted **changelog topics** + 1 warm standby replica;
  served over Interactive Query. Cold start = replay the changelog.
- **Flink (`nba-flink-engine`)** — Flink **keyed state** per operator: `SnapshotLwwFn` (per-`nbaId` snapshot),
  `StateMachineFn` (`SmState` per `nba-ca:{nbaId}:{actionId}:{channel}` with debounce/throttle/TTL timers),
  `ActionLayerFn` (delivery timers). Durability = Flink **checkpoints** (EXACTLY_ONCE_V2).
  **Note (prod/Databricks):** the Flink job writes Kafka **directly** (exactly-once checkpointed sinks), not
  through the Postgres outbox — but the lake + RL scorer **read the Kafka topics**, so they still see Flink's
  facts in authoritative mode (the outbox is the classic path's transactional emit, not a lake dependency). The
  real prod items are small: one authoritative writer at a time, gate Flink's local score stage off in favor of
  the Databricks RL scorer, and add the `channel_touch` counter. See
  [`services/nba-flink-engine/README.md`](../services/nba-flink-engine/README.md).

---

## 5. Temporal

One **`ChannelActionWorkflow`** per `nba-ca:{nbaId}:{actionId}:{channel}` on task queue `nba-channel-actions`
(reuse `ALLOW_DUPLICATE`, conflict `USE_EXISTING`). State: the lifecycle booleans + `currentState` + score +
the disposition queue; driven by signals (`suppress`/`operatorSuppress`/`disposition`/`softComplete`/
`hardComplete`) and debounce/throttle/TTL timers. The namespace must have the **custom search attributes**
`NbaActionId` + `NbaChannel` registered (`run-nba-temporal.ps1` does it on boot — a missing one fails every
start). Persistence is in-memory `start-dev` locally; a real DB (Postgres/Cassandra) in prod.

---

## 6. Databricks medallion + the model artifact

The cloud/prod tier (optional for local — the stand-ins close the loop). Unity Catalog `workspace.nba_poc`
(`NBA_LAKE_NS`). Detail in [08-data-and-lake.md](08-data-and-lake.md) / [11-ml-pipeline.md](11-ml-pipeline.md).

- **bronze** — `bronze_member_activity` (raw `datalake.streaming-inbound`).
- **silver** — `silver_fact_history` (every fact event, the audit log), `silver_eval_eligible`,
  `silver_activations`, `silver_snapshots`, `silver_milestones`, `dim_definitions`.
- **gold** — `gold_member_snapshot` (current value per `(nbaId,key)`, CDF-enabled — **the online feature store**
  the inbound hot path reads over the SQL warehouse), `gold_member_idmap`, `gold_member_facts` (view),
  `action_fact_map` (view), `gold_rulefacts_state`.
- **`rl_qnet.json`** — the trained **CQL Q-net** (source of truth `databricks/ml/rl_qnet.json`; mirrored into
  `services/nba-model/` for the local scorer). Schema: `{ state_dim, action_dim, feature_cols[], milestones[],
  obs_mean[], obs_std[], layers[]{W,b} }`. Served by the Databricks `nba-cql` endpoint (`@champion`) in prod, or
  pure-numpy by `nba-model` locally.

---

## Operational notes
- **Authoritative backups**: Postgres (`actionlib`) + the model artifact + the Redis `nba:idmap` (or the gold
  `gold_member_idmap` it can be rebuilt from). Everything else is derived and rebuildable.
- **Wipe/rebuild order**: trim/recreate the Kafka topics → flush Redis loop state (keep `nba:rulefacts` + sim) →
  truncate the medallion data tables (keep `dim_definitions`) → reseed. `infra/nba-clean-reset.sh` does this for
  the full (Databricks) loop; `up.ps1` seeds the local-only loop.
- **The id-map is load-bearing**: deleting `nba:idmap:*` without `gold_member_idmap` to rebuild from orphans
  every existing snapshot (new `nbaId`s get minted). Treat it as authoritative.
