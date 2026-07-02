# 04 · Message Schemas

The wire format on every NBA topic is **JSON** (string key, string value, optional string headers). There is no schema registry — Debezium's outbox router and every producer use `StringConverter`. This document is the authoritative schema spec. Avro IDL is provided at the end as a formal contract (the canonical types you would register if/when a registry is introduced).

## Topic catalog

All topics are 1 partition / 1 replica today (the keying is partition-ready — see [10-scaling-throughput.md](10-scaling-throughput.md)). Core topics are `cleanup.policy=compact`; DLQs are `delete` with 7-day retention.

| Topic | Policy | Key | Producers | Consumers |
|-------|--------|-----|-----------|-----------|
| `datalake.streaming-inbound` | compact | source-defined | Source systems | Databricks lake ingest |
| `nba.facts` | ~~compact~~ | — | **RETIRED** | **RETIRED** — was the all-facts ML feature firehose; ML reads features from Unity Catalog now, nothing consumes it. Not created by `create-topics.ps1`; don't recreate it. (snapshot-builder still has a dormant firehose path keyed `entityType:entityId`.) |
| `nba.member.facts` | compact | `entityType:entityId` | Lake, scorer (Databricks CQL / local nba-journey-scorer), action-library (incl. hot-path write-through), temporal (outbox), action-layer, action-router | snapshot-builder, temporal bridge, temporal disposition consumer |
| `nba.snapshots` | compact | `nbaId` | snapshot-builder | rules-engine |
| `nba.evaluations` | compact | `nbaId` | rules-engine | scorer (Databricks CQL `nba_ml_score_rl` / local nba-journey-scorer stand-in), action-router |
| `nba.activations` | compact | `nbaId:actionId:channel:sm` | temporal (outbox), action-library (inbound tracking, direct) | action-layer, Databricks lake ingest (tracking) |
| `nba.definitions` | compact | `TYPE:id` | action-library (outbox), snapshot-builder (forwarding) | rules-engine, temporal throttle-feed, action-router, KIE server |
| `nba.dlq.snapshot-builder` | delete 7d | orig. key | snapshot-builder | Command Center |
| ~~`nba.dlq.ml-scorer-features`~~ | delete 7d | orig. key | **RETIRED** (ml-scorer) | dead with the service |
| ~~`nba.dlq.ml-scorer-scorer`~~ | delete 7d | orig. key | **RETIRED** (ml-scorer) | dead with the service |
| `nba.dlq.action-router` | delete 7d | orig. key | action-router | Command Center |
| `nba.dlq.action-layer` | delete 7d | orig. key | action-layer | Command Center |
| `nba.dlq.temporal-disposition` | delete 7d | orig. key | temporal | Command Center |
| `nba.dlq.temporal-bridge` | delete 7d | orig. key | temporal | Command Center |
| `nba_connect_configs/offsets/status` | compact | — | Kafka Connect internal | Kafka Connect |

### The `.shadow` sibling-topic convention

The two **reference engines** (`nba-decision-engine`, the Kafka Streams spine port; `nba-flink-engine`, the Flink spine port) run side-by-side with the classic Redis spine in **shadow mode** (the default). In shadow mode they compute the full spine but write their outputs to `.shadow` siblings of the real topics — same shapes, zero blast radius — so the two implementations can be diffed against the live spine and latency-measured. Flip a reference engine to **authoritative** and it writes the real topics + Redis mirrors and becomes the writer (the classic services keep running untouched, for instant rollback).

The shadow siblings are pre-created (idempotently) by each engine's `run.ps1` so the engine's Kafka sources don't fail on a missing topic: `nba.member.facts.shadow`, `nba.snapshots.shadow`, `nba.evaluations.shadow`, `nba.activations.shadow`, `nba.facts.shadow`, and the engine's own DLQ shadow (`nba.dlq.flink-engine.shadow`). Same key/policy as their real counterparts.

### The `nba.member.facts` `kind` header

`nba.member.facts` multiplexes external facts and six internal message kinds, disambiguated by the Kafka `kind` header (a UTF-8 string). Routing depends on it:

| `kind` | Producer | snapshot-builder does | temporal does |
|--------|----------|----------------------|---------------|
| *(none)* + `origin=lake` | lake | snapshot it (it's a member fact) | — |
| *(none)* + `source=hotpath` | action-library (hot-path write-through) | snapshot it — re-applies the presented facts as the SELF-HEAL (event-time LWW) if the optimistic Redis write lost a race | — |
| `score` | scorer (Databricks CQL / local nba-journey-scorer) | snapshot it (firehose to `nba.facts` is dormant — topic retired) | — |
| `state` | temporal (outbox) | snapshot it + firehose | — |
| `throttle-suppress` | temporal (outbox) | route to `nba.definitions` as `THROTTLE_HOT:{ch}` | — |
| `disposition` | action-layer | snapshot it | consume (disposition consumer) |
| `completion` | action-library | snapshot it | consume (disposition consumer) |
| `router` | action-router | **skip** (pipeline control, not a member attribute) | consume (bridge) |

## The canonical fact vocabulary

A **fact** is `{entityType, entityId, key, value, valueType, eventTs, source}` (+ optional `nbaId`). `valueType` ∈ `{LONG, DOUBLE, BOOLEAN, STRING, OBJECT}`.

| Key | Type | Produced by | Meaning |
|-----|------|-------------|---------|
| `operator.activity.respondedToOutreach` | BOOLEAN | lake | Engaged with any outreach — unlocks the qualifying stage. |
| `operator.activity.registeredForPortal` | BOOLEAN | lake | Completed member-portal registration. |
| `operator.activity.hraCompleted` | BOOLEAN | lake | Finished the Health Risk Assessment. |
| `operator.activity.pcpSelected` | BOOLEAN | lake | Selected a primary care physician. |
| `operator.profile.isDNC` | BOOLEAN | lake (← `is_dnc`) | Do-Not-Contact. |
| `operator.profile.smsConsent` | BOOLEAN | lake (← `sms_consent`) | SMS consent. |
| `operator.comms.totalThisWeek` | LONG | lake comms-count | Total comms this rolling week. |
| `operator.comms.emailsThisWeek` | LONG | lake comms-count | Emails this rolling week. |
| `operator.plan` / `operator.mrr` / `operator.csat` / `operator.lastEvent` … | various | lake | Non-rulefact "color" (future ML). The old all-facts firehose (`nba.facts`) is retired — rich features now read from Unity Catalog / gold, not the bus. |
| `nba.throttle.{channel}.daily` | LONG | lake throttle-emit | Global sends today on a channel. |
| `nba.throttle.{channel}.rate` | LONG | lake throttle-emit | Global sends in the rate window. |
| `nba.score.{actionId}.{channel}` | OBJECT | scorer (Databricks CQL / local nba-journey-scorer) | Propensity score object (flattened to its `.score` double in the snapshot). |
| `nba.actionstate.{actionId}.{channel}` | STRING | temporal | Current workflow state (one of the 11). |
| `nba.disposition.{actionId}.{channel}` | STRING | action-layer | Raw provider status (e.g. `Opened`). |
| `nba.completion.{actionId}` | STRING | action-library `/completion`; **action-router** (from the eval's `newCompleted[]` transition array) | Hard-completion signal (`"completed"`). |
| `nba.milestone.{id}` | STRING | **action-router** (from the eval's `newMilestones[]` transition array) | Milestone-passed signal — published once per milestone the eval just transitioned (`treePass && fact-absent`). |
| `nba.actionsuppress.{target}` | — | action-library `/suppress` | Operator suppression (routed to `nba.definitions`). |

**Always-attach keys** (`nba.score.*`, `nba.actionstate.*`, `nba.disposition.*`, `nba.completion.*`) bypass the snapshot lean filter. Everything else must be in `nba:rulefacts` to be snapshotted.

**Hot-path source (`source=hotpath`).** On a real hot path (presented facts on the inbound serve, or an inbound disposition), the action-library's optimistic write-through emits the presented facts to `nba.member.facts` as ordinary member facts with `source=hotpath` — the DURABLE leg of the write-through. The snapshot-builder re-applies them (event-time LWW) so the bus stays the source of truth even though the API also warmed Redis directly. No new keys — these are the same member-fact keys, distinguished only by `source`.

## Message shapes

### A member fact (`nba.member.facts`)

```json
{
  "entityType": "OPERATOR",
  "entityId": "op-sg-0",
  "key": "operator.activity.respondedToOutreach",
  "value": true,
  "valueType": "BOOLEAN",
  "eventTs": 1749012345678,
  "source": "lake:crm-export",
  "origin": "lake"
}
```
Key: `OPERATOR:op-sg-0`. `origin` present only on lake re-emissions.

### Throttle count fact (`nba.member.facts`)

```json
{
  "entityType": "SYSTEM", "entityId": "__throttle",
  "key": "nba.throttle.email.daily", "value": 847, "valueType": "LONG",
  "eventTs": 1749012345678, "source": "throttle-lake", "origin": "lake", "windowSeconds": 300
}
```
Key: `SYSTEM:__throttle:nba.throttle.email.daily`.

### Per-member comms count (`nba.member.facts`)

```json
{
  "entityType": "OPERATOR", "entityId": "op-sg-0", "nbaId": "nba_abc123",
  "key": "operator.comms.emailsThisWeek", "value": 3, "valueType": "LONG",
  "eventTs": 1749012345678, "source": "comms-lake", "origin": "lake"
}
```

### Snapshot (`nba.snapshots`, key `nbaId`)

```json
{
  "nbaId": "nba_a1b2c3d4e5f6",
  "entityType": "OPERATOR",
  "entityId": "op-sg-0",
  "correlationId": "550e8400-e29b-41d4-a716-446655440000",
  "updatedTs": 1749000005000,
  "facts": {
    "operator.activity.respondedToOutreach": { "value": true, "valueType": "BOOLEAN", "eventTs": 1749000001234, "source": "lake:crm-export" },
    "operator.profile.isDNC":                { "value": false, "valueType": "BOOLEAN", "eventTs": 1749000001236, "source": "lake:crm-export" },
    "nba.score.action_reengage.email":       { "value": 17.5293, "valueType": "DOUBLE", "eventTs": 1749000003000, "source": "ml" },
    "nba.actionstate.action_reengage.email": { "value": "PRESENTED", "valueType": "STRING", "eventTs": 1749000004000, "source": "temporal" }
  }
}
```
`facts` holds only rulefacts + always-attach keys. `nba.score.*` is flattened to the bare `.score` double.

### Evaluation (`nba.evaluations`, key `nbaId`, header `type=eligibility|score`)

```json
{
  "nbaId": "nba_a1b2c3d4e5f6",
  "entityType": "OPERATOR",
  "entityId": "op-sg-0",
  "correlationId": "550e8400-...",
  "evaluatedAt": 1749000006000,
  "eligibilityChanged": true,
  "channelActions": [
    {
      "actionId": "action_reengage", "channel": "email", "name": "Re-engage Lapsed Member",
      "ttlSeconds": 600, "contentKey": "tmpl.action_reengage.email.v1",
      "eligible": true, "score": 17.5293,
      "active": false, "cancellable": false,
      "softCompleted": false, "hardCompleted": false, "workflowState": null
    }
  ],
  "milestones": [ { "id": "first_login", "name": "First Login", "completedAt": 1748900000000 } ]
}
```

ChannelAction flags: `eligible` (Drools hit, not throttle-hot, not auto-excluded), `active` (workflowState ∈ ACTIVE_STATES), `cancellable` (workflowState == `CREATED`), `softCompleted` (disposition ≥ channel soft bar, recomputed each eval), `hardCompleted` (latched), `score` (null until ML scores).

### Router decision (`nba.member.facts`, header `kind=router`)

Single op (`CREATE` / `SUPPRESS` / `SOFT_COMPLETE` / `HARD_COMPLETE`), key `nbaId:actionId:channel`:
```json
{
  "nbaId": "nba_a1b2c3d4e5f6", "entityType": "OPERATOR", "entityId": "op-sg-0", "memberId": "op-sg-0",
  "op": "CREATE", "actionId": "action_reengage", "channel": "email", "name": "Re-engage Lapsed Member",
  "contentKey": "tmpl.action_reengage.email.v1", "ttlSeconds": 600, "score": 17.5293,
  "correlationId": "550e8400-...", "source": "action-router", "eventTs": 1749000007000
}
```
Batch (`op=CREATE_BATCH`), key `nbaId:channel:batch`: no top-level `actionId`; instead `"actions": [ {actionId, contentKey, name, ttlSeconds, score}, … ]`.

### Activation (`nba.activations`, key `nbaId:actionId:channel:sm`)

```json
{
  "op": "DISPATCH",
  "entityType": "OPERATOR", "memberId": "op-sg-0",
  "nbaId": "nba_a1b2c3d4e5f6", "entityId": "op-sg-0",
  "actionId": "action_reengage", "channel": "email", "name": "Re-engage Lapsed Member",
  "contentKey": "tmpl.action_reengage.email.v1", "ttlSeconds": 600, "score": 17.5293,
  "correlationId": "550e8400-...",
  "trackingId": "nba-ca:nba_a1b2c3d4e5f6:action_reengage:email|550e8400-...",
  "source": "state-machine", "eventTs": 1749000010000
}
```
`CANCEL` is the same shape with `op=CANCEL`. Batch DISPATCH carries `actions[]` each with its own `trackingId`. Outbound dispatches now also carry `nbaId`/`entityId` attribution (single-action `emitActivation`), so an outbound send is countable per action — and an action can re-dispatch after `EXPIRED`.

#### Inbound tracking events (`op=INBOUND_SERVE` / `op=INBOUND_DISPOSITION`, `source=inbound`)

The inbound "pull" path stamps tracking events DIRECTLY onto `nba.activations` (produced by the action-library, **not** via the outbox — fire-and-forget, no distributed transaction). A single `correlationId` links the serve to its disposition:

- `GET /next-action` stamps one `correlationId` on the served set and emits `op=INBOUND_SERVE`.
- `POST /disposition` accepts that `correlationId`, emits `op=INBOUND_DISPOSITION` linked to it, then stamps a NEW `correlationId` on the next-served set and emits its `INBOUND_SERVE`.

So the serve→disposition journey is linkable downstream — in the lake and the Command Center member timeline.

```json
{
  "op": "INBOUND_SERVE",
  "entityType": "OPERATOR", "memberId": "op-sg-0",
  "nbaId": "nba_a1b2c3d4e5f6", "entityId": "op-sg-0",
  "channel": "inbound",
  "actions": [ { "actionId": "action_reengage", "state": "eligible", "score": 17.5293 } ],
  "correlationId": "550e8400-...",
  "source": "inbound", "eventTs": 1749000010000
}
```
`INBOUND_DISPOSITION` is the same `source=inbound` shape carrying the served `correlationId` plus the disposition (`actionId`, outcome), linked to its preceding `INBOUND_SERVE`.

### Disposition (`nba.member.facts`, header `kind=disposition`)

```json
{
  "entityType": "OPERATOR", "entityId": "op-sg-0",
  "key": "nba.disposition.action_reengage.email",
  "value": "Delivered",        // RAW provider status — the ONLY thing the sender reports
  "valueType": "STRING", "eventTs": 1749000012000, "source": "action-layer",
  "correlationId": "550e8400-...", "memberId": "op-sg-0", "channel": "email",
  "contentKey": "tmpl.action_reengage.email.v1",
  "trackingId": "nba-ca:nba_a1b2c3d4e5f6:action_reengage:email|550e8400-..."
}
```
**The sender reports RAW only; the state machine owns the mapping.** The action-layer (a simulated provider /
real webhook) is a dumb adapter — it puts the raw provider status in `value` and nothing else. It does **not**
decide a canonical `state` (there is no `state` field). The **state machine** classifies raw → canonical itself
(`DispositionClassifier`, kept identical in the Temporal worker and the Flink engine): `Delivered`/`Opened`/
`LinkClicked`/… → `PRESENTED`; `Bounced`/`Undelivered`/`Failed`/`NoAnswer` → `FAILED`; `Unsubscribe`/`STOP`/
`Dismissed`/`Declined` → `DECLINED`; the cancel responses `Cancelled` → `SUPPRESSED`, `AlreadySent` →
`SUPPRESS_FAILED`. The rules engine independently reads the raw `value` for soft-completion. One source of truth
for "what a raw status means for the lifecycle," owned by the lifecycle.

### State fact (`nba.member.facts`, header `kind=state`)

```json
{
  "entityType": "OPERATOR", "entityId": "op-sg-0", "nbaId": "nba_a1b2c3d4e5f6",
  "key": "nba.actionstate.action_reengage.email", "value": "IN_PROCESS", "valueType": "STRING",
  "eventTs": 1749000011000, "source": "temporal"
}
```

### Score fact (`nba.member.facts`, header `kind=score`)

```json
{
  "key": "nba.score.action_reengage.email",
  "value": { "actionId": "action_reengage", "channel": "email", "score": 17.5293,
             "name": "Re-engage Lapsed Member", "ttlSeconds": 600, "contentKey": "tmpl.action_reengage.email.v1",
             "correlationId": "550e8400-..." },
  "valueType": "OBJECT", "eventTs": 1749000008000, "source": "ml",
  "nbaId": "nba_a1b2c3d4e5f6", "entityType": "OPERATOR", "entityId": "op-sg-0"
}
```
`eventTs` is the eval's `evaluatedAt` (replay-safe), not wall-clock.

### Definition (`nba.definitions`, key `ACTION:{id}` etc.)

```json
{
  "id": "action_reengage", "name": "Re-engage Lapsed Member", "ttlSeconds": 600,
  "channels": [ { "channel": "email", "contentKey": "tmpl.action_reengage.email.v1", "softCompletion": "LinkClicked",
                  "touchKeys": [ "tmpl.action_reengage.email.touch1", "tmpl.action_reengage.email.touch2", "tmpl.action_reengage.email.touch3" ],
                  "variants": [ { "contentKey": "tmpl.action_reengage.email.vip", "percent": 100,
                                  "conditions": { "op": "all", "conditions": [ { "fact": "operator.profile.diabetic", "cmp": "eq", "value": true } ] } } ] } ],
  "inclusion": { "op": "all", "conditions": [ { "fact": "operator.activity.respondedToOutreach", "cmp": "eq", "value": true } ] },
  "exclusion": { "op": "any", "conditions": [ { "fact": "operator.profile.isDNC", "cmp": "eq", "value": true } ] },
  "completion": { "op": "all", "conditions": [ { "fact": "operator.activity.hraCompleted", "cmp": "eq", "value": true } ] },
  "hardTtlSeconds": 604800, "autoExcludeOnCompletion": true,
  "factsUsed": ["operator.activity.respondedToOutreach", "operator.profile.isDNC", "operator.activity.hraCompleted"]
}
```
(Illustrative composite — shows every optional field. The seeded `action_reengage` is simpler: always-on inclusion, `completion = respondedToOutreach`, no touchKeys/variants.)
A channel's optional `touchKeys = [firstTouch, secondTouch, thirdTouch]` is the per-channel touch-template escalation: a monotonic per-(member, channel) send counter `n` selects `touchKeys[min(n, len) - 1]` (caps at the last template) at the single DISPATCH point. Absent `touchKeys` → the variant-selected `contentKey` is used unchanged.

Other definition keys: `GLOBAL_RULE:{id}` / `CHANNEL_RULE:{id}` (a `{id, name, channel?, logic}` doc); `MILESTONE:{id}`; `THROTTLE:{ch}.{metric}` (a throttle fact body); `THROTTLE_HOT:{channel}`; `ACTION_SUPPRESS:{target}` (`{"value": true|false}`). A `null` payload is a compaction tombstone (delete).

**Condition node**: `{ op: "all"|"any", conditions: [...] }` (branch) or `{ fact, cmp, value }` (leaf). `cmp` ∈ `{exists, eq, ne, lt, lte, gt, gte, in, nin}`.

### DLQ envelope (`nba.dlq.*`)

```json
{
  "consumer": "action-router", "topic": "nba.evaluations",
  "partition": 0, "offset": 12345, "key": "nba_a1b2c3d4e5f6",
  "value": "{...original raw record...}", "headers": { "type": "eligibility" },
  "error": "<exception.toString()>", "dlqTs": 1749000020000
}
```
Keyed by the original record key so replay preserves partition affinity.

## Avro schemas (formal contract)

The wire format is JSON, but these Avro records are the canonical type contract. (If a registry is introduced, register these under `ai.das.nba`.)

```json
{
  "type": "record", "name": "MemberFact", "namespace": "ai.das.nba",
  "fields": [
    {"name": "entityType", "type": "string"},
    {"name": "entityId",   "type": "string"},
    {"name": "key",        "type": "string"},
    {"name": "value",      "type": ["null","long","double","boolean","string"], "default": null},
    {"name": "valueType",  "type": {"type":"enum","name":"ValueType","symbols":["LONG","DOUBLE","BOOLEAN","STRING","OBJECT"]}},
    {"name": "eventTs",    "type": "long"},
    {"name": "source",     "type": "string"},
    {"name": "nbaId",      "type": ["null","string"], "default": null},
    {"name": "origin",     "type": ["null","string"], "default": null}
  ]
}
```

```json
{
  "type": "record", "name": "Snapshot", "namespace": "ai.das.nba",
  "fields": [
    {"name": "nbaId", "type": "string"},
    {"name": "entityType", "type": "string"},
    {"name": "entityId", "type": "string"},
    {"name": "correlationId", "type": "string"},
    {"name": "updatedTs", "type": "long"},
    {"name": "facts", "type": {"type":"map","values":{
        "type":"record","name":"FactVal","fields":[
          {"name":"value","type":["null","long","double","boolean","string"],"default":null},
          {"name":"valueType","type":"string"},
          {"name":"eventTs","type":"long"},
          {"name":"source","type":"string"}]}}}
  ]
}
```

```json
{
  "type": "record", "name": "Evaluation", "namespace": "ai.das.nba",
  "fields": [
    {"name": "nbaId", "type": "string"},
    {"name": "entityType", "type": "string"},
    {"name": "entityId", "type": "string"},
    {"name": "correlationId", "type": "string"},
    {"name": "evaluatedAt", "type": "long"},
    {"name": "eligibilityChanged", "type": "boolean"},
    {"name": "channelActions", "type": {"type":"array","items":{
        "type":"record","name":"ChannelAction","fields":[
          {"name":"actionId","type":"string"},
          {"name":"channel","type":"string"},
          {"name":"name","type":"string"},
          {"name":"ttlSeconds","type":"long"},
          {"name":"hardTtlSeconds","type":["null","long"],"default":null},
          {"name":"contentKey","type":["null","string"],"default":null},
          {"name":"eligible","type":"boolean"},
          {"name":"active","type":"boolean"},
          {"name":"cancellable","type":"boolean"},
          {"name":"softCompleted","type":"boolean"},
          {"name":"hardCompleted","type":"boolean"},
          {"name":"workflowState","type":["null","string"],"default":null},
          {"name":"score","type":["null","double"],"default":null}]}}},
    {"name": "milestones", "type": {"type":"array","items":{
        "type":"record","name":"Milestone","fields":[
          {"name":"id","type":"string"},
          {"name":"name","type":"string"},
          {"name":"completedAt","type":"long"}]}}}
  ]
}
```

```json
{
  "type": "record", "name": "Activation", "namespace": "ai.das.nba",
  "fields": [
    {"name": "op", "type": {"type":"enum","name":"ActOp","symbols":["DISPATCH","CANCEL","INBOUND_SERVE","INBOUND_DISPOSITION"]}},
    {"name": "entityType", "type": "string"},
    {"name": "memberId", "type": "string"},
    {"name": "nbaId", "type": ["null","string"], "default": null},
    {"name": "entityId", "type": ["null","string"], "default": null},
    {"name": "actionId", "type": ["null","string"], "default": null},
    {"name": "channel", "type": "string"},
    {"name": "name", "type": ["null","string"], "default": null},
    {"name": "contentKey", "type": ["null","string"], "default": null},
    {"name": "ttlSeconds", "type": ["null","long"], "default": null},
    {"name": "score", "type": ["null","double"], "default": null},
    {"name": "correlationId", "type": "string"},
    {"name": "trackingId", "type": ["null","string"], "default": null},
    {"name": "source", "type": "string"},
    {"name": "eventTs", "type": "long"}
  ]
}
```

```json
{
  "type": "record", "name": "Disposition", "namespace": "ai.das.nba",
  "fields": [
    {"name": "entityType", "type": "string"},
    {"name": "entityId", "type": "string"},
    {"name": "key", "type": "string"},
    {"name": "value", "type": "string", "doc": "raw provider status — the state machine classifies it to canonical"},
    {"name": "valueType", "type": "string"},
    {"name": "eventTs", "type": "long"},
    {"name": "source", "type": "string"},
    {"name": "correlationId", "type": "string"},
    {"name": "memberId", "type": "string"},
    {"name": "channel", "type": "string"},
    {"name": "contentKey", "type": ["null","string"], "default": null},
    {"name": "trackingId", "type": "string"}
  ]
}
```

## Channel funnels (raw → canonical)

| Channel | Funnel (raw statuses, all → `PRESENTED`) | Soft bar default | Fail raw | Decline raw |
|---------|-------------------------------------------|------------------|----------|-------------|
| email | Delivered → Opened → LinkClicked | LinkClicked | Bounced | Unsubscribe |
| sms | Delivered → LinkClicked | LinkClicked | Undelivered | STOP |
| push | Delivered → Opened | Opened | Failed | Dismissed |
| voice | Answered → Completed | Completed | NoAnswer | Declined |
| mail | Delivered | Delivered | — | — |
| inbound/pull | Presented → Accepted → Completed | Completed | — | — |

`value` carries the raw status; `state` carries `PRESENTED` (delivery), `DECLINED`, or `FAILED`. Soft/hard completion are decided by the **rules engine**, never emitted by the activation layer.
