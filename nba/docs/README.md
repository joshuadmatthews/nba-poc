# NBA (Next-Best-Action) — Documentation

The NBA platform is a real-time, event-driven decisioning engine. It continuously assembles what is known about each member, decides the single best action to take on each communication channel, sends exactly one communication per action, watches what happens, and recirculates the outcome so the next decision is better. Every decision is durable, replayable, and auditable.

This directory is the authoritative technical documentation. It reflects the system **as built** (verified against source, June 2026).

## Reading order

| # | Doc | What it covers |
|---|-----|----------------|
| 00 | [Overview](00-overview.md) | What the system does, the mental model, the glossary. Start here. |
| 01 | [Architecture](01-architecture.md) | Components, the data-flow loop, topology diagram, design principles. |
| 02 | [Process flows](02-process-flows.md) | End-to-end sequence diagrams: ingest → decision → send → recirculation. |
| 03 | [State machine](03-state-machine.md) | The 11 canonical workflow states, transitions, debounce dedup, TTL/EXPIRED. |
| 04 | [Message schemas](04-message-schemas.md) | Every Kafka topic, every message shape, the fact vocabulary, Avro schemas. |
| 05 | [Corner cases](05-corner-cases.md) | Edge cases and how the system handles them. |
| 06 | [Component specs](06-component-specs.md) | Per-service deep specs: I/O, config, internals, failure modes. |
| 07 | [Command Center](07-command-center.md) | The operator UI + BFF, feature-by-feature, with screenshots. |
| 08 | [Data & medallion lake](08-data-and-lake.md) | The Databricks medallion, normalization, tables, the out/throttle/comms jobs. |
| 09 | [Infrastructure](09-infrastructure.md) | Containers, networking, the transactional outbox + CDC, boot order, deploy. |
| 10 | [Scaling & throughput](10-scaling-throughput.md) | Partitioning, predicted throughput, bottlenecks, capacity planning. |
| 11 | [ML pipeline](11-ml-pipeline.md) | The learned propensity layer: training, scoring, champion/challenger, simulation, RL. |
| — | [Rules engine deep-dive](rules-engine.md) | How operator JSON becomes live Drools: dynamic DRL synthesis, the definitions topic, eligibility eval. |
| — | [Hard vs soft completion](hard-soft-completion.md) | The two completion concepts in one page. |
| — | [ML bundle](../databricks/ml/README.md) | The Databricks ML Asset Bundle (notebooks + jobs) — implementation. |
| — | [Performance study](../PERFORMANCE.md) | The three-architecture throughput comparison (classic Redis / KStreams RocksDB+IQ / Flink whole-spine), decision-oriented. |
| — | [Load-test results](../infra/loadtest-results.md) | Raw load-test methodology, runs, and gotchas behind the performance study. |

## The one-paragraph summary

Source systems stream raw, dialect-specific records into a Databricks **medallion lake**, which normalizes them into a single canonical **fact** vocabulary and emits them onto Kafka. A **snapshot-builder** folds facts into a per-member snapshot; a **rules-engine** (Drools, compiled at runtime) evaluates eligibility and produces a unified `channelActions[]` evaluation; a **scorer** (the Databricks CQL model in prod, or the local **nba-journey-scorer** stand-in) attaches propensity scores; an **action-router** picks the winning action per channel; a **Temporal state machine** runs one durable workflow per (member, action, channel) that debounces, throttle-gates, dispatches, and tracks the outcome through 11 canonical states; a **unified activation layer** sends exactly one communication and reports dispositions; and every outcome **recirculates** back into the snapshot for the next decision. Alongside this async outbound loop, a **synchronous inbound hot path** (`GET /next-action` with facts → `POST /disposition` → `POST /completion`) serves a real-time decision off the facts a member presents on arrival, reading the rich model features straight from **gold** via the SQL warehouse (no Redis feature cache); inbound members are modeled by the **nba-inbound-sim** client driving those real APIs. A **Command Center** visualizes the entire live system and lets operators author actions and rules.

## Conventions

- **Member / operator** — the person an action targets. Identified externally by `entityType:entityId`, internally by a minted `nbaId`.
- **Fact** — one typed key/value about a member, e.g. `operator.activity.daysSinceLogin = 20`.
- **Action** — an authored thing you might do (e.g. "Reengage Email"). It runs on one or more **channels**.
- **ChannelAction** — one (action, channel) pair. The unit of decisioning and the unit of the state machine.
- **nba-inbound-sim** — a local client that models real inbound members by driving the inbound hot-path APIs (serve → disposition → completion) on the action-library.
- All code references are `file:line`. All services are single-purpose Java 21 apps except the lake (PySpark) and the Command Center (Node BFF + React UI).
