# NBA POC — a fact-centric Next-Best-Action engine

A working proof-of-concept of a Kafka/fact-based **Next-Best-Action** platform: member facts stream in, a
per-member **snapshot** is maintained, rules decide **eligibility**, an RL model **scores**, a router **picks**,
and a Temporal state machine **dispatches** the action on the right channel with debounce + throttle. Built
layer-by-layer on a self-contained local stack (Redpanda + Redis + Postgres + Temporal on podman). Isolated
streaming (`ais-nba-*`) so it can never wobble anything else.

This repo also contains a **three-architecture throughput study** (classic services vs Kafka Streams vs Apache
Flink) — see [Findings](#findings) and [`infra/loadtest-results.md`](infra/loadtest-results.md).

---

## Quickstart (one command)

```powershell
# First run — compiles every service image from source (~10–15 min), then boots + seeds + smoke-tests:
pwsh nba/up.ps1 -Build

# Subsequent boots reuse cached images (~2–3 min):
pwsh nba/up.ps1

# Tear down (keeps data volumes):
pwsh nba/down.ps1
```

`up.ps1` creates the network, boots infra → topics → services in dependency order (health-gated), seeds a
working set of action/rule definitions + 200 demo members, and confirms facts flow through to snapshots and
evaluations. **No Databricks, no cloud, no tunnel** — the local stand-ins (`nba-model`, `nba-journey-scorer`,
`nba-conversion-sim`) close the loop. When it's up: Temporal UI at <http://localhost:8233>.

### Prerequisites
- **podman** (or Docker with a `podman` alias) with a running machine, ~8 GB RAM free for the stack.
- **PowerShell 7+** (`pwsh`) — runs on Windows/macOS/Linux. The orchestration + service scripts are `.ps1`;
  the load-test + seed helpers are `bash` + `python3` (Git Bash/WSL on Windows).
- That's it. The ML/lake tier is optional (see [Local vs Databricks](#whats-local-vs-databricks-optional)).

---

## Architecture

### The flywheel (outbound decision loop)
```
 member facts ─▶ snapshot-builder ─▶ rules-engine ─▶ journey-scorer ─▶ action-router ─▶ temporal ─▶ action-layer
 (nba.member.facts)   │ Redis snapshot   │ eligibility    │ ML scores      │ pick top      │ debounce   │ send +
                      │ (LWW per key)    │ (Drools)       │ per channel    │ per member    │ + throttle │ disposition
                      ▼                  ▼                ▼                ▼               ▼            │
                 nba.snapshots      nba.evaluations   nba.member.facts  nba.activations  ChannelAction │
                                                       (scores)                          Workflow ◀────┘
                                                                                          (dispositions ride back as facts)
```
- **Fact** = one fact per Kafka message, keyed by member so every fact for a member lands on one partition.
  Event-time **last-writer-wins** per fact key. See [`docs/04-message-schemas.md`](docs/04-message-schemas.md).
- **Snapshot** = one Redis hash per member (`nba:snapshot:{nbaId}`): the current state the rules engine reads.
  What accumulates vs falls off, and three real journey captures, are documented in
  [`services/snapshot-builder/SNAPSHOT.md`](services/snapshot-builder/SNAPSHOT.md).
- **State machine** = a Temporal `ChannelActionWorkflow` per (member, action, channel): CREATED → (debounce) →
  SENT → … → HARD_COMPLETED, with throttle + suppression. See [`docs/03-state-machine.md`](docs/03-state-machine.md).
- There is also a **synchronous inbound hot path** (`GET /next-action`, `POST /disposition`) served by
  `action-library` + `nba-model` for real-time same-channel responses.

Full design docs live in [`docs/`](docs/) (`00-overview` → `12-healthcare-nba-redesign`).

### Three reference implementations of the spine
The same spine is implemented three ways so they can be compared head-to-head:

| | where state lives | hot-path reads | what it's for |
|---|---|---|---|
| **classic** (default) | Redis (snapshot) | Redis directly | the baseline; what `up.ps1` runs |
| **KStreams** (`nba-decision-engine`) | RocksDB (local) | Interactive Query (HTTP) | disk-backed state, shards per partition |
| **Flink** (`nba-flink-engine`) | Flink keyed state | Redis write-through mirror | the whole spine (incl. state machine) as one job |

Start the engines alongside the classic spine with `pwsh nba/up.ps1 -Engines` (shadow mode — they write
`.shadow` topics and drive nothing).

---

## Findings

Consolidated profile: [`PERFORMANCE.md`](PERFORMANCE.md). Raw runs + methodology: [`infra/loadtest-results.md`](infra/loadtest-results.md). Headlines:

**Per-stage throughput** (per instance, single partition, constraints removed):

| stage | classic | KStreams | Flink |
|---|---|---|---|
| snapshot build | 2,561/s (Redis HSET) | 6,951/s (RocksDB) | ≥20,000/s (heap, write-through OFF) |
| rules eval | 3,326/s (Drools) | — | 20,709/s (Java condition-tree) |
| action router | 3,455/s | — | ~20k (flatMap) |
| state machine | Temporal 15 → ~180/s | — | runs; not Kafka-lag-measurable |

1. **The classic spine is uniform at ~2.5–3.5k/s/instance** — JVM consumer + per-record work + Redis round-trip.
   It scales out with partitions × pods, not faster code.
2. **The compute stages are not the bottleneck — the state store and the workflow engine are.** Redis hit a
   hard **RAM wall** (OOM at ~55k members / 256 MB → system-wide write failure). Disk-backed engines shed it.
3. **Temporal's "12/s" was a client artifact, not a rate limit.** The bridge issued workflow starts serially
   on one blocking thread. Parallelizing (`NBA_BRIDGE_CONCURRENCY`) lifts it to **~180/s on the same box**,
   where it plateaus on the single-node server (zero `RESOURCE_EXHAUSTED`). Scales out in prod. *(A latent bug
   also surfaced: the custom Temporal search attributes weren't registered on boot — every start failed silently.
   Fixed: `run-nba-temporal.ps1` now registers them.)*
4. **Redis should stay the hot-path read cache for all three** — because the KStreams Interactive-Query read
   surface **failed** under load (100% HTTP 500 on rebalance/recovery). Flink's answer is **Redis write-through**
   (compute on local state, mirror to Redis). But write-through reintroduces a per-record Redis `HSET`, so a
   single instance is back to the **~2.5–3k/s Redis-write bound** — the engine's win *with* the read cache is
   **parallelism + sharded/TTL'd Redis + shedding the state RAM-wall**, not single-slot speed.
5. **The case for the disk-backed engines is the daily bulk burst:** ~8M scores dropped once a day drain in
   ~40–53 min serially on the classic path (blocking live facts), vs minutes when partitioned across slots/pods.

### Run the load tests yourself
```bash
bash nba/infra/run-loadtests.sh            # core spine matrix (classic snapshot/rules/router)
bash nba/infra/run-loadtests.sh --engines  # also the Flink shadow rules stage
```
The Temporal start-rate sweep, the authoritative write-through test, and the read-latency matrix are documented
as procedures in [`infra/loadtest-results.md`](infra/loadtest-results.md) (they need mode switches / OOM headroom
and aren't fully automated).

---

## Repo layout
```
nba/
  up.ps1 / down.ps1            one-command bring-up / teardown
  README.md                   this file
  docs/                       design docs (00-overview … 12-healthcare-redesign, process flows, schemas)
  infra/                      run-nba-*.ps1 (Redpanda/Redis/Postgres/Temporal/Connect), create-topics,
                              seed/ (captured definitions + redis state), reseed-members-local.py,
                              loadgen.py / gen.py / loadtest-*.sh, run-loadtests.sh, loadtest-results.md
  services/                   one folder per service, each with run.ps1 + Containerfile + src
    snapshot-builder/         classic snapshot (Redis)  — see SNAPSHOT.md
    rules-engine/             Drools eligibility
    action-router/            evaluations -> CREATE/SUPPRESS
    nba-temporal/             the state-machine bridge + ChannelActionWorkflow
    action-layer/             dispatch -> send -> disposition
    nba-model/                local CQL Q-net scorer (numpy)        [Databricks-free stand-in]
    nba-journey-scorer/       local async scorer                    [Databricks-free stand-in]
    nba-conversion-sim/       completes a fraction of delivered     [Databricks-free stand-in]
    nba-decision-engine/      KStreams reference impl (RocksDB + IQ)
    nba-flink-engine/         Flink reference impl (whole spine)    — see its README.md
    action-library/           REST authoring + inbound hot path
    command-center/           authoring + analytics UI (optional)
```

---

## What's local vs Databricks-optional
The **entire outbound flywheel runs locally** with no Databricks. The ML/lake tier is optional:

| local stand-in | replaces (Databricks) |
|---|---|
| `nba-model` (numpy Q-net) | the `nba-cql` serving endpoint |
| `nba-journey-scorer` | the `score-rl` Databricks job |
| `nba-conversion-sim` | external conversion reporting |
| `up.ps1` seed (direct to `nba.member.facts`) | the lake bronze→silver→gold ingest |

The Databricks integration (medallion lake, RL retraining, gold feature store for the inbound hot path) is
wired but **off by default**; it needs service-principal creds (`databricks/*.env`, gitignored) and the external
tunnel. The command-center analytics tabs go dark without it; the flywheel itself does not care.

---

## Troubleshooting
- **`network aiservices_default not found`** — `up.ps1` creates it; if a service script is run directly first,
  `podman network create aiservices_default`.
- **All workflow starts fail / 0 workflows in Temporal** — the custom search attributes aren't registered.
  `run-nba-temporal.ps1` registers `NbaActionId`/`NbaChannel` on boot (in-memory `start-dev` loses them on
  restart). Re-run it.
- **`MESSAGE_TOO_LARGE` when bulk-producing** — don't pipe a big stream into `rpk topic produce`; it batches the
  whole stdin into one over-size request. Use the bounded `loadgen.py`/`gen.py` (kafka-python) producers.
- **`rpk topic produce -f '%k|%v'` only writes one record from multi-line stdin** — the `%v` is greedy. Always
  include the newline: `-f '%k|%v\n'`. (This silently broke multi-fact seeds during development.)
- **Redis `OOM command not allowed`** — the snapshot store hit `maxmemory` (256 MB, `noeviction`). That's the
  RAM wall from the study (§3). For a big local run, raise it: `redis-cli config set maxmemory 2gb`.
- **Snapshot count keeps climbing under an authoritative engine** — the full spine self-loops on
  `nba.member.facts` (the scorer/state-machine write facts back). Expected; it's why Kafka-lag can't measure
  authoritative throughput (§6a).
- **A stage shows 0 rec/s in a load test** — the consumer group rejoined mid-window (stop/start artifact) or
  the topic is compacted and `trim-prefix` was policy-rejected. Keep engines warm and measure the standing
  backlog; for compacted topics delete+recreate to empty them.
