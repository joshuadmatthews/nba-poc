# NBA POC — performance profile of the three architectures

Consolidated, decision-oriented summary of the load-test study. The raw runs, methodology, and gotchas are in
[`infra/loadtest-results.md`](infra/loadtest-results.md); snapshot dynamics are in
[`services/snapshot-builder/SNAPSHOT.md`](services/snapshot-builder/SNAPSHOT.md).

**Conditions for every number below:** per-instance, single Kafka partition, on the dev box (a 32 GB / 24-core
podman VM), with the safety constraints lifted (debounce 60→5 s, conversion-sim 0.4→1.0, throttles raised) so
each layer runs unthrottled. Engine numbers are **shadow mode / Redis write-through OFF** unless stated. These
are *per-instance* figures — horizontal scale (partitions × pods) is structural and not captured here.

The three architectures:
- **classic** — Redis-backed snapshot + Drools rules + router + Temporal state machine (the default `up.ps1` runs).
- **KStreams** (`nba-decision-engine`) — the *snapshot* stage on RocksDB state + an Interactive-Query read surface.
- **Flink** (`nba-flink-engine`) — the *whole spine* (snapshot → rules → score → route → state machine) as one job.

---

## 1. Per-stage throughput

| stage | classic | KStreams | Flink |
|---|---|---|---|
| snapshot build | **2,561/s** (Redis HSET, network RTT) | **6,951/s** (RocksDB, local disk) | **≥20,000/s** (heap state, write-through OFF) |
| rules eval | **3,326/s** (Drools, one KieSession/eval) | — | **20,709/s** (in-JVM condition-tree flatMap) |
| action router | **3,455/s** | — | ~20k (trivial per-record flatMap; lag-artifact*) |
| state machine | **Temporal 15 → ~180/s** (see §3) | — | runs; not Kafka-lag-measurable* |

KStreams reimplements only the **snapshot** stage (+ IQ reads); Flink reimplements the **whole spine** — so the
KStreams column is one cell by design. `*` Flink's checkpoint offsets diverge from the consumer-group committed
offset and the state machine self-loops on `member.facts`, so Kafka-lag can't measure those cells (the router is
the same trivial flatMap as the 20k rules stage).

**Reading:** the classic spine is **uniform at ~2.5–3.5k/s/instance** — the limit is the JVM consumer +
per-record work + Redis round-trip, the same at every stage. Flink's stateless transforms are ~6× because they
swap a KieSession-per-eval for an in-JVM condition-tree on heap state.

---

## 2. Read profile (the synchronous hot path)

The hot path is a point read of one member's current state (snapshot / eligibility) on the channel they just
used. Each flavor serves that read from a different store — and the **read model is the real differentiator
between the flavors, not the latency** (which is a wash, ~1.5–1.7 ms across all three, measured
app-representatively over the podman bridge — one network hop, the way the app actually reads).

| flavor | hot-path read store | read p50 (p95) | Redis required? |
|---|---|---|---|
| **classic** | Redis `HGETALL nba:snapshot:{id}` | **1.66 ms** (~14.4k rps) | **yes** — Redis is the authoritative read store |
| **KStreams** | Interactive Query → local RocksDB (HTTP `GET /snapshot/{id}`) | **1.53 ms** (3.12 ms) | **no** — serves from its own state; Redis dropped |
| **Flink** | Redis write-through mirror (`RouterFn` writes through on the authoritative path) | **1.66 ms** (== classic) | **yes** — Queryable State is deprecated in 1.18, so Flink mirrors to Redis rather than serving from state |

**Headline: read latency is a wash (~1.5–1.7 ms across all three).** What differs is *what infra the read needs*:

- **KStreams uniquely drops Redis.** With the IQ surface tuned for availability — KIP-535 stale reads
  (`enableStaleStores()`) + serve-from-standby (a standby-holding pod answers from its warm copy instead of
  bouncing to a possibly-dead active; `IqServer.java:78,88`) — reads stay up through a node kill (validated:
  200 reads straight through a rolling instance failure) and come back **latency-neutral vs Redis** (1.53 vs
  1.66 ms). This is the earlier "liability as built" surface, fixed: the availability problem was the
  redirect-to-dead-active, not the store. *Caveat: the read surface itself is still the POC `com.sun`
  `HttpServer` — single-threaded, measured earlier to degrade ~3.5× (~8 ms) at read-concurrency 10. The state
  store is production-grade; the HTTP shim is not — for production read QPS swap it for Netty/Javalin + a
  routing-aware retrying client.*
- **Classic + Flink keep Redis.** Classic is Redis-native. Flink *could* in principle serve from its keyed
  RocksDB state, but Flink Queryable State is deprecated as of 1.18 (the version we run) — so the engine is
  deliberately built to **write-through to Redis** on the authoritative path (`RouterFn`) and read from the
  mirror. Same ~1.66 ms as classic, same Redis dependency.

`redis-benchmark` raw-GET ceiling for reference (single key, no snapshot hash, no app hop): SET 86k/s, GET
77k/s, HSET 82k/s, p50 ~0.3 ms — the ~1.66 ms above is the full `HGETALL` of the snapshot hash across the bridge.

---

## 3. The three things that actually bound the system
The compute stages are not the limiter — these three are:

1. **Redis RAM wall — now at a FAIR ceiling.** The central Redis was originally capped at an unfairly-low
   `maxmemory 256 MB`, which OOM'd (`OOM command not allowed`, `noeviction`) at only **~55k members** on a 32 GB VM —
   not a fair comparison when RocksDB/Flink get the whole disk. **Raised to 8 GB** (`run-nba-redis.ps1`), which holds
   **~1.7M members**; the 150k-member load test now fits in ~120 MB with no OOM. The wall is still *architectural* —
   Redis state is RAM-bounded while RocksDB/Flink are **disk-backed (no wall)** — but at 8 GB it's a high, fair
   ceiling, not a 256 MB artifact. (The earlier "KStreams went down via the idmap setnx" cascade was a symptom of the
   same too-small cap, not a KStreams fault.)
2. **Temporal start rate.** The famous "12/s" was a **serial-client artifact**, not a rate limit: the bridge
   issued blocking `WorkflowClient.start` calls one at a time on a single consumer thread. Parallelizing
   (`NBA_BRIDGE_CONCURRENCY`): **1→15/s, 64→178/s, 128→170/s (plateau)** — single-node server-bound (~2 cores,
   worker idle, **zero `RESOURCE_EXHAUSTED`**). Scales out in prod (history shards + real persistence). *(A
   latent bug also surfaced: the custom search attributes weren't registered on boot — fixed in
   `run-nba-temporal.ps1`.)*
3. **Write-through cost (the read-cache tax).** The Flink 20k is **write-through OFF** (local compute, IQ reads).
   Turn the Redis read cache ON — the only viable read surface, since IQ fails (§2) — and a single instance does
   the same per-record `redis.hset` as classic, so it converges to the **~2.5–3k/s Redis-write bound**. The
   engine's advantage *with* the read cache is therefore **parallelism (N slots/pods × ~3k) + sharded Redis +
   shedding the state RAM-wall**, not single-slot speed.

---

## 4. Per-approach synthesis

**Classic (Redis state — the default).** Uniform **~2.5–3.5k/s/instance** at every stage; fastest and always-up
reads (Redis); lightest footprint (~260 MB/service). Hard ceiling: the central-RAM wall. **Wins on** read
latency, operational simplicity, small/medium member-state.

**KStreams (RocksDB + IQ).** Snapshot ~2.7× classic (**6,951/s**); disk-backed state sheds the RAM wall and
shards per partition. But the IQ read surface failed under load, and it's heavier (**~720 MB resident, 2.7–3.4×**
snapshot-builder, plus RocksDB on disk). **Wins on** state-size economics — only if reads don't depend on IQ.

**Flink (whole spine).** Stateless transforms ~6× classic with the cache off (**rules 20.7k**); the only one that
also replaces the **state machine** (now feature-complete — ThrottleGate + member-keyed dedup ported). With
write-through on it converges to the Redis-write bound per slot, so its real edge is **parallelism + sharded
Redis + shedding the state RAM-wall**. **Wins on** the daily bulk burst and full-spine scale-out.

---

## 5. Bottom line
**The compute stages are not the bottleneck — the state store (Redis RAM wall) and the workflow engine (Temporal
start path) are.** The shape the testing converges on is a **hybrid**:

- **disk-backed partitioned compute** (KStreams/Flink) to absorb the **~8M-scores/day bulk burst** — ~40–53 min
  of serial backlog on the classic path (blocking live facts) vs **minutes** when partitioned across slots/pods —
  and to scale member-state past economical RAM;
- **the hot-path read** stays on Redis for classic (native) and Flink (Queryable State deprecated in 1.18 →
  write-through mirror); **KStreams can drop Redis entirely** — the cross-flavor campaign (§8) showed the
  HA-tuned IQ surface reads latency-neutral vs Redis (1.53 vs 1.66 ms) and rides through a node kill. Where
  Redis is kept, TTL it to hot members so the mirror doesn't re-create the RAM wall;
- **Temporal scaled out** (more history shards / parallel starts) for the daily action-dispatch spike — *unless*
  you move to Flink, which removes the Temporal start path altogether.

### Which flavor for prod (the decision rule)
- **Classic** — the default. Stays the right call until member-state exceeds economical RAM, or write QPS
  saturates one Redis, or the daily burst can't drain in time. Simplest to operate; Redis-native reads.
- **KStreams** — the pick when you must **keep Temporal's per-workflow durability/isolation/interactive control**
  but the pain is the **Redis RAM/read tier**. It's the *only* flavor that drops Redis (HA-tuned IQ), shedding
  the RAM wall and the read-cache infra without touching the dispatch layer. Cost: harden the IQ HTTP surface
  (Netty/Javalin + routing-aware client) for prod read QPS; the state store itself is production-grade.
- **Flink** — the pick when the goal is to **bypass Temporal**. It's the bigger lever: it gets *both* of
  KStreams' state-tier wins **and** collapses ~7 processes → 1 (drops Temporal + the Postgres dynamic state +
  the Debezium/outbox tier) **and** removes the ~178/s Temporal start bound (bulk-suppress ~4.7 s/1M vs ~1.6 h).
  It attacks the dispatch ceiling, the RAM wall, and the component count in one move — and keeps Redis as the
  read mirror (less to productionize than KStreams' Redis-free path, since Redis reads are already prod-grade).
  Cost: you give up Temporal's per-workflow reset/terminate/query + history UI (domain suppress/cancel *is*
  wired in-stream; see §8's *Temporal trade*).

**Net:** the campaign made **KStreams the best "keep Temporal, drop Redis" option**, but it did **not** dethrone
**Flink as the answer to "bypass Temporal"** — the two win on different axes and are not competing for the same
slot. When the RAM / burst / dispatch math binds and Temporal's per-workflow control isn't a hard requirement,
**Flink is the preferred prod target.**

---

## 6. Re-validation (2026-06-30)

A `infra/run-loadtests.sh` re-run on the dev box reaffirmed the profile above:

- **Classic per-stage drain (full spine live, single partition):** rules **~2,375/s**, router **~1,916/s** — the
  same ~2k/s/instance order as §1. They sit below the isolated figures because the live spine *and* the load
  generator competed for CPU (stages pinned at **99–102%**); contention, not a regression.
- **The snapshot stage can't be measured with the spine running.** Its own loop-back (score / router / state facts
  re-enter `member.facts`) grows the consumer backlog faster than it drains, so the *net* rate reads negative. The
  §1 figure (**2,561/s**) is the downstream-isolated number, and that isolation is required for a clean read.
- **RAM wall — found, then fixed for fairness.** The first re-run (Redis still at the old 256 MB) reproduced the §3
  wall: driving 150k members overflowed Redis (`noeviction`) → `OOM` on writes → the snapshot-builder **wedged at
  0 drain**. But 256 MB is an unfairly-low cap on a 32 GB VM — it OOM'd classic at ~55k members while RocksDB/Flink
  had the whole disk. **Raised Redis to 8 GB** (`run-nba-redis.ps1`) and re-measured: the isolated classic snapshot
  drains at **~1,950/s** and Redis held the 150k-member working set in **~120 MB with no OOM**. The wall now sits at
  ~1.7M members — architectural (Redis is RAM-bounded), but a fair ceiling rather than a 256 MB artifact.
- **The shared scorer is the real throughput floor — and it's an I/O artifact, not compute.** The local
  journey-scorer (the Databricks-free heuristic: md5 + arithmetic bands, **no numpy**) drains `nba.evaluations` at
  only **~618/s** isolated. The cause is one line: it `producer.flush()`es **every eval** — a synchronous Kafka ack
  round-trip (~1.6 ms) that blocks the loop, so records never batch across evals. The scoring itself is microseconds.
  Two fixes, both measured here:
  - **Batch the produce** (`linger_ms` + drop the per-eval flush) → **~7,011/s on a single instance — 11×** — a
    one-line change. One batched instance beats four un-batched instances (1,510/s, contended box) by ~4.6×.
  - **Scale out** (Kafka consumer group, capped by partition count): 1 → 4 instances = 618 → ~1,510/s on this single
    box (sub-linear from CPU/broker contention; near-linear on real infra with more partitions/nodes).
  This is **cross-flavor**: classic / KStreams / Flink all route the *scored* decision through the SAME shared scorer
  (prod = the Databricks RL scorer), so an engine's compute speed (Flink rules ~20k/s) is moot for the scored path
  until the scorer keeps up. Scored-decision scale is a **scorer** problem — batch it first, then shard it.

**Bottom line — the FAIR comparison.** Per-instance write throughput is **classic ~2k/s (Redis HSET network
round-trip) < KStreams ~7k/s (local RocksDB) < Flink ~20k/s (heap)** — that gap is the *state medium*, not a memory
cap. Raising Redis memory removes the unfair early OOM but does **not** change classic's per-instance speed (it's
the network round-trip). The real bounds, in priority order: the **shared scorer** (~618/s as written → ~7k/s
batched → then shardable — the tightest bound but the cheapest to fix), the **state store** (Redis RAM-bounded at a
fair ~1.7M members vs disk-backed engines with no wall), and the **Temporal start path** (§3–§5). The compute stages
(snapshot / rules / router) are **not** the bottleneck on any flavor.

**End-to-end check (scorer batched, full spine).** Swapping the batched scorer into the live pipeline and driving
300k members confirmed the cascade: with the scorer no longer throttling, the **snapshot-builder's backlog explodes
(+~9k/s) while every other stage stays near zero** — the fast scorer *floods* `member.facts` with scores faster than
one snapshot-builder can `HSET` them. So the real per-lane limit is the **snapshot-builder's Redis write**, and it
can't be fixed by speeding up a single stage (that just dumps load downstream) — it needs partitioned lanes + a
write tier that scales (§5). Note the snapshot-builder already batches Redis (pipelined reads + one `MULTI` for the
writes — ~2 round-trips per *batch*, not per record), and `redis-benchmark` does ~82k HSET/s, so the ~1,950/s is the
**per-batch EOS commit + the single-partition design**, not Redis saturating. It is explicitly built to scale out
(`transactional.id … unique across instances when scaling out`): partition `member.facts` → N snapshot-builders → N
lanes.

---

## 7. Cloud Redis — Azure Cache for Redis vs Azure Managed Redis

The §3 RAM wall and the §5 "shard Redis to keep scale-out linear" conclusions point straight at the cloud Redis
choice. *(Tiers, limits, and retirement dates below are as of early 2026 — verify against current Azure docs.)*

**The core difference.** *Azure Cache for Redis* (the OSS-based Basic/Standard/Premium tiers, on a retirement path)
is **open-source Redis — single-threaded per shard**. *Azure Managed Redis* (the go-forward, built on the **Redis
Enterprise** stack) is **multi-threaded per node** — many shards/proxies across all cores, transparently. That one
fact drives the comparison against our three limits:

| our limit | Azure Cache for Redis (OSS tiers) | Azure Managed Redis (Enterprise) |
|---|---|---|
| **write throughput** (the snapshot `HSET` lane) | single-thread **per shard** → aggregate = shards × one core; high write QPS needs *many* shards | **multi-threaded per node** → far higher per-node writes → fewer nodes/shards for the same aggregate |
| **RAM wall** (§3/§6) | pure-RAM (Premium ~120 GB/instance); SSD tiering only on the separate *Enterprise Flash* tier | **Flash Optimized** tier = NVMe SSD tiering, hot data in RAM → large member-state at much lower $/GB — directly mitigates the RAM wall |
| **sharding** (partition by member) | **OSS Redis Cluster** — the *client* must be cluster-aware (our `JedisPooled` → `JedisCluster`), hash-slot routing, cross-slot ops restricted | **transparent clustering** — single endpoint (or OSS-cluster API), shards handled internally; shard-by-`nbaId` is simpler |

Plus Managed Redis adds active-active geo-replication (CRDTs), the modules (Search / JSON / …) included, and
generally better price/performance.

**For this system** Managed Redis is the better fit *and* the surviving product. Our bottleneck is the per-lane Redis
write, and the fix is "more lanes + shard Redis" (§5). On the OSS tiers that means many **single-threaded shards**
plus a cluster-aware client; on Managed Redis a **multi-threaded node absorbs many lanes before you even shard** —
same throughput, fewer Redis nodes, simpler client code. And the RAM wall we measured (§3/§6) is answered by the
platform via **flash tiering** (spill cold member-state to SSD) instead of buying ever-larger RAM nodes — exactly the
"TTL'd to hot members" idea in §5, solved by the tier. Migration note: had we run on OSS Premium clustering, the
snapshot-builder's `JedisPooled` would need cluster-awareness — moving to Managed Redis's single transparent endpoint
*avoids* that, so it's a simplification, not a port.

**At our prod scale — where does it actually bind?** Prod runs **10 partitions of everything** for **~200M
messages/day ≈ ~2,315/s average** (~231/s per partition; ~5–12k/s at peak). That's ~12% of the measured per-partition
snapshot bound (~2 k/s), and even at peak only ~10–15% of a *single* OSS-Redis thread (~80–100k ops/s). **So at
200M/day the write *rate* is not the bottleneck** — on either engine, either Redis: 10 lanes carry ~2–9× headroom and
one Redis thread absorbs the aggregate. The constraint that actually binds at this scale is the **RAM wall**, and it's
set by the **member population** (× ~4.6 KB/snapshot), *not* the message rate — tens of millions of members is
tens-to-hundreds of GB, exactly where OSS Redis's ~120 GB cap vs Managed Redis's **flash tiering** vs a disk-backed
engine's **no-wall** diverge.

**Where partitioning changes the story (the growth hedge).** The single-thread *write* ceiling only bites ≳10× higher
(toward billions/day / >80–100k writes/s) — and there the two disk-backed engines pull ahead **by construction**:
KStreams/Flink write **partition-local RocksDB**, so write capacity scales **linearly with the same 10 (→ N)
partitions we already run** — no central store to saturate, no RAM wall. A single unsharded Redis can't; matching it
means clustering (OSS: many single-threaded shards + a cluster-aware client) or leaning on Managed Redis's
multi-threaded nodes. Measured that it scales: 1 partition folded ~2,058/s, **2 partitions/2 instances ~5,263/s**
(partition-linear).

**"Which Redis" is undecided — so the safe hedge is not to bet on it.** On **Azure Cache for Redis** (single-thread,
RAM-capped, retiring) both ceilings bind sooner and partitioned RocksDB is the stronger play; on **Azure Managed
Redis** (multi-threaded + flash + transparent clustering) they bind far later and Redis stays viable. Since that
choice is open, the **disk-backed partitioned state (KStreams or Flink) is Redis-flavor-independent** — it scales
writes *and* state with the compute partitions we already run, whichever Redis we land on (kept purely as the
hot-read cache). That independence is itself an argument for it at 200M/day-and-growing.

---

## 8. What each flavor actually earns — components, Temporal, and the Redis-hot-path question

Throughput (§1, §6) is the *smallest* of the differences. The real trade is **hosted-component count, the Temporal
start-rate bound, and what disk-backed state buys when Redis stays the hot path.**

### Component count — Flink collapses the decision path into one job
The classic decision path is **seven hosted services**: snapshot-builder, rules-engine (Drools), action-router,
**temporal-worker + the Temporal server** (the state machine), action-layer, and a **Debezium / Kafka-Connect** worker
(`ais-nba-connect`). And the state machine doesn't live in Temporal alone — it leans on **Postgres + Debezium** for
both its dynamic state and its reliable publishing: the per-(member,channel) **send/touch counter** (`channel_touch`,
bumped atomically at each dispatch), the **in-flight/dispatch tracker** (`nba_inflight`), and a transactional
**outbox** (`outbox_*`) that **Debezium CDC-tails into Kafka** (→ `nba.member.facts` / activations / definitions —
"the loop's spine"). So the classic state-machine + dispatch tier is really **Temporal (server + worker) + Postgres
+ Debezium**. On top sit the shared scorer and the shared infra (Redpanda, Redis, the action-library, the BFF).
**Flink is ONE job** that wires all of it (`SpineJob`: snapshot → rules → score → route → **state-machine →
action-layer**). So Flink is *internally* more complex (one large stateful, checkpointed job) but **operationally
simpler — ~7 processes → 1** — and the headline: **it deletes Temporal** (worker + server; here an embedded DB, in
prod a whole frontend/history/matching cluster + DB), folds the Postgres-backed dynamic state into the stream, **and
drops the outbox + Debezium entirely** (Flink writes Kafka directly with exactly-once sinks — see below). KStreams
does **not** consolidate: it adds one service (the decision-engine) and
reimplements **only the snapshot stage**, keeping classic's rules, scorer, Temporal and action-layer — net component
count is essentially unchanged.

### Flink replacing Temporal is the scale unlock — quantified
Temporal's start path is one of the three hard bounds (§3): **~178 workflow-starts/s, single-node server-bound, a
plateau** that more client concurrency doesn't move (it needs history shards + real persistence to go further). For
the daily action-dispatch burst that's a wall — starting a few million workflows at ~178/s is **hours**, and it
competes with live traffic while it drains. Flink's state machine is a **`KeyedProcessFunction` + timers**
(`StateMachineStage`) with the throttle gate, the member-keyed dedup, and the `channel_touch` send-counter **ported
to Flink-managed keyed state** — so "dispatch" is keyed state advancing at the **stream rate, partitioned**, with no
per-workflow-start cost *and no per-dispatch Postgres round-trip*. So Flink doesn't only delete the Temporal
server/worker — it also retires the **per-member dynamic state** (`nba_inflight` / `channel_touch`) the classic path
keeps in Postgres, folding dispatch + throttle + send-counts + dedup into one keyed stream. And because Flink
**writes Kafka directly with exactly-once (EOS) sinks**, the stream's transactional sink *is* the reliable publish —
so it also drops the **outbox → Debezium** tier the classic path needs for that guarantee. **One Flink job retires
Temporal, the Postgres dynamic state, *and* the Debezium/outbox tier at once — that's the scale unlock: it removes a
single-node start bound, a per-dispatch database write, and a CDC hop in a single move.** (Postgres still backs the
action *catalog*, shared by both flavors — Flink reduces the Postgres footprint, it doesn't eliminate it.) This is a real trade, not a free win — see *The Temporal trade* below.

### The Temporal trade — what Flink gives up
Temporal and a keyed-stream job have genuinely different properties for stateful work. The honest list:

- **Out-of-band per-workflow ops + history UI (not domain control).** The *domain* cancel works on Flink — operator
  suppress is a first-class event (`StateMachineFn`: `OPERATOR_SUPPRESS` → pre-send `SUPPRESSED`, post-send
  `SUPPRESSING` → `CANCEL` → `SUPPRESSED`/`SUPPRESS_FAILED`), the **same cascade** as the Temporal workflow. What
  Temporal adds is the **generic, out-of-band** lever: open the Web UI, read any workflow's full history, query live
  state, and **terminate / reset an arbitrary stuck instance** for debugging/recovery. Flink's state is **keyed
  state** — no per-key history browser and no generic reset (you'd emit a domain event or surgically edit a
  savepoint). *Mitigated:* the same canonical events flow either way, so the Command Center timeline + medallion
  telemetry give full visibility into *what* happened — the gap is the **debug/recover-one-instance tooling**, not
  domain control or observability.
- **Bulk / fleet-wide control — the flip side (here the stream model *wins*).** Suppressing one action across a
  million members is the *opposite* trade. Classic already does it well — `suppressMatching` fires a **server-side
  Temporal Batch Operation** (Visibility query `NbaActionId='X' AND ExecutionStatus='Running'` → signal
  `operatorSuppress`), so one client call fans out across the namespace without enumerating workflows. But it's
  fundamentally **O(N) durable signals**: each matching workflow receives the signal, appends to its own history, runs
  a task, and persists — server-throttled, minutes-scale for millions, and it loads the cluster + persistence store.
  Flink's keyed-state model makes the same fan-out **naturally cheap**: broadcast *one* control record and apply it to
  every matching keyed entry locally (`applyToKeyedState` over the broadcast that already feeds the ThrottleGate) —
  memory-speed in each operator, no per-entity round-trip, durability folded into the next checkpoint (one write for
  all keys). *Status — wired end-to-end, tested, and measured:* `StateMachineFn` fans the cancel across every matching
  keyed instance via `applyToKeyedState` (pre-send → terminal `SUPPRESSED`; post-send → `SUPPRESSING` + a `CANCEL`
  activation, then the layer's disposition drains it — the same cascade as the per-key path), driven by the **existing**
  `POST /suppress` producer (its `ACTION_SUPPRESS` flag on `nba.definitions`, on the `false→true` transition; an explicit
  `OPSUPPRESS` command works too). Harness tests cover the fan-out, the producer-flag path, action/channel scoping,
  idempotency, and a **1,000-action in-process queue cancelled by one broadcast and drained to `SUPPRESSED`**. Measured
  (next subsection): a **1M-instance fan-out in ~4.7 s vs ~1.6 h** of Temporal Batch-Operation signals. Bulk mutation is
  where the data-parallel stream model structurally beats per-workflow actors.
- **Recovery scope (poison is already handled).** Poison records do **not** stall the job — Flink dead-letters them:
  `ClassifyResolveFn` routes unparseable records to a **DLQ side-output** (`DLQ_TAG` → `nba.dlq`, like the classic
  builder), and the other operators guard with try/catch. The real difference is *recovery scope*: a genuine job
  failure (OOM, checkpoint issue) restarts the **whole job from the last checkpoint** — a seconds-scale, all-keys
  blip — versus Temporal isolating a failure to the one affected workflow. Coarser, but a brief blip, not a poison
  stall.
- **State evolution / versioning.** Temporal versions and patches *running* workflows; changing the Flink
  state-machine logic is a **job redeploy + savepoint migration** with constrained keyed-state schema evolution — a
  heavier change process for live state.
- **Reliable external side-effects — a wash for *this* design (the one "downside" that mostly isn't).** The outbound
  send is already isolated in the **activation layer** (`ActionLayer` / `ActionLayerFn`: consume `nba.activations` →
  send → emit the delivery disposition) in **both** flavors, so the send's idempotency was never Temporal's job. What
  Temporal wraps in classic is only the *emit*: `emitActivation` is a retried activity (5 attempts / 30 s) landing in
  `outbox_activations`; Flink emits the same activation via an **EOS Kafka sink** — if anything *cleaner* (exactly-once
  vs the activity's at-least-once + a CDC hop). Temporal's activity model would shine only if the external call were a
  *synchronous, retried, heartbeated activity* — but we deliberately use the **async activation + disposition** pattern
  instead, which Flink replicates 1:1. So nothing real shifts here.
- **Long human-timescale durability.** Temporal is purpose-built for workflows that wait days/weeks durably; Flink
  timers handle TTL-scale waits fine, but very-long-lived per-entity state held in keyed state is heavier.

**Net:** the genuine Flink trade-offs are narrower than they first look — the *domain* operations (suppress = cancel,
debounce, throttle) and poison-handling (DLQ) are all there, and **bulk / fleet-wide control actually favors the
stream model** (broadcast fan-out vs O(N) durable signals). What stays in Temporal's column: the **out-of-band
single-instance tooling** (inspect / reset / terminate one arbitrary workflow + its history UI), **state evolution**
(a logic change is a job redeploy + savepoint migration vs patching running workflows), and **coarser recovery**
(whole-job checkpoint restart vs per-workflow isolation). Observability stays good either way (the same events flow).
For a high-throughput, mostly-automated decision loop that's a good trade; where you need rich single-instance
debug/intervention or very-long human-in-the-loop waits, Temporal still earns its keep.

### Bulk suppress, measured — one broadcast vs N durable signals
The bulk-control asymmetry above is now **wired end-to-end and measured**. Producer: the existing `POST /suppress`
already publishes the `ACTION_SUPPRESS:{target}` flag to `nba.definitions`; the Flink state machine broadcasts that
topic in, and on the flag's `false→true` transition `StateMachineFn` fans a fleet-cancel across every matching keyed
instance via `applyToKeyedState` (pre-send → `SUPPRESSED`; in-flight → `SUPPRESSING` + a `CANCEL` activation). No new
producer — the same operator action drives both engines. Classic does the same fan-out with a server-side Temporal
**Batch Operation** (`suppressMatching`: a `NbaActionId='X' AND ExecutionStatus='Running'` Visibility query → signal
`operatorSuppress`).

**Measured Flink fan-out** (operator test harness, **heap state backend = the engine's prod config**, single
sub-task, single thread; each key builds its `SUPPRESSING` fact + `CANCEL` activation JSON — the work *both* engines
do — and *excludes* the async Kafka production of those events, which is pipelined downstream off the fan-out path):

| in-flight instances | Flink fan-out (measured) | Temporal Batch Op @ the measured ~178/s server bound (§3) |
|---|--:|--:|
| 10,000 | **0.11 s** | ~56 s |
| 100,000 | **0.80 s** | ~9.4 min |
| 1,000,000 | **4.7 s** | ~1.6 h |

The Flink scan is linear (~5–11 µs/key across the range; JIT/GC variance, ~100–200k keys/s). The Temporal column is
**computed from the previously-measured ~178/s single-node server bound** (§3), used as a *generous* proxy for the
Batch Operation's per-workflow signal rate — each signal is a durable history append + workflow task + persist, the
same class of server-bound durable op, and real Batch-Operation RPS is often throttled *below* this. So the ~10³×
gap **favors Temporal in the framing** and is still three-plus orders of magnitude. Both scale horizontally (Temporal:
more history shards/nodes; Flink: more parallel sub-tasks, each fanning out its key-partition), so the asymmetry —
in-memory keyed-state scan vs N durable signals — holds at equal parallelism.

*Grounding — live-measured on the running classic stack (71,172 in-flight `ChannelActionWorkflow`s).* Firing the
**real Temporal Batch Operation** — `operatorSuppress` signalled over the `NbaActionId='action_reengage' AND
NbaChannel='push' AND ExecutionStatus='Running'` Visibility query, exactly what `suppressMatching` issues — against
**6,753** running workflows, the batch completed **~3,000 signals in ~3 min (≈17/s)** before throttle/worker-bound
stalling. That's *below* the §3 ~178/s server bound because Temporal's Batch Operation is deliberately RPS-throttled
to protect the cluster (configurable). At the measured ~17/s a 1M-instance suppress is **~16 h**; even at the generous
~178/s bound it's ~1.6 h — versus Flink's **4.7 s**. Two honest caveats from the live run: (1) the `POST /suppress`
*trigger* was **broken** — the temporal bridge that starts the `SuppressionWorkflow` was ~812k records behind on
`nba.member.facts` (workflow starts are serial gRPC at `NBA_BRIDGE_CONCURRENCY=1`, ~15/s) and operator suppress rode
that backlogged member stream, so I invoked the identical Batch Operation directly to measure the *mechanism*. **Now
fixed:** operator suppress rides the low-volume `nba.definitions` `ACTION_SUPPRESS` broadcast instead (an *action-level*
priority signal — the same pattern the Flink engine uses), and the bridge concurrency default is raised to 64 (the
~178/s plateau) so it keeps up. *(2)* post-send workflows don't leave `Running` on suppress — they emit a CANCEL and
await the cancel disposition/TTL — so the wall-clock to fully *drain* exceeds even
the signal time. Both make the live gap **wider** than the table, not narrower.

### KStreams — what it actually buys (measured), and what it doesn't
KStreams (`nba-decision-engine`) **materializes** the rules output — it does *not* reimplement rules, and everything
downstream (Temporal bridge, scorer, hot path — plus the suppress + concurrency fixes above) is **byte-for-byte the
classic path**. It differs on exactly one axis: the **snapshot/eligibility store** (Redis → RocksDB).

**Throughput is a wash — measured.** Replaying a 2.66M-record `nba.member.facts` backlog (single partition, shadow),
KStreams folds at **~2,058/s** vs the classic snapshot-builder's **~1,950/s**. Both are **EOS-commit-bound** (KStreams
`exactly_once_v2` @ 100 ms commit, ~59% CPU; classic per-batch transactions) — *not* store-bound; RocksDB-local vs
Redis-network write is **not** the limiter. So even with **Temporal *and* scorer scaled to infinity**, per-partition
throughput is identical and both scale out with partitions. **KStreams is not a throughput play.**

What it *does* buy — two real, non-throughput properties:
1. **No central-store ceiling — RAM *or* write-thread.** Classic's snapshot *is* Redis, so a single Redis is both a
   **RAM wall** (~1.7M @ 8 GB, then OOM) *and* a **single-thread write ceiling** (~80–100k ops/s on OSS / Azure Cache
   for Redis) that all lanes funnel into. KStreams' state is **RocksDB, partition-local** → writes *and* state scale
   **linearly with partitions** (no central store, no RAM wall). At our 10-partition / ~200M-day scale the write rate
   has ample headroom on either engine (§7), so this is a **growth hedge** — and a **Redis-flavor-independent** one:
   it doesn't care whether prod lands on Azure Cache for Redis or Managed Redis.
2. **Emit-exact recovery — no re-eval storm.** KStreams commits its **state changelog + the input offset in one EOS
   transaction**, so a state loss restores from the changelog and **resumes emit-exact**: only *genuinely changed*
   snapshots re-emit, so only real evals/scorings fire. Measured — a mid-replay restart re-emitted **+1,079**, not the
   +624k it had already built. Classic can't match this: its Redis write is a **dual-write *outside* the Kafka
   transaction**, so state and offset aren't atomic — even the smart rebuild (from the compacted `nba.snapshots` topic
   + delta replay) must replay a **conservative overlap** and **over-emit** it (`SnapshotBuilder:54` re-emits every
   touched member, *"NOT gated on whether Redis changed"*, because it can't tell a reprocess from a change) → a re-eval
   + re-score pass proportional to that overlap. It's a **resilience/cost** property, and **Flink shares it** (EOS keyed
   state) — classic is the one that pays, because its store sits outside the transaction.

*Read-model caveat:* the engine **as built** serves reads from **IQ** (local RocksDB, "no Redis") — fastest happy-path,
but IQ is fragile under load/failover (§2). The alternative keeps **Redis as the hot cache** with RocksDB as the
disk-backed authoritative store (IQ unused). Which wins depends on whether you trust IQ at your load.

### The honest scorecard
| | classic | KStreams | Flink |
|---|---|---|---|
| **decision-path processes** | ~7 (incl. Temporal worker + server, Debezium) | ~7 (swaps the snapshot store) | **1 job, no Temporal** |
| **removes the Temporal start bound?** | no | no | **yes** (state machine in-stream) |
| **state-machine dynamic state** | Temporal + **Postgres** (`channel_touch` / `nba_inflight`) | Temporal + Postgres (unchanged) | **in-stream keyed state** (no Temporal, no per-dispatch Postgres) |
| **reliable publish to Kafka** | outbox → **Debezium** CDC | outbox → Debezium (unchanged) | **direct EOS Kafka sink** (no outbox/CDC) |
| **single-instance ops** | Temporal: query / **reset / terminate any one workflow** + history UI | Temporal (unchanged) | domain **cancel (suppress) ✓**; no out-of-band reset/terminate or per-key history UI |
| **bulk / fleet-wide suppress** | Batch Operation — O(N) **durable** signals (~1.6 h / 1M @ measured bound) | Temporal (unchanged) | **broadcast → keyed-state fan-out** (wired via `POST /suppress`; measured **~4.7 s / 1M**) |
| **RAM wall on the source of truth** | yes (Redis-bound) | **no** (RocksDB) | **no** (disk/heap) |
| **state-build throughput** | ~1,950/s (1 part) → **~2,865/s** (1 builder, 2 part) measured | **~2,058/s** (1 part) → **~5,263/s** (2 inst, 2 part) measured — wash per instance, EOS-commit-bound, **scales ~linearly with partitions/instances** | in-stream (not the bound), ~2k/s per partition |
| **recovery re-eval** | **replay + over-emit** (Redis dual-write, not atomic w/ offset) | **emit-exact** (state+offset one EOS txn) | **emit-exact** (EOS checkpoint) |
| **hot-path reads** | Redis `HGETALL` **1.66 ms** (always-up, Redis-native) | **IQ (RocksDB) 1.53 ms — no Redis**; HA-tuned (KIP-535 stale reads + serve-from-standby, validated through a node kill), **latency-neutral vs Redis**; POC `com.sun` HTTP shim needs hardening for prod QPS | Redis write-through **1.66 ms** (== classic; Queryable State deprecated in 1.18, so it mirrors to Redis) |
| **earns its complexity for** | nothing — it's the simple default | member-state > economical RAM, Redis kept | **dispatch burst + operational consolidation + RAM wall** |

**Bottom line.** KStreams earns its keep on the **state tier** — disk-backed authoritative state (no RAM wall) with
**emit-exact recovery** (no re-eval storm) — and, as the campaign confirmed, it can serve the **hot-path read
straight from its own RocksDB state via IQ, dropping Redis entirely** (1.53 ms, latency-neutral vs Redis, and
HA through a node kill once the stale-read + standby tuning is on). It leaves the dispatch bound, the throughput
(~2,000/s per partition, a measured wash that scales ~linearly with partitions/instances — 2 inst/2 part
folded ~5,263/s), and the component count untouched. **Flink earns the most**: it gets *both*
of KStreams' state-tier wins (disk-backed state + EOS emit-exact recovery) **and** additionally **shrinks the
operational footprint** (≈7 processes → 1, dropping Temporal + the Postgres dynamic state + the Debezium/outbox tier)
and **removes a hard scale bound** (Temporal's ~178/s start path). The one thing Flink does *not* get is the
no-Redis read: Queryable State is deprecated in 1.18, so Flink write-throughs to Redis for hot-path reads (~1.66 ms,
same as classic) — **only KStreams drops Redis.** The trade is
real — you give up Temporal's **per-workflow durability, isolation, and interactive control** (*The Temporal trade*
above) — but because the same canonical events flow either way, **observability stays good**. So when the RAM / burst
/ dispatch math binds, the bigger lever is **Flink, not KStreams** — it attacks the dispatch ceiling and the service
sprawl at once (keeping Redis as the fast hot-read mirror); KStreams is the pick when dropping the Redis read tier
is itself the goal.
