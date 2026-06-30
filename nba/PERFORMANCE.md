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

| read path | idle p50 | under max write load | p99 | concurrency |
|---|---|---|---|---|
| **Redis HGETALL** | 0.25 ms | **0.25 ms (unaffected)** | 0.8 ms | scales fine (−c 10, ~80k ops/s) |
| **IQ /snapshot (RocksDB)** | 2.4 ms | ~3 ms | 8–23 ms | **degrades 3.5× to ~8 ms @ C=10**; 100% HTTP 500 during rebalance/recovery |

Redis reads are ~10× faster, flat under write load, and live in independent infra that stays up. The KStreams
Interactive-Query surface is a **liability as built** (single-threaded `com.sun` HttpServer, dies with the stream
app). `redis-benchmark` ceiling for reference: SET 86k/s, GET 77k/s, HSET 82k/s, p50 ~0.3 ms.

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
- **Redis kept as the fast, always-up hot-path read cache** (because IQ fails), fed by write-through and ideally
  **TTL'd to hot members** so the mirror doesn't re-create the RAM wall;
- **Temporal scaled out** (more history shards / parallel starts) for the daily action-dispatch spike.

**Classic stays the right call** until member-state exceeds economical RAM, or write QPS saturates one Redis, or
the daily burst can't drain in time — at which point the disk-backed engine earns its complexity, with Redis
retained purely as the read surface.

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

---

## 8. What each flavor actually earns — components, Temporal, and the Redis-hot-path question

Throughput (§1, §6) is the *smallest* of the differences. The real trade is **hosted-component count, the Temporal
start-rate bound, and what disk-backed state buys when Redis stays the hot path.**

### Component count — Flink collapses the decision path into one job
The classic decision path is **six hosted services**: snapshot-builder, rules-engine (Drools), action-router,
**temporal-worker + the Temporal server** (the state machine), and action-layer. And the state machine doesn't live
in Temporal alone — it **leans on Postgres** for its dynamic per-member state: the per-(member,channel) **send/touch
counter** (`channel_touch`, bumped atomically at each dispatch), the **in-flight/dispatch tracker** (`nba_inflight`),
and the transactional **outbox** (the shared `actionlib` DB). On top sit the shared scorer and the shared infra
(Redpanda, Redis, the action-library, the BFF). **Flink is ONE job** that wires all of it (`SpineJob`: snapshot →
rules → score → route → **state-machine → action-layer**). So Flink is *internally* more complex (one large stateful,
checkpointed job) but **operationally simpler — ~6 processes → 1** — and the headline: **it deletes Temporal** (worker
+ server; here an embedded DB, in prod a whole frontend/history/matching cluster + its database) *and* folds the
state machine's Postgres-backed dynamic state into the stream (below). KStreams does **not** consolidate: it adds one
service (the decision-engine) and
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
keeps in Postgres, folding dispatch + throttle + send-counts + dedup into one keyed stream. **That is the scale
unlock: it removes a single-node start bound *and* a per-dispatch database write at once.** (Postgres still backs the
action *catalog*, shared by both flavors — Flink reduces the Postgres dependency, it doesn't eliminate it.) The cost
is real and worth stating: one big stateful job means a hot key or a bug can stall the whole spine, scaling means
repartitioning the entire job, and recovery is one large checkpoint — versus classic's services that scale and fail
independently.

### KStreams, assuming Redis stays the hot path — what it actually buys
Keep **Redis as the hot-path read cache** (its real strength, §2) and KStreams' own read surface (IQ) goes unused —
fine, since IQ is fragile under load (§2). So KStreams' *only* remaining contribution is on the **write/state side**:
the authoritative member-state lives in **RocksDB (disk-backed, no RAM wall, sharded per partition)** while Redis is
demoted to a **write-through, TTL'd hot-member cache**. Classic structurally can't do this — in classic the snapshot
*is* Redis, so Redis must hold **every** member (the RAM wall). A disk-backed engine (KStreams or Flink) is what lets
you keep tens of millions of members on disk while Redis caches only the hot working set. **That is the entire value
of KStreams when Redis stays the hot path: it decouples total state-size from cache-size.** It does not touch the
rules, the scorer, or the Temporal bound — a narrow state-economics play, worth it only once member-state outgrows
economical Redis RAM.

### The honest scorecard
| | classic | KStreams | Flink |
|---|---|---|---|
| **decision-path processes** | ~6 (incl. Temporal worker + server) | ~6 (swaps the snapshot store) | **1 job, no Temporal** |
| **removes the Temporal start bound?** | no | no | **yes** (state machine in-stream) |
| **state-machine dynamic state** | Temporal + **Postgres** (`channel_touch` / `nba_inflight`) | Temporal + Postgres (unchanged) | **in-stream keyed state** (no Temporal, no per-dispatch Postgres) |
| **RAM wall on the source of truth** | yes (Redis-bound) | **no** (RocksDB) | **no** (disk/heap) |
| **hot-path reads** | **Redis (fast, always-up)** | Redis (IQ unused) | needs Redis write-through |
| **earns its complexity for** | nothing — it's the simple default | member-state > economical RAM, Redis kept | **dispatch burst + operational consolidation + RAM wall** |

**Bottom line.** KStreams earns its keep **only** as a state-size play — disk-backed authoritative state behind a
Redis hot cache; it leaves the dispatch bound and the component count untouched. **Flink earns the most**: it is the
one flavor that both **shrinks the operational footprint** (≈6 processes → 1) and **removes a hard scale bound**
(Temporal's ~178/s start path). So when the RAM / burst / dispatch math finally binds, the bigger lever is **Flink,
not KStreams** — it attacks the dispatch ceiling and the service sprawl at once, with Redis retained purely as the
fast hot-read cache.
