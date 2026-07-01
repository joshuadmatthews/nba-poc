# Load test — Redis (classic) vs Kafka Streams engine (RocksDB + IQ), under max load

Local single-box podman, NBA POC. Goal: settle definitively whether the KStreams engine (RocksDB state +
Interactive-Query reads) beats the classic snapshot-builder + Redis at scale — on **write throughput**,
**read latency under write load**, and **behaviour at the ceiling**.

Harness (reusable): `loadgen.py` (producer), `loadtest-clean.sh` (isolated drain), `loadtest-isolated.sh`
(no-flywheel write rate), `loadtest-reads.sh` + `iqprobe.py` (reads under write load).

> **⚠️ UPDATE (cross-flavor campaign — see §8).** Sections 2/4/6b and the original Verdict below concluded
> *"IQ is a liability, keep Redis, don't drop it."* That was correct **for the IQ surface as built** (untuned:
> redirect-to-dead-active → 100% 500 during rebalance). A later clean-start campaign **tuned IQ for
> availability** (KIP-535 stale reads + serve-from-standby) and re-measured on a 2-partition stack. The verdict
> flipped: **IQ reads are latency-neutral vs Redis (1.53 vs 1.66 ms) and stay up through a node kill — so
> KStreams *can* drop Redis.** §8 has the current numbers and supersedes the read-surface conclusions below.
> The state/RAM-wall/burst findings (§3/§7) are unchanged.

## Methodology notes (what was wrong before it was right)
- **The first pass was invalid.** `rpk topic produce` from a piped stdin batches the whole stream into one
  produce request that exceeds Redpanda's `kafka_batch_max_bytes` (1 MiB) → `MESSAGE_TOO_LARGE`; only ~40 of
  each 50k burst actually landed. Every "≈180k facts/s, dead heat" number from that pass measured
  produce-failure + poll overhead, **not** processing. Fixed by producing via kafka-python with a bounded
  `batch_size` (`loadgen.py`) and verifying the topic high-watermark grew by N every run.
- **Stop/start isolation is fragile.** Abruptly stopping a front-end mid-drain and restarting it leaves the
  consumer group rejoining / KStreams recovering during the measurement window → spurious 0 facts/s. The
  honest write-rate is the **warm, both-running** steady-state.
- Single partition on `nba.member.facts` ⇒ these are **per-instance** numbers; horizontal scaling (partitions
  × pods, sharded RocksDB vs one central Redis) is structural and not reproducible on one box.

## 1. Write throughput — NOT the differentiator locally
Warm steady-state, both running, flywheel active (realistic): **classic ≈2.7k facts/s, engine ≈2.1k facts/s** —
comparable, low-thousands/s, gated by the downstream flywheel (rules→scorer→router→temporal reacting to every
snapshot) and the modest box, **not** by the snapshot front-end. Redis sat at <5% of its measured ceiling
(`redis-benchmark`: SET 86k/s, GET 77k/s, HSET 82k/s, p50 ~0.3ms). Neither front-end is the bottleneck here.

## 2. Read latency under MAX write load — Redis wins clearly (the hot path)
| read path | idle p50 | under-write-load p50 | p99 | concurrency |
|---|---|---|---|---|
| **Redis HGETALL** | 0.25ms | **0.25ms (unchanged)** | 0.8ms | -c 10, 80k ops/s, fine |
| **IQ /snapshot (RocksDB)** | 2.4ms (serial) | ~3ms | 8–23ms | **degrades: 8.4ms p50 @ C=10** |

- **Redis reads are ~10× faster and completely unaffected by heavy concurrent write load.** It serves reads
  fine even *at* `maxmemory` (OOM only blocks writes — a sampled HGET succeeded at the cap).
- **IQ** is single-digit-ms when healthy but the POC surface is a single-threaded `com.sun` HttpServer:
  latency grows ~3.5× from serial → C=10 (it doesn't scale concurrency). Production would need a real
  server + routing-aware client + standby replicas.

## 3. The ceiling — Redis hit a HARD RAM WALL (the real scale finding)
Under sustained load the central Redis (`maxmemory 256MB`, `noeviction`) **filled at ~55k members
(~4.6KB/member) and began rejecting ALL writes** — `OOM command not allowed when used memory > 'maxmemory'`.
That:
- **crashed the KStreams engine** — its one remaining Redis dependency (the `nba:idmap` setnx) threw, and the
  Streams exception handler did `SHUTDOWN_CLIENT` → the app went to **ERROR** with no auto-recovery;
- would equally fail snapshot-builder's `HSET`.

This is the central-RAM ceiling the whole exercise was about, hit empirically. **RocksDB (the engine's snapshot
state) is disk-backed and has no such wall** — after the crash the engine recovered its state locally in ~4s;
it only died because of its *Redis* coupling, not its own state store.

## 4. Availability — Redis is independent infra; IQ lives and dies with the stream app
The engine went to ERROR repeatedly under load (Redis-idmap OOM; and replay storms when restarted against a
large backlog). **Every time, 100% of IQ reads returned HTTP 500 with no recovery** until a manual restart
against a trimmed backlog. Redis, by contrast, is a separate process that stayed up and kept serving reads
throughout. For a hot-path read store this is a decisive operational difference.

## 5. Resource
Engine resident memory **~2.7–3.4× snapshot-builder** (≈720MB vs ≈260MB warm) + RocksDB state on disk.

## 6. Full per-stage throughput matrix — all 3 architectures, constraints removed
The earlier sections compared the *snapshot front-end* only. This pass isolates **every spine stage**
(snapshot → rules → router → state-machine) across **classic / KStreams / Flink**, warm, draining a standing
backlog, with the safety constraints lifted (debounce 60→5s, conversion-sim 0.4→1.0, throttles raised) so each
layer runs unthrottled. Per-instance, single partition.

| stage | classic | KStreams | Flink |
|---|---|---|---|
| **snapshot build** | 2,561/s — Redis HSET (network RTT) | **6,951/s** — RocksDB (local disk) | **≥20,000/s** — heap state |
| **rules eval** | 3,326/s — Drools, one KieSession/eval | — (engine = snapshot only) | **20,709/s** — in-JVM condition-tree flatMap |
| **action router** | 3,455/s | — | ~20k* — trivial per-record flatMap |
| **state machine** | **Temporal 15 → ~180/s** — serial client 15/s; parallel bridge plateaus ~180/s (server-bound) | — | runs (~15 cores @ 300k); not lag-measurable* |

Readings:
- **The classic spine is uniform at 2.5–3.5k/s/instance.** Snapshot 2.56k, rules 3.33k, router 3.46k — the
  limit is the JVM consumer + per-record work + Redis round-trip, the *same* ceiling at every stage on one
  pod / one partition. Not stage-specific; it scales out with partitions × pods.
- **Flink's stateless transforms are ~6× the classic equivalents** (rules 20.7k vs Drools 3.3k): it replaces a
  KieSession-per-eval with an in-JVM condition-tree flatMap over heap state and fans across task slots. The
  router is the same trivial per-record pick.
- **The original 12/s was a CLIENT artifact, not a Temporal rate limit** (see §6a). The bridge issued
  `WorkflowClient.start` serially on one consumer thread (each start a blocking gRPC ~70ms → ~12–15/s).
  Parallelizing the start path (`NBA_BRIDGE_CONCURRENCY`) lifts it to **~180/s on the same single-node box**,
  where it **plateaus** — worker CPU idle (~25%), Temporal-server CPU ~2 cores, **zero `RESOURCE_EXHAUSTED`**.
  So the ceiling is the single-node `start-dev` server's real start/persist/index throughput, **not** a
  configured RPS limit and **not** the throttle. It still scales horizontally in prod (history shards + real
  persistence); ~180/s is the **dev-box server floor**, and the spine's bottleneck for the once-a-day burst.
- **The Flink state machine is feature-complete with the Temporal path** (ported `ThrottleGate` per-channel
  token-bucket + member-keyed `debounceLost` dedup; build green, committed). It processes 300k CREATEs at
  ~15 cores. *Its throughput could not be read off Kafka consumer lag* — Flink commits checkpoint offsets that
  diverge from the group's committed offset, and the sm self-loops on `member.facts.shadow` (reads and writes
  the same topic), so lag grows even while it works. Quantifying it needs Flink's own `numRecordsIn` metric;
  functionally it is verified. (`*` = same caveat for the Flink router lag reading.)

### 6a. Temporal start throughput — the 12/s was a serial client, not a rate limit
Pushed on whether 12/s was a relaxable Temporal limit, I isolated the start path. Two findings:

1. **The bridge started workflows serially.** `runBridge` looped a single Kafka consumer and called the
   **blocking** `WorkflowClient.start` one record at a time — each a ~70ms gRPC round-trip → ~12–15/s,
   regardless of Temporal's capacity. Fixed by fanning the per-record starts across a pool
   (`NBA_BRIDGE_CONCURRENCY`, default 1 = legacy serial; starts are idempotent — deterministic workflowId +
   USE_EXISTING conflict policy — and the batch still awaits before commit, preserving at-least-once).

2. **A latent env regression masqueraded as the cap.** The workflows set two *custom* search attributes at
   start (`NbaActionId`, `NbaChannel`). `start-dev` is in-memory, so a server restart (the VM recovery) dropped
   the registration → **every** start failed `INVALID_ARGUMENT('search attribute not defined')` and silently
   DLQ'd. Re-test with concurrency but unregistered SAs showed a phantom "2,466/s" that was 100% **failures**
   (0 `activate`, Temporal workflow count 0, 81k DLQ). Registering the SAs (now wired into
   `run-nba-temporal.ps1` so a restart self-heals) made starts real.

Concurrency sweep, real successful starts (verified against Temporal's own `workflow count`):

| `NBA_BRIDGE_CONCURRENCY` | successful starts/s | worker CPU | temporal-server CPU | DLQ |
|---|---|---|---|---|
| 1 (serial — the old "12/s") | ~15/s | low | low | 0 |
| 64 | ~178/s | ~23% (idle) | — | 0 |
| 128 | ~170/s | ~25% (idle) | ~203% (~2 cores) | 0 |

**Verdict:** 12/s was the serial client. Parallel starts give **~12–15×** (→ ~180/s) on the same box, then
**plateau** — the worker sits idle while the single-node `start-dev` server saturates ~2 cores. No
`RESOURCE_EXHAUSTED`, no configured RPS limit hit: the ceiling is genuine single-node server start/persist/index
capacity, which scales out in prod. (The throttle was never the gate — it limits *sends* after debounce, not
starts.)

### 6b. Redis is an inbound cache for the CLASSIC path only (not apples-to-apples)
The architectures deliberately differ in hot-path Redis usage, so the classic↔engine gap reflects **both**
engine speed and state-store locality:

| | classic | KStreams | Flink |
|---|---|---|---|
| idmap resolve (new member) | Redis `get`/`setnx` | Redis `get`/`setnx` | Redis `get`/`setnx` |
| snapshot LWW (per fact) | Redis `HGET` (pipelined) | **RocksDB local** | **Flink MapState local** |
| eligibility change-detect (per eval) | Redis `GET` | **keyed state** | **keyed ValueState** |
| router maxbatch / eligibility | Redis `HGET` / `SET` | local + IQ | **Flink state** (optional write-through) |
| **net Redis ops / record (hot path)** | **~4–5** | **~1–2 (idmap only)** | **~1–2 (idmap only)** |

So **Redis is an inbound cache only for the classic path.** KStreams/Flink touch Redis *only* for idmap
resolution (new members; shared by all three) and an *optional* write-through mirror — snapshot LWW and
change-detect run entirely on local state. This is by design (moving state off central Redis is the whole point
of the disk-backed engines), but it means part of the classic stages' lower throughput is the per-record Redis
round-trip, not only Drools. The idmap is the one Redis touchpoint common to all three.

**This is really the read-surface decision (ties to §2/§4) — and the engine numbers above were measured with
the read cache OFF.** Moving state off Redis forces a choice for the synchronous hot-path read
(`getSnapshot`/eligibility), and the tree has both designs:
- **KStreams → Interactive Query** (`IqServer`, a `com.sun` HttpServer over the RocksDB stores; the code says
  it *"replaces the nba:snapshot + nba:eligibility Redis reads"* and is *"a POC read surface"*). §4 showed IQ is
  a **liability**: 100% HTTP 500 during engine ERROR/rebalance/recovery; §2: single-threaded, degrades 3.5×
  from serial→C=10. Not viable as a hot-path read surface as built.
- **Flink → Redis write-through** (`redisWriteThrough`, default **true** in authoritative): compute on local
  state, mirror snapshot+eligibility back to Redis so reads stay on Redis (0.25ms, unaffected by write load,
  independent/always-up — §2).

So **yes — Redis should stay the hot-path read cache for all three; the IQ failures (§4) are exactly why not to
drop it.** The engine's job is to shed the *state* RAM-wall (§3) and shard the *write* burst (§7); Redis remains
the fast/available *read* mirror (ideally TTL'd to hot members so the mirror doesn't reproduce the wall).

**Caveat on §6's engine numbers:** they were measured in **shadow mode with write-through OFF**
(`redisWriteThrough` is gated on authoritative; the test drove `.shadow` topics) — pure local-state compute,
**zero Redis writes**. The authoritative deployment that feeds the Redis read cache adds a Redis `HSET`/`SET`
per record to the snapshot + router stages, so the real *with-cache* engine throughput sits **below** the
20k/≥20k local-compute ceiling and narrows the gap to classic — though the write-through can be
async/pipelined/best-effort and Redis sharded, unlike classic's synchronous inline `HSET`.

**Authoritative-mode (write-through ON) result — the "with read cache" figure.** Running Flink authoritative
exposed two things. (1) Kafka-lag throughput is **unmeasurable** in authoritative mode for the same reason as
the state machine: the full spine self-loops on `nba.member.facts` (the scorer + state machine write facts
back), so the input topic grows faster than it drains. (2) Driving 200k members **reproduced the §3 Redis RAM
wall** — the per-record write-through filled the 256MB Redis and writes began failing. But the number itself is
already in hand: **the classic snapshot-builder, 2,561/s, _is_ the snapshot-with-write-through measurement** —
it does the identical per-record `redis.hset(snapKey, …)` and is Redis-`HSET`-bound. Flink-authoritative issues
the same `hset`, so a single slot converges to the same **~2.5–3k/s Redis-write bound**, not the 20k
local-compute ceiling. **Conclusion: turning the Redis read cache on (write-through) collapses single-instance
snapshot throughput from ~20k to the ~2.5–3k/s Redis-write rate.** The engine's advantage _with_ the read cache
is therefore (a) **parallelism** — N slots/pods × ~3k/s against a **sharded** Redis — and (b) shedding the
*state* RAM-wall by keeping full state local while the Redis mirror is TTL'd to hot members; it is **not** a
single-slot speedup. The IQ alternative (no write-through) keeps the 20k but fails the read surface (§4). That
is the core architecture tradeoff this whole exercise surfaces.

## 7. Why this matters — the 8M/day bulk-score burst
The scenario that justifies the whole comparison: bulk scoring drops **~8M scores once a day**, and the system
must absorb them *fast* so it can keep reacting to live facts on the pipe. At the classic ~2.5–3.5k/s/instance,
a single pod drains 8M in **~40–53 min of serial backlog**, during which live dispositions queue behind the
burst. The disk-backed engines do two things the classic path can't: (1) they shed the central-RAM wall
(section 3 — Redis OOM'd system-wide at ~55k members), and (2) they **partition the burst across slots/pods** —
N-way parallelism turns ~50 min into minutes and keeps the live-fact path responsive. This is the structural
case for KStreams/Flink that the at-this-scale latency/throughput numbers (sections 1–2) deliberately did not
make.

## 8. Cross-flavor campaign — clean-start, 2 partitions, per-flavor read model (supersedes §2/§4/§6b read verdict)
Goal: a **published-quality** characterization of each flavor's hot path, with proper hygiene. Two method fixes
over the earlier passes:
- **Clean start per test** — every run begins on empty topics/DLQs and a flushed Redis (compacted state topics
  delete+recreated, data topics trimmed to the edge, `flushdb`), so no run inherits another's backlog or cache.
- **2 partitions, 2 instances** — the co-partitioned keying (`create-topics.ps1`, members hash to stable
  partitions) run at real parallelism, not the single-partition per-instance floor of §1/§6.
- **App-representative reads over the podman bridge** — the read probe does the *full* `HGETALL` of the
  snapshot hash (classic) or the IQ HTTP `GET /snapshot/{id}` (KStreams), each across the bridge, one network
  hop — the way the app actually reads. (This supersedes §2's 0.25 ms, which was a raw single-key
  `redis-benchmark` GET, not the app's HGETALL over the network.)

### The IQ availability fix (why the §4 "liability" verdict flipped)
§4's 100% HTTP 500 during rebalance/recovery was **not** the RocksDB store failing — it was `IqServer`
redirecting reads to the *active* host for a key even when that host had just died. Two changes make the read
surface ride through a failover (`IqServer.java:78,88`):
- **serve-from-standby** — a pod that holds a **standby** replica of a key answers from its warm copy instead
  of 307-redirecting to a possibly-dead active (`md.standbyHosts().contains(self)`), backed by
  `num.standby.replicas`;
- **KIP-535 stale reads** — `enableStaleStores()` lets a pod serve a partition that is still *restoring*
  (slightly stale) rather than throwing `InvalidStateStoreException`.

Validated: **200 reads straight through a rolling instance kill**, zero 500s. The store was always fine; the
availability bug was the redirect target.

### Per-flavor result (clean-start, 2 part)
| flavor | state-build fold | hot-path read store | read p50 (p95) | Redis needed? |
|---|---|---|---|---|
| **classic** | ~2,865/s (1 builder, 2 part) | Redis `HGETALL nba:snapshot:{id}` | **1.66 ms** (~14.4k rps) | **yes** (authoritative read store) |
| **KStreams** | **~5,263/s** (2 inst, 2 part) | IQ → local RocksDB (HTTP) | **1.53 ms** (3.12 ms) | **no** — served from own state |
| **Flink** | ~2k/s per partition (in-stream) | Redis write-through mirror (`RouterFn`) | **1.66 ms** (== classic) | **yes** — Queryable State deprecated in 1.18 → mirrors to Redis |

### Findings
- **Hot-path read latency is a wash (~1.5–1.7 ms across all three)** measured apples-to-apples over the bridge.
  The read *store* is the differentiator, not the read *speed*.
- **KStreams uniquely drops Redis.** IQ reads are latency-neutral vs Redis (1.53 vs 1.66 ms) and now HA through
  a node kill — so the classic Redis read tier can be removed entirely on this flavor. *Caveat: the read
  surface is still the POC `com.sun` `HttpServer` (single-threaded, §2's ~3.5× degradation at read-concurrency
  10 stands). The **availability** problem is fixed; the **concurrency** problem needs a real server
  (Netty/Javalin) + routing-aware retrying client for production read QPS. The state store is production-grade;
  the HTTP shim is not.*
- **Flink cannot drop Redis** despite holding the same keyed RocksDB state: Flink Queryable State is deprecated
  as of 1.18 (our version), so the engine is deliberately built to **write-through to Redis** on the
  authoritative path (`RouterFn`, `Conf.redisWriteThrough`) and read from the mirror — ~1.66 ms, same as
  classic. This directly answers "same RocksDB hot path for Flink too?": conceptually yes, but the supported
  path in 1.18 is the Redis mirror, so **only KStreams gets the no-Redis read**.
- **Fold throughput scales ~linearly with partitions/instances** and remains EOS-commit-bound per instance
  (~2–2.9k/s): classic 1 builder / 2 part ~2,865/s; KStreams 2 inst / 2 part ~5,263/s (~2,630/s each). Not a
  single-instance speedup — a partition/parallelism play, consistent with §6/§7.

**Net for the published POC:** the read latency claim in §2 ("Redis ~10× faster") does **not** hold
app-representatively — it's a wash. The real read-model story per flavor is: **classic = Redis (native),
KStreams = IQ/RocksDB (no Redis, HA-tuned, latency-neutral), Flink = Redis write-through (QS deprecated).**

## Verdict (evidence-based; read-surface bullets updated per §8)
- **~~At this scale: keep Redis. Faster reads (10×)…~~** — *superseded by §8.* App-representative reads are a
  **wash** (Redis 1.66 ms vs IQ 1.53 ms), not 10×; the §2 0.25 ms was a raw `redis-benchmark` GET, not the
  app's HGETALL over the network. Redis is still the right default for **classic** (native) and required for
  **Flink** (Queryable State deprecated in 1.18 → write-through), but **KStreams can drop Redis**: IQ is
  latency-neutral and HA once tuned (stale reads + standby). Keep Redis where the flavor needs it; it is no
  longer a *latency* argument.
- **KStreams/Flink "takes the lead" specifically on STATE-SIZE economics and write-path scaling, NOT
  latency/throughput.** Redis is RAM-bound with a hard wall (demonstrated: OOM at 55k members / 256MB →
  system-wide write failure); RocksDB spills to disk and shards per-partition with no central write
  bottleneck. The crossover is "member state exceeds economical RAM" or "write QPS saturates one Redis," not
  "we want lower latency."
- **If KStreams drops Redis, the read surface needs two fixes — one now done, one still open (§8).** The
  **availability** half is fixed: standby replicas + KIP-535 stale reads keep IQ serving through a node
  kill (validated, 200 reads clean). The **concurrency** half remains: the POC `com.sun` HttpServer is
  single-threaded (degrades ~3.5× at C=10) — production read QPS wants a Netty/Javalin server + routing-aware
  retrying client. Also: the engine must not hard-depend on Redis for anything else (the `idmap` coupling is
  what let Redis's OOM take the stream app down). Flink keeps Redis regardless (QS deprecated in 1.18).
- **Across the full spine (section 6): the compute stages are not the problem — the state store and the
  workflow engine are.** Snapshot/rules/router all sit in the low-thousands/s on classic and ~20k/s on Flink;
  the two things that actually bound the system are (a) Redis's central-RAM wall on the hot store and (b)
  Temporal's dev start/persist pipeline (12/s). The right shape is therefore the hybrid the testing converges
  on: **disk-backed partitioned compute (KStreams/Flink) to absorb the bulk burst + scale state, Redis kept as
  the fast hot-path read store, and Temporal scaled out (or its start-path offloaded) for the once-a-day spike.**
  The Flink spine is now feature-complete with the Temporal path (throttle + dedup ported) and is a viable
  drop-in for the async layers; the remaining unknown is its raw state-machine throughput, which needs Flink's
  internal metrics rather than Kafka lag to measure.
