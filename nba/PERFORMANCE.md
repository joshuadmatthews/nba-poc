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

1. **Redis RAM wall.** Under sustained load the central Redis (`maxmemory 256 MB`, `noeviction`) filled at
   **~55k members (~4.6 KB/member)** and rejected **all** writes (`OOM command not allowed`) — system-wide write
   failure, and it took the KStreams engine down through its one remaining Redis dependency (the idmap setnx).
   RocksDB / Flink state is **disk-backed → no wall** (the engine recovered its local state in ~4 s after a crash).
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
