package ai.das.nba.engine;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static ai.das.nba.engine.SnapshotLogic.*;

/**
 * The snapshot stage as a Kafka Streams Processor — the faithful port of snapshot-builder's per-batch
 * loop, run per-record. Forwards routes to the defs/firehose sinks and folds member facts into a keyed
 * state store by event-time LWW (the store's changelog replaces Redis as the durable source of truth;
 * the nba:snapshot Redis write becomes a best-effort MIRROR, authoritative-mode only).
 *
 * Children (wired in SpineTopology): "sink-defs", "sink-facts", "sink-snapshots".
 *
 * EMIT-ON-CHANGE — and why it's now CORRECT, not just leaner. snapshot-builder writes TWO unlinked systems
 * per batch: Redis (snapshot HSET) then Kafka (nba.snapshots emit), with no cross-system atomicity. After a
 * Redis-commit-then-Kafka-fail, the retry re-runs LWW against a Redis that already holds the values, so it
 * cannot tell a genuine no-change from a reprocess of a half-committed batch — it MUST re-emit every touched
 * member every batch (over-emit) to never lose an emission (that is exactly what "removes the need for an
 * outbox" there). KStreams collapses state-put + nba.snapshots emit + offset commit into ONE EOS_v2
 * transaction, so a reprocess is always a clean replay from an UNCOMMITTED offset (never half-applied). That
 * makes emit-ONLY-on-change provably correct: replaying an already-committed fact finds the state unchanged
 * (no emit — the prior commit already emitted it); a new fact changes state (emit). No outbox, no over-emit,
 * exactly-once. Downstream, fewer redundant snapshots = fewer wasted rules-engine evals (the same over-emission
 * its change-detect read existed to dampen).
 */
public class SnapshotProcessor implements Processor<String, String, String, String> {
    private static final Logger log = LoggerFactory.getLogger(SnapshotProcessor.class);
    private static final TypeReference<HashMap<String, String>> HASH = new TypeReference<>() {};

    /** Lean-filter fact set (union of rule factsUsed) — refreshed from nba:rulefacts by DecisionEngine's
     *  refresh thread, so the snapshot stays lean = exactly what the rules engine cares about. Empty = snapshot
     *  ALL (the fail-open default, matching snapshot-builder when nba:rulefacts is empty). */
    static volatile Set<String> RULE_FACTS = Set.of();

    /** Keys removed from nba:rulefacts (queued by the refresh thread); the prune punctuator sweeps them out of
     *  every stored snapshot. De-reference pruning — parity with snapshot-builder.pruneFacts. */
    static final Set<String> PENDING_PRUNE = ConcurrentHashMap.newKeySet();

    private final DecisionEngine.Conf cfg;
    private ProcessorContext<String, String> ctx;
    private KeyValueStore<String, String> store;
    private JedisPooled redis;
    private NbaIdResolver resolver;

    public SnapshotProcessor(DecisionEngine.Conf cfg) { this.cfg = cfg; }

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.ctx = context;
        this.store = context.getStateStore("nba-snapshot-store");
        this.redis = new JedisPooled(cfg.redisHost, cfg.redisPort);
        // NBAID resolve via the SHARED Redis id-map (read + race-safe setnx) — identical to snapshot-builder, so the
        // engine derives the SAME nbaId as the classic path (the idmap is shared infra; setnx is idempotent).
        this.resolver = (et, id) -> resolveNbaId(redis, et, id);
        // De-reference pruning runs on the STREAM THREAD (safe store access) via a wall-clock punctuator: when
        // nba:rulefacts shrinks, the refresh thread queues the removed keys and this sweeps them out of state.
        context.schedule(Duration.ofSeconds(15), PunctuationType.WALL_CLOCK_TIME, ts -> pruneDereferenced());
    }

    @Override
    public void process(Record<String, String> rec) {
        Metrics.counter("nba_engine_facts_in_total").increment();
        long t0 = System.nanoTime();
        JsonNode value;
        try { value = SnapshotLogic.M.readTree(rec.value()); }
        catch (Exception e) {
            Metrics.counter("nba_engine_parse_errors_total").increment();
            ctx.forward(dlqEnvelope(rec, "deserialize failed (unparseable JSON)"), "sink-dlq");   // replayable poison envelope
            return;
        }

        String kind = header(rec, "kind");
        Classified c = classifyOne(new Parsed(rec.key(), value, kind, rec.value()), RULE_FACTS, resolver);

        for (Forward fwd : c.forwards()) {
            String child = fwd.route() == Route.DEFS ? "sink-defs" : "sink-facts";
            Record<String, String> out = new Record<>(fwd.key(), fwd.value(), rec.timestamp());
            if (fwd.headerKind() != null)
                out = out.withHeaders(new RecordHeaders(new Header[]{
                        new RecordHeader("kind", fwd.headerKind().getBytes(StandardCharsets.UTF_8))}));
            ctx.forward(out, child);
        }

        SnapFact sf = c.snap();
        if (sf != null) {
            Map<String, String> hash = loadHash(sf.nbaId());
            boolean changed = applyLww(hash, sf, rec.timestamp());     // event-time LWW (informational __updatedTs = stream time)
            if (changed) {
                store.put(sf.nbaId(), serialize(hash));                // the state store IS the snapshot now — NO Redis write
                try {
                    ctx.forward(new Record<>(sf.nbaId(), buildSnapshotJson(sf.nbaId(), hash), rec.timestamp()), "sink-snapshots");
                    Metrics.counter("nba_engine_snapshots_emitted_total").increment();
                } catch (Exception e) { log.warn("snapshot emit failed for {}", sf.nbaId(), e); }
            }
        }
        Metrics.timer("nba_engine_snapshot_seconds").record(System.nanoTime() - t0, TimeUnit.NANOSECONDS);
    }

    private Map<String, String> loadHash(String nbaId) {
        String s = store.get(nbaId);
        if (s == null) return new HashMap<>();
        try { return SnapshotLogic.M.readValue(s, HASH); } catch (Exception e) { return new HashMap<>(); }
    }

    private String serialize(Map<String, String> hash) {
        try { return SnapshotLogic.M.writeValueAsString(hash); } catch (Exception e) { throw new RuntimeException(e); }
    }

    /** Sweep de-referenced fact fields (keys dropped from nba:rulefacts) out of every stored snapshot and re-emit
     *  the changed members. On the stream thread (punctuator) so store access is safe; collect-then-apply so we
     *  never mutate the store mid-iteration. NOTE: store.all() is a full LOCAL scan — fine at POC scale; at large
     *  scale this wants to be bounded/incremental (parity with snapshot-builder.pruneFacts, which SCANs Redis). */
    private void pruneDereferenced() {
        if (PENDING_PRUNE.isEmpty()) return;
        Set<String> removed = new HashSet<>(PENDING_PRUNE);
        PENDING_PRUNE.removeAll(removed);
        List<String[]> rewrites = new ArrayList<>();                       // [nbaId, newHashJson]
        try (KeyValueIterator<String, String> it = store.all()) {
            while (it.hasNext()) {
                var kv = it.next();
                Map<String, String> hash;
                try { hash = SnapshotLogic.M.readValue(kv.value, HASH); } catch (Exception e) { continue; }
                boolean changed = false;
                for (String k : removed) if (hash.remove("fact:" + k) != null) changed = true;
                if (changed) rewrites.add(new String[]{kv.key, serialize(hash)});
            }
        }
        for (String[] rw : rewrites) {
            store.put(rw[0], rw[1]);
            try {
                Map<String, String> hash = SnapshotLogic.M.readValue(rw[1], HASH);
                ctx.forward(new Record<>(rw[0], buildSnapshotJson(rw[0], hash), ctx.currentSystemTimeMs()), "sink-snapshots");
            } catch (Exception e) { log.warn("prune re-emit failed for {}", rw[0], e); }
        }
        if (!rewrites.isEmpty()) {
            log.info("pruned {} de-referenced fact(s) from {} member(s)", removed.size(), rewrites.size());
            Metrics.counter("nba_engine_pruned_total").increment(rewrites.size());
        }
    }

    /** Replayable DLQ envelope for a poison record — source coordinates + raw value + error (mirrors
     *  snapshot-builder.dlqEnvelope), keyed by the original key so a replay keeps partition affinity. */
    private Record<String, String> dlqEnvelope(Record<String, String> rec, String error) {
        ObjectNode env = SnapshotLogic.M.createObjectNode();
        env.put("consumer", "nba-decision-engine");
        ctx.recordMetadata().ifPresent(m -> { env.put("topic", m.topic()); env.put("partition", m.partition()); env.put("offset", m.offset()); });
        if (rec.key() != null) env.put("key", rec.key()); else env.putNull("key");
        env.put("value", rec.value());
        env.put("error", error);
        env.put("dlqTs", ctx.currentSystemTimeMs());
        String body;
        try { body = SnapshotLogic.M.writeValueAsString(env); } catch (Exception e) { body = rec.value(); }
        return new Record<>(rec.key(), body, rec.timestamp());
    }

    static String header(Record<String, String> r, String name) {
        Header h = r.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    /** NBAID resolve — read the shared id-map; race-safe setnx mints one when absent (first writer wins). */
    static String resolveNbaId(JedisPooled redis, String entityType, String entityId) {
        String idmapKey = "nba:idmap:" + entityType + ":" + entityId;
        String existing = redis.get(idmapKey);
        if (existing != null) return existing;
        String nbaId = "nba_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        long set = redis.setnx(idmapKey, nbaId);
        return set == 1 ? nbaId : redis.get(idmapKey);
    }

    @Override
    public void close() { /* JedisPooled has no close hook needed here */ }
}
