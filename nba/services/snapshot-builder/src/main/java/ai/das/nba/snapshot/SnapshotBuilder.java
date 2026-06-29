package ai.das.nba.snapshot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Response;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * NBA snapshot builder — BATCHED + TRANSACTIONAL.
 *
 * The member-facts channel is high-volume, so we do NOT evaluate one fact at a time. Each poll
 * yields a BATCH of mixed content; one pass classifies the whole batch into three sinks and a set
 * of member snapshots, then commits it with two transactions:
 *
 *   1) ROUTE  — population-wide throttle levels and operator action-suppressions go to nba.definitions
 *               (broadcast); internally-born facts (a "kind" header) are firehosed to nba.facts.
 *   2) SNAPSHOT — every OTHER member fact is folded into its member's Redis snapshot hash
 *               (event-time last-writer-wins per fact key), keyed by NBAID.
 *
 * Atomicity & retry:
 *   - ALL Redis writes for the batch are applied in ONE Redis transaction (MULTI/EXEC).
 *   - ALL Kafka produces for the batch (routes + firehose + snapshots) plus the consumer offset
 *     commit are ONE Kafka transaction (read-process-write EOS).
 *   - On any failure we abort the Kafka txn, do NOT commit offsets, seek back, and RETRY THE WHOLE
 *     BATCH. Snapshots are additive / LWW, so re-applying and re-emitting is idempotent — we always
 *     re-emit a snapshot for every member touched in the batch (NOT gated on whether Redis changed
 *     this attempt), which is exactly what removes the need for an outbox.
 *
 * Partitioning: producers key member facts by memberId (entityType:entityId), so all of a member's
 * facts land on one partition => one replica owns a member => no cross-replica Redis race. Single
 * partition today; raising the partition count is the only change needed to scale out.
 */
public class SnapshotBuilder {
    private static final Logger log = LoggerFactory.getLogger(SnapshotBuilder.class);
    static final ObjectMapper M = new ObjectMapper();
    static final String SEP = "";   // ref separator (never appears in nbaId/factKey)

    // In-memory mirror of the Redis "rulefacts" cache — the union of every fact key any action rule
    // references. The builder only snapshots facts in this set, so the snapshot stays lean = exactly
    // what the rules engine cares about. Refreshed from Redis so definition changes propagate live.
    static volatile Set<String> RULE_FACTS = Set.of();

    // ---- resolver seam (real = Redis id-map; test = a fake) ----
    interface NbaIdResolver { String resolve(String entityType, String entityId); }

    // ---- a record parsed out of the poll batch (kafka key + body + raw + the "kind" header) ----
    record Parsed(String kafkaKey, JsonNode value, String kind, String raw) {}

    // ---- a fact destined for the member snapshot (post lean-filter, NBAID resolved) ----
    record SnapFact(String nbaId, String entityType, String entityId, String factKey,
                    long eventTs, ObjectNode fv) {}

    // ---- a produce destined for another topic (nba.definitions or the nba.facts firehose) ----
    static final class Forward {
        final String topic, key, value, headerKey;
        final byte[] headerVal;
        Forward(String topic, String key, String value, String headerKey, byte[] headerVal) {
            this.topic = topic; this.key = key; this.value = value;
            this.headerKey = headerKey; this.headerVal = headerVal;
        }
    }

    // ---- the plan for one batch: what to forward, what to snapshot, who to (re)emit ----
    static final class BatchPlan {
        final List<Forward> forwards = new ArrayList<>();
        final List<SnapFact> snapFacts = new ArrayList<>();
        final LinkedHashSet<String> emitMembers = new LinkedHashSet<>();   // nbaIds (re-emit even on retry)
    }

    // ---- winners after event-time LWW: per snapKey, the fact fields to write + member meta ----
    static final class Winners {
        final LinkedHashMap<String, LinkedHashMap<String, String>> byKey = new LinkedHashMap<>();
        final Map<String, String[]> meta = new HashMap<>();   // snapKey -> {entityType, entityId, nbaId}
    }

    // =================================================================================
    // PURE planning (no I/O) — unit-tested directly.
    // =================================================================================

    /**
     * Classify a whole batch into routes (defs/firehose), snapshot facts, and the set of members to
     * (re)emit. Pure given an NBAID resolver. Routing decisions use the fact BODY + the "kind" header,
     * never the kafka key, so it's robust to the memberId re-keying.
     */
    static BatchPlan classify(List<Parsed> batch, Set<String> ruleFacts, NbaIdResolver resolver,
                              String factsTopic, String defsTopic) {
        BatchPlan plan = new BatchPlan();
        for (Parsed p : batch) {
            JsonNode f = p.value();
            String fkey = f.path("key").asText("");

            // population-wide channel throttle level (from the lake) -> broadcast on definitions, never snapshot
            if (fkey.startsWith("nba.throttle.")) {
                plan.forwards.add(new Forward(defsTopic, "THROTTLE:" + fkey.substring("nba.throttle.".length()),
                        p.raw(), null, null));
                continue;
            }
            // operator action-suppress -> definitions only
            if (fkey.startsWith("nba.actionsuppress.")) {
                plan.forwards.add(new Forward(defsTopic, "ACTION_SUPPRESS:" + fkey.substring("nba.actionsuppress.".length()),
                        p.raw(), null, null));
                continue;
            }

            // internally-born facts carry a "kind" header -> re-emit onto the nba.facts firehose so the
            // ML feature source / lake see them (external facts are already on nba.facts and carry no kind).
            String kind = p.kind();
            if (kind != null) {
                if ("throttle-suppress".equals(kind)) {
                    // channel parsed from the fact key (nba.actionstate.{action}.{channel}) — no kafka-key dependency
                    String channel = fkey.substring(fkey.lastIndexOf('.') + 1);
                    if (!channel.isEmpty())
                        plan.forwards.add(new Forward(defsTopic, "THROTTLE_HOT:" + channel, p.raw(), null, null));
                }
                plan.forwards.add(new Forward(factsTopic, p.kafkaKey(), p.raw(),
                        "kind", kind.getBytes(StandardCharsets.UTF_8)));
                // router decisions are nba-internal (CREATE/SUPPRESS) — firehosed above, but NOT a member attribute
                if ("router".equals(kind)) continue;
            }

            SnapFact sf = toSnapFact(f, ruleFacts, resolver);
            if (sf != null) { plan.snapFacts.add(sf); plan.emitMembers.add(sf.nbaId()); }
        }
        return plan;
    }

    /** Build the snapshot fact (lean-filter + NBAID resolve + value flattening), or null if irrelevant. */
    static SnapFact toSnapFact(JsonNode f, Set<String> ruleFacts, NbaIdResolver resolver) {
        String entityType = f.path("entityType").asText("");
        String entityId = f.path("entityId").asText("");
        String key = f.path("key").asText("");
        if (entityType.isEmpty() || entityId.isEmpty() || key.isEmpty()) return null;

        boolean isScore = key.startsWith("nba.score.");
        boolean isState = key.startsWith("nba.actionstate.");
        // Disposition (soft-completion funnel) + completion (hard-completion signal) facts always ride the
        // snapshot — the rules engine derives soft completion from the disposition and latches hard
        // completion from nba.completion.* — so they're attached regardless of the lean rulefacts filter,
        // exactly like scores and action states.
        boolean isDisposition = key.startsWith("nba.disposition.");
        boolean isCompletion = key.startsWith("nba.completion.");
        boolean isMilestone = key.startsWith("nba.milestone.");      // durable milestone-completion fact (rules-engine)
        boolean alwaysAttach = isScore || isState || isDisposition || isCompletion || isMilestone;
        if (!alwaysAttach && !ruleFacts.isEmpty() && !ruleFacts.contains(key)) return null;   // not used by any rule

        long eventTs = f.path("eventTs").asLong(0);
        String nbaId = f.hasNonNull("nbaId") ? f.get("nbaId").asText() : resolver.resolve(entityType, entityId);
        if (nbaId == null || nbaId.isEmpty()) return null;

        ObjectNode fv = M.createObjectNode();
        if (isScore) {                                   // flatten ChannelActionScore to its numeric score
            JsonNode v = f.get("value");
            fv.set("value", (v != null && v.has("score")) ? v.get("score") : v);
            fv.put("valueType", "DOUBLE");
        } else {
            fv.set("value", f.get("value"));
            fv.put("valueType", f.path("valueType").asText("STRING"));
        }
        fv.put("eventTs", eventTs);
        fv.put("source", f.path("source").asText(""));
        return new SnapFact(nbaId, entityType, entityId, key, eventTs, fv);
    }

    /**
     * Event-time last-writer-wins. Given the batch's snapshot facts and the CURRENT stored eventTs per
     * (member, factKey), keep the newest fact per ref and drop anything not strictly newer than stored.
     * Returns the writes to apply. NOTE: this is intentionally independent of the emit set — on a retry
     * where Redis already holds the values, winners is empty yet the snapshots are still re-emitted.
     */
    static Winners selectWinners(List<SnapFact> facts, Map<String, Long> curTsByRef) {
        Map<String, SnapFact> newest = new LinkedHashMap<>();   // ref -> newest fact in this batch
        for (SnapFact sf : facts) {
            String ref = ref(sf.nbaId(), sf.factKey());
            SnapFact ex = newest.get(ref);
            if (ex == null || sf.eventTs() > ex.eventTs()) newest.put(ref, sf);
        }
        Winners w = new Winners();
        for (SnapFact sf : newest.values()) {
            long cur = curTsByRef.getOrDefault(ref(sf.nbaId(), sf.factKey()), -1L);
            if (sf.eventTs() <= cur) continue;            // stale -> LWW drop
            String snapKey = snapKey(sf.nbaId());
            w.byKey.computeIfAbsent(snapKey, k -> new LinkedHashMap<>())
                    .put("fact:" + sf.factKey(), fvJson(sf.fv()));
            w.meta.put(snapKey, new String[]{sf.entityType(), sf.entityId(), sf.nbaId()});
        }
        return w;
    }

    /** Build the full snapshot JSON for a member from its Redis hash. */
    static String buildSnapshotJson(String nbaId, Map<String, String> all) throws Exception {
        ObjectNode snap = M.createObjectNode();
        snap.put("nbaId", nbaId);
        snap.put("entityType", all.getOrDefault("__entityType", ""));
        snap.put("entityId", all.getOrDefault("__entityId", ""));
        snap.put("correlationId", UUID.randomUUID().toString());
        snap.put("updatedTs", Long.parseLong(all.getOrDefault("__updatedTs", "0")));
        ObjectNode facts = snap.putObject("facts");
        for (Map.Entry<String, String> e : all.entrySet())
            if (e.getKey().startsWith("fact:")) facts.set(e.getKey().substring(5), M.readTree(e.getValue()));
        return M.writeValueAsString(snap);
    }

    static String snapKey(String nbaId) { return "nba:snapshot:" + nbaId; }
    static String ref(String nbaId, String factKey) { return nbaId + SEP + factKey; }
    static String fvJson(ObjectNode fv) { try { return M.writeValueAsString(fv); } catch (Exception e) { throw new RuntimeException(e); } }

    // =================================================================================
    // I/O shell: poll -> classify -> (1 Redis txn) -> (1 Kafka txn) -> commit; retry whole batch.
    // =================================================================================

    public static void main(String[] args) {
        String bootstrap = env("NBA_BOOTSTRAP", "nba-redpanda:9092");
        String redisHost = env("NBA_REDIS_HOST", "nba-redis");
        int redisPort = Integer.parseInt(env("NBA_REDIS_PORT", "6379"));
        String topicIn = env("NBA_TOPIC_IN", "nba.member.facts");
        String topicOut = env("NBA_TOPIC_OUT", "nba.snapshots");
        String factsTopic = env("NBA_FACTS_TOPIC", "nba.facts");
        String defsTopic = env("NBA_DEFINITIONS_TOPIC", "nba.definitions");
        String dlq = env("NBA_DLQ", "nba.dlq.snapshot-builder");   // per-consumer DLQ (envelope-wrapped, replayable)
        String group = env("NBA_GROUP", "snapshot-builder");
        // transactional.id MUST be stable per instance (and unique across instances when scaling out).
        String txnId = env("NBA_TXN_ID", "snapshot-builder-" + hostname());

        // Prometheus /metrics on a side port (no HTTP surface otherwise).
        Metrics.serve(Integer.parseInt(env("NBA_METRICS_PORT", "9408")));

        JedisPooled redis = new JedisPooled(redisHost, redisPort);
        NbaIdResolver resolver = (et, id) -> resolveNbaId(redis, et, id);

        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        cp.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pp.put(ProducerConfig.ACKS_CONFIG, "all");
        pp.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        pp.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp);
        KafkaProducer<String, String> producer = new KafkaProducer<>(pp);
        producer.initTransactions();

        // The rulefacts refresh / de-reference prune re-emits snapshots on a background thread, so it
        // gets its OWN non-transactional producer (the main producer owns the single in-flight txn).
        KafkaProducer<String, String> pruneProducer = new KafkaProducer<>(plainProps(bootstrap));
        startRuleFactsRefresh(redis, pruneProducer, topicOut);

        consumer.subscribe(List.of(topicIn));
        log.info("up (batched/transactional): in=" + topicIn + " out=" + topicOut
                + " redis=" + redisHost + ":" + redisPort + " bootstrap=" + bootstrap + " txn=" + txnId);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> { try { consumer.wakeup(); } catch (Exception ignore) {} }));

        long backoffMs = 1000;
        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
            if (recs.isEmpty()) continue;
            try {
                processBatch(recs, redis, producer, consumer, resolver, topicOut, factsTopic, defsTopic, dlq);
                backoffMs = 1000;                          // batch committed — reset backoff
            } catch (Exception e) {
                // Whole-batch retry: abort the txn, rewind to the batch start, re-poll the SAME records.
                // Idempotent because snapshots are additive/LWW and we re-emit per member regardless.
                log.warn("batch FAILED, retrying whole batch: " + e);
                try { producer.abortTransaction(); } catch (Exception ignore) {}
                for (TopicPartition tp : recs.partitions())
                    consumer.seek(tp, recs.records(tp).get(0).offset());
                try { Thread.sleep(backoffMs); } catch (InterruptedException ie) { return; }
                backoffMs = Math.min(30000, backoffMs * 2);
            }
        }
    }

    /** One batch: classify, apply Redis writes in one txn, then produce + commit offsets in one Kafka txn. */
    static void processBatch(ConsumerRecords<String, String> recs, JedisPooled redis,
                             KafkaProducer<String, String> producer, KafkaConsumer<String, String> consumer,
                             NbaIdResolver resolver, String topicOut, String factsTopic, String defsTopic, String dlq)
            throws Exception {
        List<Parsed> parsed = new ArrayList<>();
        List<ConsumerRecord<String, String>> poison = new ArrayList<>();
        for (ConsumerRecord<String, String> r : recs) {
            try {
                JsonNode v = M.readTree(r.value());
                parsed.add(new Parsed(r.key(), v, header(r, "kind"), r.value()));
            } catch (Exception e) {
                poison.add(r);                              // unparseable -> DLQ (still inside the txn below)
            }
        }

        BatchPlan plan = classify(parsed, RULE_FACTS, resolver, factsTopic, defsTopic);

        // ---- read CURRENT stored eventTs for every (member, fact) the batch touches (one pipeline) ----
        LinkedHashMap<String, String[]> refs = new LinkedHashMap<>();   // ref -> {snapKey, field}
        for (SnapFact sf : plan.snapFacts)
            refs.putIfAbsent(ref(sf.nbaId(), sf.factKey()), new String[]{snapKey(sf.nbaId()), "fact:" + sf.factKey()});
        Map<String, Long> curTs = new HashMap<>();
        if (!refs.isEmpty()) {
            Map<String, Response<String>> resp = new LinkedHashMap<>();
            try (var pipe = redis.pipelined()) {       // try-with-resources: return the pooled connection
                for (Map.Entry<String, String[]> e : refs.entrySet())
                    resp.put(e.getKey(), pipe.hget(e.getValue()[0], e.getValue()[1]));
                pipe.sync();
                for (Map.Entry<String, Response<String>> e : resp.entrySet()) {
                    String cur = e.getValue().get();
                    long t = -1;
                    if (cur != null) { try { t = M.readTree(cur).path("eventTs").asLong(-1); } catch (Exception ignore) {} }
                    curTs.put(e.getKey(), t);
                }
            }
        }

        Winners winners = selectWinners(plan.snapFacts, curTs);

        // ---- (1) ONE Redis transaction: apply all winning fact writes + member meta ----
        if (!winners.byKey.isEmpty()) {
            String nowMs = String.valueOf(System.currentTimeMillis());
            try (var tx = redis.multi()) {             // try-with-resources: return the pooled connection
                for (Map.Entry<String, LinkedHashMap<String, String>> e : winners.byKey.entrySet()) {
                    String snapKey = e.getKey();
                    for (Map.Entry<String, String> fe : e.getValue().entrySet()) tx.hset(snapKey, fe.getKey(), fe.getValue());
                    String[] meta = winners.meta.get(snapKey);
                    tx.hset(snapKey, "__entityType", meta[0]);
                    tx.hset(snapKey, "__entityId", meta[1]);
                    tx.hset(snapKey, "__nbaId", meta[2]);
                    tx.hset(snapKey, "__updatedTs", nowMs);
                }
                tx.exec();
            }
        }

        // ---- (2) ONE Kafka transaction: routes + firehose + DLQ + per-member snapshots + offset commit ----
        producer.beginTransaction();
        try {
            for (Forward fwd : plan.forwards) {
                ProducerRecord<String, String> pr = new ProducerRecord<>(fwd.topic, fwd.key, fwd.value);
                if (fwd.headerKey != null) pr.headers().add(fwd.headerKey, fwd.headerVal);
                producer.send(pr);
            }
            for (ConsumerRecord<String, String> p : poison)
                producer.send(dlqEnvelope(dlq, "snapshot-builder", p, "deserialize failed (unparseable JSON)"));
            // Re-emit a snapshot for EVERY member the batch touched — independent of LWW outcome, so a
            // retry (where Redis already holds the values) still re-emits. This is what removes the outbox.
            for (String nbaId : plan.emitMembers) {
                Map<String, String> all = redis.hgetAll(snapKey(nbaId));
                if (all.isEmpty()) continue;               // pure-stale brand-new member: nothing stored, nothing to emit
                long t0 = System.nanoTime();
                try {
                    producer.send(new ProducerRecord<>(topicOut, nbaId, buildSnapshotJson(nbaId, all)));
                } finally {
                    Metrics.timer("nba_snapshot_build_seconds").record(System.nanoTime() - t0, java.util.concurrent.TimeUnit.NANOSECONDS);
                }
                Metrics.counter("nba_snapshots_built_total").increment();
            }
            producer.sendOffsetsToTransaction(offsetsAfter(recs), consumer.groupMetadata());
            producer.commitTransaction();
            log.info("batch committed: in=" + recs.count()
                    + " routes=" + plan.forwards.size() + " writes=" + winners.byKey.size()
                    + " snapshots=" + plan.emitMembers.size() + " dlq=" + poison.size());
        } catch (Exception e) {
            producer.abortTransaction();
            throw e;                                        // -> whole-batch retry in main()
        }
    }

    static Map<TopicPartition, OffsetAndMetadata> offsetsAfter(ConsumerRecords<String, String> recs) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition tp : recs.partitions()) {
            List<ConsumerRecord<String, String>> prs = recs.records(tp);
            offsets.put(tp, new OffsetAndMetadata(prs.get(prs.size() - 1).offset() + 1));
        }
        return offsets;
    }

    static String header(ConsumerRecord<String, String> r, String name) {
        var h = r.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    /** Standard DLQ envelope: the poison record's SOURCE coordinates (topic/partition/offset/key/headers) +
     *  raw value + error, so the command center monitors per consumer and a replay re-produces the EXACT
     *  original record to its source topic. Keyed by the original key so replayed records keep their partition. */
    static ProducerRecord<String, String> dlqEnvelope(String dlqTopic, String consumer, ConsumerRecord<String, String> r, String error) {
        try {
            ObjectNode env = M.createObjectNode();
            env.put("consumer", consumer);
            env.put("topic", r.topic());
            env.put("partition", r.partition());
            env.put("offset", r.offset());
            if (r.key() != null) env.put("key", r.key()); else env.putNull("key");
            env.put("value", r.value());
            ObjectNode h = env.putObject("headers");
            for (org.apache.kafka.common.header.Header hd : r.headers())
                if (hd.value() != null) h.put(hd.key(), new String(hd.value(), StandardCharsets.UTF_8));
            env.put("error", error);
            env.put("dlqTs", System.currentTimeMillis());
            return new ProducerRecord<>(dlqTopic, r.key(), M.writeValueAsString(env));
        } catch (Exception e) {
            return new ProducerRecord<>(dlqTopic, r.key(), r.value());   // fallback: the raw value
        }
    }

    // =================================================================================
    // rulefacts cache + de-reference pruning (unchanged behavior; own producer)
    // =================================================================================

    static void startRuleFactsRefresh(JedisPooled redis, KafkaProducer<String, String> producer, String topicOut) {
        Runnable load = () -> {
            try {
                Set<String> next = redis.smembers("nba:rulefacts");
                if (next == null) next = Set.of();
                Set<String> prev = RULE_FACTS;
                RULE_FACTS = next;
                if (!prev.isEmpty() && !next.isEmpty()) {
                    Set<String> removed = new java.util.HashSet<>(prev);
                    removed.removeAll(next);
                    if (!removed.isEmpty()) pruneFacts(redis, producer, topicOut, removed);
                }
            } catch (Exception e) {
                log.warn("rulefacts refresh failed", e);
            }
        };
        load.run();
        log.info("rulefacts loaded: " + RULE_FACTS.size()
                + (RULE_FACTS.isEmpty() ? " (empty -> snapshot ALL, fail-open for dev)" : " " + RULE_FACTS));
        Thread t = new Thread(() -> {
            while (true) {
                try { Thread.sleep(10_000); } catch (InterruptedException ie) { return; }
                load.run();
            }
        }, "rulefacts-refresh");
        t.setDaemon(true);
        t.start();
    }

    /** Remove de-referenced fact fields from every snapshot hash and re-emit the changed snapshots. */
    static void pruneFacts(JedisPooled redis, KafkaProducer<String, String> producer, String topicOut, Set<String> removed) {
        int pruned = 0;
        ScanParams sp = new ScanParams().match("nba:snapshot:*").count(200);
        String cur = ScanParams.SCAN_POINTER_START;
        do {
            ScanResult<String> sr = redis.scan(cur, sp);
            for (String snapKey : sr.getResult()) {
                boolean changed = false;
                for (String f : removed) {
                    Long n = redis.hdel(snapKey, "fact:" + f);
                    if (n != null && n > 0) { changed = true; pruned++; }
                }
                if (changed) {
                    String nbaId = redis.hget(snapKey, "__nbaId");
                    if (nbaId != null) {
                        try { producer.send(new ProducerRecord<>(topicOut, nbaId, buildSnapshotJson(nbaId, redis.hgetAll(snapKey)))); }
                        catch (Exception e) { /* best-effort */ }
                    }
                }
            }
            cur = sr.getCursor();
        } while (!ScanParams.SCAN_POINTER_START.equals(cur));
        if (pruned > 0) log.info("pruned " + pruned + " de-referenced fact(s) " + removed);
    }

    static String resolveNbaId(JedisPooled redis, String entityType, String entityId) {
        String idmapKey = "nba:idmap:" + entityType + ":" + entityId;
        String existing = redis.get(idmapKey);
        if (existing != null) return existing;
        String nbaId = "nba_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        long set = redis.setnx(idmapKey, nbaId);     // race-safe: first writer wins
        return set == 1 ? nbaId : redis.get(idmapKey);
    }

    static Properties plainProps(String bootstrap) {
        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pp.put(ProducerConfig.ACKS_CONFIG, "all");
        return pp;
    }

    static String hostname() {
        String h = System.getenv("HOSTNAME");
        return (h == null || h.isBlank()) ? "0" : h;
    }

    static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? d : v;
    }
}
