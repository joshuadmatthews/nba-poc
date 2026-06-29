package ai.das.nba.router;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import redis.clients.jedis.JedisPooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NBA Action Router.
 *
 * Consumes nba.evaluations and emits a router DECISION fact (op=CREATE / SUPPRESS) to
 * nba.member.facts with a kind=router header — the member-level "who gets what" — for the
 * TOP-scored ChannelAction of each member (or the top-N batch on the winning channel). It reads
 * scores straight off the eval — the rules engine carries them over — so it never touches snapshots.
 *
 * The eval is re-emitted whenever a score changes (with eligibilityChanged=false), so the
 * router always sees the latest scores; the loop is prevented upstream by the ML layer
 * ignoring those score-only evals. The router de-dupes on the chosen action so CREATE fires
 * only when the top actually changes. The Temporal bridge consumes the kind=router facts next
 * (debounce, TTL, CANCEL lifecycle) and DISPATCH/CANCEL onto nba.activations for the action layer.
 */
public class ActionRouter {
    private static final Logger log = LoggerFactory.getLogger(ActionRouter.class);
    static final ObjectMapper M = new ObjectMapper();

    // Operator-suppressed actions / action-channels, mirrored IN MEMORY so each eval's strip is a pure
    // HashSet lookup — no Redis round-trip in the hot activation path. Fed by the compacted nba.definitions
    // broadcast (ACTION_SUPPRESS:{target}), same pattern the rules engine uses for its definitions.
    static final Set<String> SUPPRESSED = ConcurrentHashMap.newKeySet();

    // The SINGLE eligibility object: the router OWNS + WRITES it (kafka -> router -> redis) as the latest eval,
    // verbatim. The rules engine reads it only to detect change; the lake + inbound serve read it for current
    // state. The router never reads it back — dispatch dedup is the temporal bridge's job. No other Redis keys.
    static final String ELIG_KEY = "nba:eligibility:";
    // ELIGIBILITY SOURCE: "redis" = persist nba:eligibility here (the hot path + lake read it) | "kstreams" = the
    // decision-engine materializes eligibility from nba.evaluations and serves it via IQ, so we SKIP the Redis
    // write. Default redis (the flag is the only change). Routing decisions (CREATE/SUPPRESS) are emitted regardless.
    static final String ELIG_SOURCE = env("NBA_ELIG_SOURCE", "redis");

    public static void main(String[] args) {
        String bootstrap = env("NBA_BOOTSTRAP", "nba-redpanda:9092");
        String redisHost = env("NBA_REDIS_HOST", "nba-redis");
        int    redisPort = Integer.parseInt(env("NBA_REDIS_PORT", "6379"));
        String evalTopic = env("NBA_EVAL_TOPIC", "nba.evaluations");
        // Router DECISIONS (CREATE/SUPPRESS) are member-level facts about who-gets-what — they ride
        // nba.member.facts with a kind=router header (consumed ONLY by the Temporal bridge). nba.activations
        // is reserved for the state machine's DISPATCH/CANCEL — the portal to the downstream channels.
        String memberFacts = env("NBA_MEMBER_FACTS", "nba.member.facts");
        String defsTopic = env("NBA_DEFINITIONS_TOPIC", "nba.definitions");
        String dlq       = env("NBA_DLQ", "nba.dlq.action-router");
        String group     = env("NBA_GROUP", "action-router");

        // Prometheus /metrics on a side port (no HTTP surface otherwise).
        Metrics.serve(Integer.parseInt(env("NBA_METRICS_PORT", "9406")));

        JedisPooled redis = new JedisPooled(redisHost, redisPort);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps(bootstrap));
        startSuppressionCache(bootstrap, defsTopic, redis);   // in-memory mirror for the hot-path strip

        KafkaConsumer<String, String> c = new KafkaConsumer<>(consumerProps(bootstrap, group));
        c.subscribe(List.of(evalTopic));
        log.info("up: in=" + evalTopic + " out=" + memberFacts + " (kind=router)");

        while (true) {
            ConsumerRecords<String, String> recs = c.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : recs) {
                try { maybeInjectFault(r.value()); activate(r.value(), redis, producer, memberFacts); }
                catch (Exception e) {
                    log.warn("DLQ <- " + e + " :: " + r.value());
                    producer.send(dlqEnvelope(dlq, "action-router", r, String.valueOf(e)));
                }
            }
            if (!recs.isEmpty()) c.commitSync();
        }
    }

    static void activate(String evalRaw, JedisPooled redis, Producer<String, String> producer,
                         String outTopic) throws Exception {
        JsonNode eval = M.readTree(evalRaw);
        String nbaId = eval.path("nbaId").asText("");
        if (nbaId.isEmpty()) return;
        String entityType = eval.path("entityType").asText("");
        String entityId = eval.path("entityId").asText("");
        String correlationId = eval.path("correlationId").asText("");
        JsonNode channelActions = eval.path("channelActions");

        ObjectNode evalObj = (ObjectNode) eval;

        // Publish durable completion/milestone FACTS straight off the eval's TRANSITION lists. The rules engine
        // DETECTS the transition (the goal / milestone logic passes this eval with no durable fact yet) and flags
        // exactly the new ones on newCompleted[] / newMilestones[]; the router — already a member-facts producer —
        // PUBLISHES them, so journey rules can reference "action X completed" / "milestone Y reached" directly. The
        // engine computed the diff once from the snapshot, so the message ALREADY carries it: NO Redis read. Facts
        // are LWW-idempotent, so a re-publish during the round-trip (an unrelated re-eval before the fact lands) is
        // a snapshot no-op — at-least-once is free here, which is what lets it ride the message instead of the cache.
        for (JsonNode aid : eval.path("newCompleted"))
            emitFact(producer, outTopic, entityType, entityId, nbaId, "nba.completion." + aid.asText(), "true", "BOOL", "completion");
        for (JsonNode m : eval.path("newMilestones"))
            emitFact(producer, outTopic, entityType, entityId, nbaId, "nba.milestone." + m.path("id").asText(),
                    String.valueOf(m.path("completedAt").asLong(0)), "LONG", "milestone");

        // OPERATOR suppression is enforced HERE at the outbound edge (and at the inbound serve) from the
        // IN-MEMORY mirror — NOT by re-evaluating 8M members. The eval stays suppression-agnostic; we just
        // never ACTIVATE a suppressed action. The lake's gold snapshot reconverges as members are touched.
        Set<String> suppressed = SUPPRESSED;

        // The router is the BRIDGE from the rules-engine eval to the state machine. It emits exactly FOUR
        // events and NEVER inspects a disposition state — it reads the coarse flags the rules engine folded
        // onto each ChannelAction in the unified channelActions[]: eligible (passed the rules) · active (a
        // workflow is in flight) · cancellable (CREATED, not sent yet) · softCompleted · hardCompleted. Every
        // ChannelAction carries its CHANNEL, so completion bridges per-channel with no fan-out.
        //
        // 1) Bridge completion to the state machine (idempotent — the workflow dedups). BOTH soft and hard are
        //    RULE-decided by the rules engine (soft = the disposition cleared the action's soft bar, defaulting
        //    to a sensible per-channel default; hard = the goal). The router just bridges the flags it sees;
        //    hardCompleted wins over softCompleted on the same ChannelAction. A completed action stays on the
        //    eval (marked hardCompleted) until its workflow walks terminal, so this is the one bridge point.
        for (JsonNode ca : channelActions) {
            if (isSuppressed(suppressed, ca)) continue;
            if (ca.path("hardCompleted").asBoolean(false))
                emit(producer, outTopic, "HARD_COMPLETE", nbaId, entityType, entityId, correlationId, ca);
            else if (ca.path("softCompleted").asBoolean(false))
                emit(producer, outTopic, "SOFT_COMPLETE", nbaId, entityType, entityId, correlationId, ca);
        }

        // 2) SUPPRESS not-sent-yet actions that dropped out of eligibility (in flight but no longer eligible).
        for (JsonNode ca : channelActions)
            if (!ca.path("eligible").asBoolean(false) && ca.path("cancellable").asBoolean(false)) {
                emit(producer, outTopic, "SUPPRESS", nbaId, entityType, entityId, correlationId, ca);
                Metrics.counter("nba_router_decisions_total", "kind", "suppress").increment();
            }

        // 3) The slot occupant = the top-scored eligible action with a workflow already in flight.
        JsonNode cur = null; double curScore = Double.NEGATIVE_INFINITY;
        for (JsonNode ca : channelActions) {
            if (isSuppressed(suppressed, ca) || !ca.path("eligible").asBoolean(false)
                    || !ca.path("active").asBoolean(false)) continue;
            double sc = ca.path("score").isNumber() ? ca.get("score").asDouble() : Double.NEGATIVE_INFINITY;
            if (sc > curScore) { cur = ca; curScore = sc; }
        }

        // 4) The candidate = top-scored eligible FREE (not active, not converted) scored action.
        JsonNode cand = pickCandidate(channelActions, suppressed);
        double candScore = cand != null ? cand.get("score").asDouble() : Double.NEGATIVE_INFINITY;

        if (cur != null) {
            // 5) Slot occupied? Replace ONLY if the occupant hasn't sent yet (cancellable) and is out-scored;
            //    else HOLD (a sent action owns the slot until its lifecycle closes). The state machine resolves
            //    the SUPPRESS (SUPPRESSED if it caught it, else SUPPRESS_FAILED and the send proceeds).
            if (cand != null && candScore > curScore && cur.path("cancellable").asBoolean(false)) {
                emit(producer, outTopic, "SUPPRESS", nbaId, entityType, entityId, correlationId, cur);
                Metrics.counter("nba_router_decisions_total", "kind", "suppress").increment();
            }
        } else if (cand != null) {
            // 6) Slot free -> CREATE the candidate (single, or the top-N batch on the winning channel). NO double-
            //    dispatch guard: the temporal bridge owns dedup, and trying to suppress a second bid here would
            //    fight the slot auction. Re-CREATE of an action still in its propagation window is a no-op (same
            //    workflowId + USE_EXISTING -> returns the running execution, no new run, no debounce reset). And a
            //    DIFFERENT action winning the slot mid-window is DESIRED: both reach the CREATED debounce and the
            //    higher score sends, the loser self-DEBOUNCEs (ActionActivitiesImpl.debounceLost). The router bids;
            //    the auction decides. Same-action duplicate is idempotent (workflowId); different-action is the win.
            String winChannel = cand.path("channel").asText();
            int maxBatch = readMaxBatch(redis, winChannel);
            java.util.List<JsonNode> batch = maxBatch <= 1 ? java.util.List.of(cand)
                    : selectBatch(channelActions, winChannel, suppressed, maxBatch);
            if (batch.size() == 1) emit(producer, outTopic, "CREATE", nbaId, entityType, entityId, correlationId, batch.get(0));
            else if (batch.size() > 1) emitBatch(producer, outTopic, nbaId, entityType, entityId, correlationId, winChannel, batch);
            if (!batch.isEmpty()) Metrics.counter("nba_router_decisions_total", "kind", "activate").increment();
        }

        // 7) Persist the SINGLE eligibility object (kafka -> router -> redis): the latest eval. The rules engine
        //    reads it ONLY to detect change; the lake + inbound serve read it for current state. The router WRITES
        //    but never READS it — dispatch dedup is the temporal bridge's job (score-ranked debounce), not a Redis
        //    marker. Strip the transient transition lists first: newCompleted/newMilestones are this eval's rules-
        //    engine->router instructions (already published as durable facts), not member state — the perpetual
        //    completed[]/milestones[] are the record that stays on the object.
        evalObj.remove("newCompleted");
        evalObj.remove("newMilestones");
        // source=kstreams: the engine materializes eligibility from nba.evaluations (same strip) + serves it via IQ,
        // so skip the Redis write entirely. Under redis, write it best-effort (the router never reads it back).
        if (!"kstreams".equalsIgnoreCase(ELIG_SOURCE)) {
            try { redis.set(ELIG_KEY + nbaId, M.writeValueAsString(evalObj)); }
            catch (Exception ex) { log.warn("eligibility write-through failed", ex); }
        }
    }

    /** Publish a durable internal fact (member-fact shape + kind header) onto nba.member.facts — the snapshot-builder
     *  folds it (+ firehoses). The rules engine DETECTS completion/milestone; the router PUBLISHES them here. */
    static void emitFact(Producer<String, String> producer, String topic, String et, String ei, String nbaId,
                         String key, String value, String valueType, String kind) throws Exception {
        if (et.isEmpty() || ei.isEmpty()) return;
        ObjectNode fact = M.createObjectNode();
        fact.put("entityType", et); fact.put("entityId", ei);
        fact.put("key", key); fact.put("value", value); fact.put("valueType", valueType);
        fact.put("eventTs", System.currentTimeMillis()); fact.put("source", "action-router");
        if (nbaId != null && !nbaId.isEmpty()) fact.put("nbaId", nbaId);
        ProducerRecord<String, String> rec = new ProducerRecord<>(topic, et + ":" + ei, M.writeValueAsString(fact));
        rec.headers().add("kind", kind.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        producer.send(rec);
    }

    /** PURE: the top-scored eligible action that is FREE — passed the rules (eligible=true), no workflow in
     *  flight (active=false), not already converted (hardCompleted=false), not operator-suppressed, and scored.
     *  Ties break on the smallest slug. */
    static JsonNode pickCandidate(JsonNode channelActions, Set<String> suppressed) {
        JsonNode cand = null; double candScore = Double.NEGATIVE_INFINITY; String candSlug = null;
        for (JsonNode ca : channelActions) {
            if (isSuppressed(suppressed, ca) || !ca.path("eligible").asBoolean(false)) continue;
            if (ca.path("active").asBoolean(false) || ca.path("hardCompleted").asBoolean(false)) continue;
            if (!ca.path("score").isNumber()) continue;                    // not scored yet
            double sc = ca.get("score").asDouble();
            String s = slug(ca);
            if (sc > candScore || (sc == candScore && s.compareTo(candSlug) < 0)) {
                cand = ca; candScore = sc; candSlug = s;
            }
        }
        return cand;
    }

    /** PURE: the top-N FREE eligible actions on the winning channel, ordered by score desc, capped at maxBatch. */
    static java.util.List<JsonNode> selectBatch(JsonNode channelActions, String winChannel,
                                                Set<String> suppressed, int maxBatch) {
        java.util.List<JsonNode> batch = new ArrayList<>();
        for (JsonNode ca : channelActions) {
            if (!winChannel.equals(ca.path("channel").asText())) continue;
            if (isSuppressed(suppressed, ca) || !ca.path("eligible").asBoolean(false)) continue;   // operator-pulled / ineligible
            if (ca.path("active").asBoolean(false) || ca.path("hardCompleted").asBoolean(false)) continue;
            if (!ca.path("score").isNumber()) continue;
            batch.add(ca);
        }
        batch.sort((a, b) -> Double.compare(b.path("score").asDouble(-1), a.path("score").asDouble(-1)));
        if (batch.size() > maxBatch) batch = batch.subList(0, maxBatch);
        return batch;
    }

    /** Mirror the operator-suppressed set IN MEMORY from the compacted nba.definitions topic
     *  (ACTION_SUPPRESS:{target} = {value:bool}) so the per-eval strip needs no Redis call. Fresh group +
     *  earliest replays current state on startup; the broadcast converges every router instance. action-library
     *  owns/writes the set (Redis nba:suppressed is the durable copy); we hydrate from it once as a floor. */
    static void startSuppressionCache(String bootstrap, String defsTopic, JedisPooled redis) {
        try { SUPPRESSED.addAll(redis.smembers("nba:suppressed")); } catch (Exception ignore) {}
        Thread t = new Thread(() -> {
            Properties cp = consumerProps(bootstrap, "action-router-suppress-" + UUID.randomUUID().toString().substring(0, 8));
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp);
            consumer.subscribe(List.of(defsTopic));
            log.info("suppression cache up: " + defsTopic + " (ACTION_SUPPRESS:*)");
            while (true) {
                ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : recs) {
                    String k = r.key();
                    if (k == null || !k.startsWith("ACTION_SUPPRESS:")) continue;
                    String target = k.substring("ACTION_SUPPRESS:".length());
                    boolean on = false;
                    try { on = r.value() != null && M.readTree(r.value()).path("value").asBoolean(false); } catch (Exception ignore) {}
                    if (on) SUPPRESSED.add(target); else SUPPRESSED.remove(target);
                }
            }
        }, "suppress-cache");
        t.setDaemon(true);
        t.start();
    }

    /** Suppressed by whole action (actionId) or just this action-channel (actionId.channel)? */
    static boolean isSuppressed(java.util.Set<String> suppressed, JsonNode ca) {
        if (suppressed.isEmpty()) return false;
        String aid = ca.path("actionId").asText(), ch = ca.path("channel").asText();
        return suppressed.contains(aid) || suppressed.contains(aid + "." + ch);
    }

    static int readMaxBatch(JedisPooled redis, String channel) {
        try { String v = redis.hget("nba:channel:maxbatch", channel); return v == null ? 1 : Math.max(1, Integer.parseInt(v)); }
        catch (Exception e) { return 1; }
    }

    /** One CREATE carrying the batch of actions (op=CREATE, channel + actions[]); no per-action actionId. */
    static void emitBatch(Producer<String, String> producer, String outTopic, String nbaId,
                          String entityType, String entityId, String correlationId, String channel, java.util.List<JsonNode> batch) {
        if (batch.isEmpty()) return;
        ObjectNode act = M.createObjectNode();
        act.put("nbaId", nbaId); act.put("entityType", entityType); act.put("entityId", entityId);
        act.put("memberId", entityId); act.put("op", "CREATE_BATCH"); act.put("channel", channel);
        var actions = act.putArray("actions");
        for (JsonNode ca : batch) {
            ObjectNode o = actions.addObject();
            o.put("actionId", ca.path("actionId").asText());
            o.put("contentKey", ca.path("contentKey").asText());
            o.put("name", ca.path("name").asText());
            o.put("ttlSeconds", ca.path("ttlSeconds").asLong(0));
            if (ca.path("score").isNumber()) o.put("score", ca.get("score").asDouble());
        }
        act.put("correlationId", correlationId); act.put("source", "action-router");
        act.put("eventTs", System.currentTimeMillis());
        String key = nbaId + ":" + channel + ":batch";   // one batch orchestrator per (member, channel)
        try { producer.send(routerRecord(outTopic, key, M.writeValueAsString(act))); }
        catch (Exception e) { throw new RuntimeException(e); }
        log.info("CREATE batch " + nbaId + " " + channel + " x" + batch.size());
    }

    static String slug(JsonNode ca) { return ca.path("actionId").asText() + "::" + ca.path("channel").asText(); }

    /** A router decision rides nba.member.facts with a kind=router HEADER, so the Temporal bridge can pick
     *  it up by header (no body deserialize) and the snapshot-builder can SKIP it (it's not a member fact). */
    static ProducerRecord<String, String> routerRecord(String topic, String key, String value) {
        ProducerRecord<String, String> rec = new ProducerRecord<>(topic, key, value);
        rec.headers().add("kind", "router".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return rec;
    }

    static void emit(Producer<String, String> producer, String outTopic, String op, String nbaId,
                     String entityType, String entityId, String correlationId, JsonNode ca) {
        ObjectNode act = M.createObjectNode();
        act.put("nbaId", nbaId);
        act.put("entityType", entityType);
        act.put("entityId", entityId);
        act.put("memberId", entityId);    // original member id rides the whole path (not nbaId)
        act.put("op", op);
        act.put("actionId", ca.path("actionId").asText());
        act.put("channel", ca.path("channel").asText());
        act.put("name", ca.path("name").asText());
        act.put("contentKey", ca.path("contentKey").asText());
        act.put("ttlSeconds", ca.path("ttlSeconds").asLong(0));
        if (ca.path("score").isNumber()) act.put("score", ca.get("score").asDouble());
        act.put("correlationId", correlationId);
        act.put("source", "action-router");
        act.put("eventTs", System.currentTimeMillis());
        // keyed per (member, action, channel) so CREATE and SUPPRESS for different actions both
        // survive compaction and are seen in order by the state machine.
        String key = nbaId + ":" + ca.path("actionId").asText() + ":" + ca.path("channel").asText();
        try { producer.send(routerRecord(outTopic, key, M.writeValueAsString(act))); }
        catch (Exception e) { throw new RuntimeException(e); }
        log.info("" + op + " " + nbaId + " -> " + slug(ca)
                + (ca.path("score").isNumber() ? " score=" + ca.get("score").asDouble() : ""));
    }

    static Properties consumerProps(String bootstrap, String group) {
        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return cp;
    }

    static Properties producerProps(String bootstrap) {
        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pp.put(ProducerConfig.ACKS_CONFIG, "all");
        return pp;
    }

    /** Standard DLQ envelope: source coordinates + raw value + error (replayable to the source topic). */
    static ProducerRecord<String, String> dlqEnvelope(String dlqTopic, String consumer, ConsumerRecord<String, String> r, String error) {
        try {
            ObjectNode env = M.createObjectNode();
            env.put("consumer", consumer); env.put("topic", r.topic());
            env.put("partition", r.partition()); env.put("offset", r.offset());
            if (r.key() != null) env.put("key", r.key()); else env.putNull("key");
            env.put("value", r.value());
            ObjectNode h = env.putObject("headers");
            for (org.apache.kafka.common.header.Header hd : r.headers())
                if (hd.value() != null) h.put(hd.key(), new String(hd.value(), java.nio.charset.StandardCharsets.UTF_8));
            env.put("error", error); env.put("dlqTs", System.currentTimeMillis());
            return new ProducerRecord<>(dlqTopic, r.key(), M.writeValueAsString(env));
        } catch (Exception e) { return new ProducerRecord<>(dlqTopic, r.key(), r.value()); }
    }

    // Test hook: when NBA_FAULT_INJECT is set, any record whose raw value CONTAINS that substring throws —
    // exercises this consumer's DLQ + replay path on demand. Empty (default) = no-op, no prod impact.
    static final String FAULT_INJECT = env("NBA_FAULT_INJECT", "");
    static void maybeInjectFault(String raw) {
        if (!FAULT_INJECT.isEmpty() && raw != null && raw.contains(FAULT_INJECT))
            throw new RuntimeException("injected fault (NBA_FAULT_INJECT=" + FAULT_INJECT + ")");
    }

    static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? d : v;
    }
}
