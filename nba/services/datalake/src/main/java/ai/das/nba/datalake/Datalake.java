package ai.das.nba.datalake;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import redis.clients.jedis.JedisPooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * RETIRED (2026-06) — DO NOT RUN. Comms-frequency counting moved to the transactional Delta lake:
 * nba/databricks/nba_comms_count.py counts per-member weekly comms from silver_fact_history and
 * native-produces operator.comms.* with origin=lake. This Redis-counter version was a dual write
 * (redis.incr + Kafka emit) that couldn't be kept consistent; the lake is transactional, so the
 * counter is gone. Kept for history only — the container is removed and run.ps1 is a retired stub.
 *
 * NBA datalake counter (STUB for the comms-frequency slice).
 *
 * The real datalake derives count facts from dispositions in the lake. For the POC this watches
 * the firehose for "sent" workflow-state transitions (nba.actionstate.{actionId}.{channel} ==
 * sent) and increments per-member weekly counters, emitting them back as facts:
 *   operator.comms.commsThisWeek      (all channels)
 *   operator.comms.{channel}sThisWeek (e.g. emailsThisWeek)
 * Those are exactly the rulefacts the global + channel cap rules read, so a send tightens
 * eligibility on the next pass — closing the loop (and making a superseded action that slipped
 * through to "sent" trim its competitor "on its own").
 *
 * Counters carry a 7-day TTL ~ a rolling week. De-duped per (slug,eventTs) so a compacted
 * replay can't double-count.
 */
public class Datalake {
    private static final Logger log = LoggerFactory.getLogger(Datalake.class);
    static final ObjectMapper M = new ObjectMapper();
    static final long WEEK_SECONDS = 7 * 24 * 3600;

    public static void main(String[] args) {
        String bootstrap  = env("NBA_BOOTSTRAP", "nba-redpanda:9092");
        String factsTopic = env("NBA_FACTS_TOPIC", "nba.facts");
        String memberFacts= env("NBA_MEMBER_FACTS", "nba.member.facts");
        String dlq        = env("NBA_DLQ", "nba.facts.dlq");
        String redisHost  = env("NBA_REDIS_HOST", "nba-redis");
        int    redisPort  = Integer.parseInt(env("NBA_REDIS_PORT", "6379"));

        Metrics.serve(Integer.parseInt(env("NBA_METRICS_PORT", "9407")));

        JedisPooled redis = new JedisPooled(redisHost, redisPort);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps(bootstrap));
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(bootstrap));
        consumer.subscribe(List.of(factsTopic));
        log.info("up: in=" + factsTopic + " -> comms count facts");

        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : recs) {
                Metrics.counter("nba_datalake_records_total", "topic", r.topic()).increment();
                try { count(r.value(), redis, producer, factsTopic, memberFacts); }
                catch (Exception e) {
                    log.warn("DLQ <- " + r.value(), e);
                    producer.send(new ProducerRecord<>(dlq, r.key(), r.value()));
                }
            }
            if (!recs.isEmpty()) consumer.commitSync();
        }
    }

    static void count(String raw, JedisPooled redis, KafkaProducer<String, String> producer,
                      String factsTopic, String memberFacts) throws Exception {
        JsonNode f = M.readTree(raw);
        String key = f.path("key").asText("");
        if (!key.startsWith("nba.actionstate.")) return;          // only workflow-state facts
        if (!"sent".equals(f.path("value").asText(""))) return;   // only the sent transition

        String entityType = f.path("entityType").asText("");
        String entityId = f.path("entityId").asText("");
        String nbaId = f.path("nbaId").asText("");
        long eventTs = f.path("eventTs").asLong(0);
        String rem = key.substring("nba.actionstate.".length());
        int dot = rem.lastIndexOf('.');
        if (dot < 0) return;
        String channel = rem.substring(dot + 1);
        String slug = nbaId + ":" + rem;

        // de-dup: count each sent transition once even if the fact is redelivered
        if (redis.sadd("nba:dl:counted", slug + ":" + eventTs) == 0) return;

        long total = bump(redis, "nba:dl:" + nbaId + ":comms");
        long chan  = bump(redis, "nba:dl:" + nbaId + ":" + channel);

        // Keys must match the cap rules exactly: global = operator.comms.totalThisWeek,
        // channel = operator.comms.{channel}sThisWeek (e.g. emailsThisWeek).
        emitCount(producer, factsTopic, memberFacts, entityType, entityId, nbaId, "operator.comms.totalThisWeek", total);
        emitCount(producer, factsTopic, memberFacts, entityType, entityId, nbaId, "operator.comms." + channel + "sThisWeek", chan);
        log.info("sent " + slug + " -> totalThisWeek=" + total + " " + channel + "sThisWeek=" + chan);
    }

    static long bump(JedisPooled redis, String key) {
        long v = redis.incr(key);
        redis.expire(key, WEEK_SECONDS);   // rolling ~week
        return v;
    }

    static void emitCount(KafkaProducer<String, String> producer, String factsTopic, String memberFacts,
                          String entityType, String entityId, String nbaId, String key, long value) {
        ObjectNode fact = M.createObjectNode();
        fact.put("entityType", entityType);
        fact.put("entityId", entityId);
        fact.put("nbaId", nbaId);
        fact.put("key", key);
        fact.put("value", value);
        fact.put("valueType", "LONG");
        fact.put("eventTs", System.currentTimeMillis());
        fact.put("source", "datalake");
        String json;
        try { json = M.writeValueAsString(fact); } catch (Exception e) { throw new RuntimeException(e); }
        // keyed by memberId (entityType:entityId) so a member's facts co-locate on one partition (the fact
        // key rides the body); member.facts is transport, not a per-fact compacted store.
        String kafkaKey = entityType + ":" + entityId;
        ProducerRecord<String, String> rec = new ProducerRecord<>(memberFacts, kafkaKey, json);
        rec.headers().add("kind", "count".getBytes(java.nio.charset.StandardCharsets.UTF_8));  // -> snapshot builder re-routes to nba.facts (ML fatigue feature)
        producer.send(rec);
    }

    static Properties consumerProps(String bootstrap) {
        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, "datalake-comms-counter");
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");   // count only sends from now on
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

    static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? d : v;
    }
}
