package ai.das.nba.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * NBA decision engine — a Kafka Streams port of the stream-processor SPINE (snapshot-builder +
 * rules-engine + action-router), as ONE app over co-located keyed state. This is a faithful 1:1
 * port of the existing processing SHAPE: it reuses the classic services' pure functions (event-time
 * LWW snapshot merge, the structured-logic -> Drools KieBase build, the router slot/dedup decision)
 * and only swaps the shell (hand-rolled EOS poll loops -> KStreams KTable / GlobalKTable / Processor).
 *
 * OUT OF SCOPE (unchanged): scoring stays the Databricks model serving endpoint; features stay in
 * Lakebase (both read by the existing dbx scorer, which consumes nba.evaluations and emits nba.score.*
 * facts back exactly as today). This app does NO ML, NO feature store, NO model.
 *
 * SAFETY — additive + setting-gated:
 *   NBA_DECISION_ENGINE_MODE = shadow | authoritative   (default: shadow)
 *     shadow        — computes the full spine, writes its outputs to SHADOW topics/keys, drives nothing.
 *                     Lets us diff against the classic spine + measure latency with zero blast radius.
 *     authoritative — writes the real sinks (nba.snapshots, nba.evaluations, nba.member.facts kind=router,
 *                     and the nba:snapshot / nba:eligibility Redis mirrors) so downstream (hot path,
 *                     Temporal bridge, dbx scorer) is byte-for-byte unaffected — the engine just becomes
 *                     the writer. Flip the master switch NBA_DECISION_ENGINE=kstreams to cut over; instant
 *                     rollback by flipping back (the classic services keep running untouched).
 */
public class DecisionEngine {
    private static final Logger log = LoggerFactory.getLogger(DecisionEngine.class);
    static final ObjectMapper M = new ObjectMapper();

    /** Resolved runtime config for the topology stages (topics + shadow/authoritative sink selection). */
    static final class Conf {
        final boolean authoritative;
        final String memberFacts, snapshots, evaluations, definitions, facts, dlq;
        final String redisHost; final int redisPort;
        Conf(boolean authoritative, String memberFacts, String snapshots, String evaluations,
             String definitions, String facts, String dlq, String redisHost, int redisPort) {
            this.authoritative = authoritative; this.memberFacts = memberFacts; this.snapshots = snapshots;
            this.evaluations = evaluations; this.definitions = definitions; this.facts = facts; this.dlq = dlq;
            this.redisHost = redisHost; this.redisPort = redisPort;
        }
        /** Sink topic for a stage output: the real topic when authoritative, a ".shadow" sibling otherwise. */
        String sink(String topic) { return authoritative ? topic : topic + ".shadow"; }
    }

    public static void main(String[] args) {
        String bootstrap = env("NBA_BOOTSTRAP", "nba-redpanda:9092");
        String appId     = env("NBA_ENGINE_APP_ID", "nba-decision-engine");
        String stateDir  = env("NBA_STATE_DIR", "/tmp/nba-decision-engine");
        boolean authoritative = "authoritative".equalsIgnoreCase(env("NBA_DECISION_ENGINE_MODE", "shadow"));
        // Advertised host:port for Interactive Queries — the snapshot READ surface that replaces nba:snapshot.
        // Each pod advertises its own address so cross-pod reads can be routed/redirected to the key's owner.
        String advertised = env("NBA_ENGINE_ADVERTISED", "nba-decision-engine:7020");
        String[] adv = advertised.split(":");
        HostInfo self = new HostInfo(adv[0], Integer.parseInt(adv[1]));

        Conf cfg = new Conf(
                authoritative,
                env("NBA_MEMBER_FACTS", "nba.member.facts"),
                env("NBA_TOPIC_OUT", "nba.snapshots"),
                env("NBA_EVALUATIONS_TOPIC", "nba.evaluations"),
                env("NBA_DEFINITIONS_TOPIC", "nba.definitions"),
                env("NBA_FACTS_TOPIC", "nba.facts"),
                env("NBA_DLQ", "nba.dlq.decision-engine"),
                env("NBA_REDIS_HOST", "nba-redis"),
                Integer.parseInt(env("NBA_REDIS_PORT", "6379")));

        // Prometheus /metrics on a side port (no HTTP surface otherwise).
        Metrics.serve(Integer.parseInt(env("NBA_METRICS_PORT", "9410")));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        // EXACTLY-ONCE: replaces snapshot-builder's hand-rolled read-process-write Kafka transaction.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        // COMMIT INTERVAL — the dominant async-latency knob: under EOS the nba.snapshots/evaluations emits become
        // visible downstream only at COMMIT. Lower = fresher emits, but more txn commits (broker overhead + commit
        // markers on every touched partition), smaller producer batches, and LESS cache coalescing (more output
        // records). Default 100ms (the EOS default); tune per environment — low at POC, weigh throughput at scale.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Integer.parseInt(env("NBA_COMMIT_INTERVAL_MS", "100")));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        // SCALE: warm standby for fast failover/rescale (per the "more pods" plan — parallelism ceiling is the
        // input-topic partition count, so provision partitions for the max pod count up front).
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, Integer.parseInt(env("NBA_STANDBY_REPLICAS", "1")));
        // Advertise this pod for IQ key routing (queryMetadataForKey resolves the owning pod by this address).
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, advertised);
        // Offset reset: 'earliest' (default) rebuilds every member's snapshot into state, parity with snapshot-builder;
        // 'latest' starts at the live edge (used for the shadow latency test so we don't replay history first).
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), env("NBA_OFFSET_RESET", "earliest"));

        // PARITY: keep the snapshot LEAN — refresh the rule-facts set from nba:rulefacts (union of rule factsUsed)
        // so the engine snapshots only what rules care about, and queue removed keys for the prune punctuator.
        startRuleFactsRefresh(cfg.redisHost, cfg.redisPort);

        Topology topology = SpineTopology.build(cfg);

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);
        streams.setStateListener((next, prev) -> log.info("state {} -> {}", prev, next));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { streams.close(); latch.countDown(); }));

        log.info("starting engine app={} mode={} bootstrap={} (in={} -> snapshots={}, evals={}, router->{})",
                appId, authoritative ? "AUTHORITATIVE" : "shadow", bootstrap,
                cfg.memberFacts, cfg.sink(cfg.snapshots), cfg.sink(cfg.evaluations), cfg.sink(cfg.memberFacts));
        streams.start();
        // The IQ read surface (replaces the nba:snapshot + nba:eligibility Redis reads). Serves 503 until RUNNING.
        IqServer.serve(self, streams, SpineTopology.SNAPSHOT_STORE, SpineTopology.ELIGIBILITY_STORE);
        try { latch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    /** Refresh the lean-filter fact set from nba:rulefacts (union of every rule's factsUsed) + queue de-referenced
     *  keys for pruning. Daemon thread, ~10s, fail-open (empty set -> snapshot all). Mirrors snapshot-builder. */
    static void startRuleFactsRefresh(String redisHost, int redisPort) {
        redis.clients.jedis.JedisPooled redis = new redis.clients.jedis.JedisPooled(redisHost, redisPort);
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    java.util.Set<String> next = redis.smembers("nba:rulefacts");
                    if (next == null) next = java.util.Set.of();
                    java.util.Set<String> prev = SnapshotProcessor.RULE_FACTS;
                    SnapshotProcessor.RULE_FACTS = next;
                    if (!prev.isEmpty() && !next.isEmpty()) {                 // a real shrink -> queue the removed keys for pruning
                        java.util.Set<String> removed = new java.util.HashSet<>(prev);
                        removed.removeAll(next);
                        if (!removed.isEmpty()) SnapshotProcessor.PENDING_PRUNE.addAll(removed);
                    }
                } catch (Exception e) { log.warn("rulefacts refresh failed", e); }
                try { Thread.sleep(10_000); } catch (InterruptedException ie) { return; }
            }
        }, "rulefacts-refresh");
        t.setDaemon(true);
        t.start();
    }

    static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? d : v;
    }
}
