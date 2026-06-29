package ai.das.nba.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NBA Flink engine — the WHOLE NBA spine as ONE Apache Flink DataStream job, REPLACING the five bespoke
 * services (snapshot-builder, rules-engine, journey-scorer, action-router) AND Temporal (the per-action
 * lifecycle becomes a KeyedProcessFunction with timers). Third reference implementation alongside the classic
 * Redis spine and the Kafka Streams engine.
 *
 * Layers (each a Flink operator; Kafka topics are the connective tissue, exactly as the live system, so this is
 * a drop-in-shaped, feature-complete port — same topics, same flow, one runtime):
 *   1. SNAPSHOT   member.facts -> classify (defs/firehose/dlq side-outputs) -> keyed event-time LWW -> nba.snapshots
 *   2. RULES      nba.snapshots + broadcast(nba.definitions) -> Drools KieBase eval -> nba.evaluations
 *   3. SCORE      nba.evaluations -> journey scores -> nba.member.facts (kind=score)   [loops via Kafka]
 *   4. ROUTE      nba.evaluations -> per-member slot/dedup/suppress -> nba.member.facts (kind=router)
 *   5. STATE M/C  member.facts(kind=router|disposition) -> KeyedProcessFunction + timers -> nba.actionstate.* + nba.activations
 *   6. ACT LAYER  nba.activations -> simulated send -> dispositions -> nba.member.facts (kind=disposition)  [loops via Kafka]
 *
 * Runs EMBEDDED via a local MiniCluster (env.execute()) so it deploys as one container.
 */
public class NbaFlinkApp {
    private static final Logger log = LoggerFactory.getLogger(NbaFlinkApp.class);

    public static void main(String[] args) throws Exception {
        Conf cfg = Conf.fromEnv();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(cfg.parallelism);
        env.enableCheckpointing(cfg.checkpointMs);   // exactly-once state + (with the EOS sink) end-to-end EOS

        log.info("NBA Flink engine starting mode={} bootstrap={} parallelism={}",
                cfg.authoritative ? "AUTHORITATIVE" : "shadow", cfg.bootstrap, cfg.parallelism);

        SpineJob.build(env, cfg);

        env.execute("nba-flink-engine");
    }

    // ── shared source/sink builders ──
    static KafkaSource<FactRecord> source(Conf cfg, String topic, String groupSuffix, OffsetsInitializer init) {
        return KafkaSource.<FactRecord>builder()
                .setBootstrapServers(cfg.bootstrap)
                .setTopics(topic)
                .setGroupId(cfg.group + "-" + groupSuffix)
                .setStartingOffsets(init)
                .setDeserializer(new FactDeserializer())
                .build();
    }

    /** Event streams start at the live edge (additive, no history replay). */
    static DataStream<FactRecord> sourceStream(StreamExecutionEnvironment env, Conf cfg, String topic, String groupSuffix) {
        return sourceStream(env, cfg, topic, groupSuffix, OffsetsInitializer.latest());
    }

    /** COMPACTED config topics (nba.definitions) must start at EARLIEST so the broadcast loads the full current
     *  state (the latest record per key) — at latest the rules engine would see an empty definitions set. */
    static DataStream<FactRecord> sourceStream(StreamExecutionEnvironment env, Conf cfg, String topic, String groupSuffix, OffsetsInitializer init) {
        return env.fromSource(source(cfg, topic, groupSuffix, init), WatermarkStrategy.noWatermarks(), "src-" + groupSuffix);
    }

    static KafkaSink<KafkaOut> sink(Conf cfg, String topic) {
        return KafkaSink.<KafkaOut>builder()
                .setBootstrapServers(cfg.bootstrap)
                .setRecordSerializer(new KafkaOutSerializer(topic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    static void sinkTo(SingleOutputStreamOperator<KafkaOut> s, Conf cfg, String topic, String name) {
        s.sinkTo(sink(cfg, topic)).name(name);
    }

    static void sinkTo(DataStream<KafkaOut> s, Conf cfg, String topic, String name) {
        s.sinkTo(sink(cfg, topic)).name(name);
    }
}
