package ai.das.nba.flink;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static ai.das.nba.flink.NbaFlinkApp.sinkTo;
import static ai.das.nba.flink.NbaFlinkApp.sourceStream;

/**
 * STAGE 2 — RULES/ELIGIBILITY: snapshots (keyed by nbaId) evaluated against the BROADCAST definitions -> nba.evaluations.
 * Reads the previous Flink stage's snapshot topic (self-contained pipeline) + the live definitions topic.
 */
final class RulesStage {
    private RulesStage() {}

    static void wire(StreamExecutionEnvironment env, Conf cfg) {
        DataStream<FactRecord> snaps = sourceStream(env, cfg, cfg.sink(cfg.snapshots), "rules-snaps");
        // definitions is COMPACTED config -> read EARLIEST so the broadcast loads the full current action/rule set
        DataStream<FactRecord> defs = sourceStream(env, cfg, cfg.definitions, "rules-defs", OffsetsInitializer.earliest());
        BroadcastStream<FactRecord> defsB = defs.broadcast(RulesFn.DEFS_DESC);
        SingleOutputStreamOperator<KafkaOut> evals = snaps
                .keyBy((KeySelector<FactRecord, String>) r -> r.key == null ? "" : r.key)
                .connect(defsB)
                .process(new RulesFn())
                .name("rules-eligibility");
        sinkTo(evals, cfg, cfg.sink(cfg.evaluations), "sink-evals");
    }
}
