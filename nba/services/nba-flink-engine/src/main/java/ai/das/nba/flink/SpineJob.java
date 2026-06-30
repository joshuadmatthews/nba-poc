package ai.das.nba.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static ai.das.nba.flink.NbaFlinkApp.*;

/** Assembles the whole NBA spine onto the StreamExecutionEnvironment. Each stage is a separate method so the
 *  job reads top-to-bottom like the data flow. Stages are wired by Kafka topics (the live system's topics). */
final class SpineJob {
    private SpineJob() {}

    static void build(StreamExecutionEnvironment env, Conf cfg) {
        stageSnapshot(env, cfg);
        stageRules(env, cfg);
        if (cfg.scoreEnabled) stageScore(env, cfg);   // OFF in prod: the Databricks RL scorer owns nba.score.*
        stageRoute(env, cfg);
        stageStateMachine(env, cfg);
        stageActionLayer(env, cfg);
    }

    /** STAGE 1 — SNAPSHOT: member.facts -> classify (defs/firehose/dlq side-outputs) -> keyed event-time LWW -> nba.snapshots. */
    static void stageSnapshot(StreamExecutionEnvironment env, Conf cfg) {
        DataStream<FactRecord> facts = sourceStream(env, cfg, cfg.memberFacts, "facts");
        // Shadow mode: the pipeline's own loop-back facts (scores/router/state/dispositions) land on the .shadow
        // sibling; union it so the snapshot sees both live external facts AND the Flink loop (self-contained).
        // Authoritative writes the real member.facts, which this entry source already reads — no union needed.
        if (!cfg.authoritative) facts = facts.union(sourceStream(env, cfg, cfg.sink(cfg.memberFacts), "facts-loop"));
        SingleOutputStreamOperator<SnapMsg> classified =
                facts.process(new ClassifyResolveFn(cfg.redisHost, cfg.redisPort)).name("classify");
        SingleOutputStreamOperator<KafkaOut> snapshots =
                classified.keyBy(m -> m.nbaId)
                        .process(new SnapshotLwwFn(cfg.redisHost, cfg.redisPort, cfg.redisWriteThrough))
                        .name("snapshot-lww");
        sinkTo(snapshots, cfg, cfg.sink(cfg.snapshots), "sink-snapshots");
        sinkTo(classified.getSideOutput(ClassifyResolveFn.DEFS_TAG), cfg, cfg.sink(cfg.definitions), "sink-defs");
        sinkTo(classified.getSideOutput(ClassifyResolveFn.FIREHOSE_TAG), cfg, cfg.sink(cfg.facts), "sink-firehose");
        sinkTo(classified.getSideOutput(ClassifyResolveFn.DLQ_TAG), cfg, cfg.sink(cfg.dlq), "sink-dlq");
    }

    /** STAGE 2 — RULES/ELIGIBILITY (filled in RulesStage). */
    static void stageRules(StreamExecutionEnvironment env, Conf cfg) { RulesStage.wire(env, cfg); }

    /** STAGE 3 — SCORE (filled in ScoreStage). Skipped when NBA_FLINK_SCORE=off (prod: Databricks RL scorer scores). */
    static void stageScore(StreamExecutionEnvironment env, Conf cfg) { ScoreStage.wire(env, cfg); }

    /** STAGE 4 — ROUTE (filled in RouteStage). */
    static void stageRoute(StreamExecutionEnvironment env, Conf cfg) { RouteStage.wire(env, cfg); }

    /** STAGE 5 — STATE MACHINE (filled in StateMachineStage). */
    static void stageStateMachine(StreamExecutionEnvironment env, Conf cfg) { StateMachineStage.wire(env, cfg); }

    /** STAGE 6 — ACTION LAYER (filled in ActionLayerStage). */
    static void stageActionLayer(StreamExecutionEnvironment env, Conf cfg) { ActionLayerStage.wire(env, cfg); }
}
