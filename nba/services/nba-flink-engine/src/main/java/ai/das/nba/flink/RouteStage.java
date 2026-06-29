package ai.das.nba.flink;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static ai.das.nba.flink.NbaFlinkApp.sinkTo;
import static ai.das.nba.flink.NbaFlinkApp.sourceStream;

/**
 * STAGE 4 — ROUTE: evaluations -> per-member slot/dedup/suppress + completion bridge -> nba.member.facts
 * (kind=router|completion|milestone), which the state machine + the snapshot consume.
 */
final class RouteStage {
    private RouteStage() {}

    static void wire(StreamExecutionEnvironment env, Conf cfg) {
        SingleOutputStreamOperator<KafkaOut> decisions =
                sourceStream(env, cfg, cfg.sink(cfg.evaluations), "route-evals")
                        .flatMap(new RouterFn(cfg.redisHost, cfg.redisPort, cfg.redisWriteThrough))
                        .name("route");
        sinkTo(decisions, cfg, cfg.sink(cfg.memberFacts), "sink-router");
    }
}
