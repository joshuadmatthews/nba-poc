package ai.das.nba.flink;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static ai.das.nba.flink.NbaFlinkApp.sinkTo;
import static ai.das.nba.flink.NbaFlinkApp.sourceStream;

/**
 * STAGE 6 — ACTION LAYER: activations (keyed by member,action,channel) -> simulated delivery -> dispositions
 * back onto nba.member.facts (kind=disposition), closing the loop into the state machine.
 */
final class ActionLayerStage {
    private ActionLayerStage() {}

    static void wire(StreamExecutionEnvironment env, Conf cfg) {
        SingleOutputStreamOperator<KafkaOut> dispositions =
                sourceStream(env, cfg, cfg.sink(cfg.activations), "al-acts")
                        .keyBy((KeySelector<FactRecord, String>) r -> r.key == null ? "" : r.key)
                        .process(new ActionLayerFn(cfg.dispositionStepMs))
                        .name("action-layer");
        sinkTo(dispositions, cfg, cfg.sink(cfg.memberFacts), "sink-dispositions");
    }
}
