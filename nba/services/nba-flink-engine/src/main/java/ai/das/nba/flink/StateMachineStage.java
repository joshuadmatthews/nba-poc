package ai.das.nba.flink;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static ai.das.nba.flink.NbaFlinkApp.sinkTo;
import static ai.das.nba.flink.NbaFlinkApp.sourceStream;

/**
 * STAGE 5 — STATE MACHINE (replaces Temporal), now feature-complete with the rate limiter + sibling dedup:
 *   member.facts(kind=router|disposition) -> StateEventMapper
 *     -> [member-keyed] MemberDedupFn (debounceLost; DEBOUNCED losers to member.facts)
 *     -> [(member,action,channel)-keyed] StateMachineFn + broadcast(nba.definitions for the ThrottleGate)
 *        -> state facts (member.facts, kind=state) + DISPATCH/CANCEL (ACT side-output)
 *     -> [(nbaId,channel)-keyed] TouchFn (the channel_touch counter -> escalating touchKeys) -> nba.activations.
 */
final class StateMachineStage {
    private StateMachineStage() {}

    static void wire(StreamExecutionEnvironment env, Conf cfg) {
        SingleOutputStreamOperator<StateEvent> events =
                sourceStream(env, cfg, cfg.sink(cfg.memberFacts), "sm-facts").flatMap(new StateEventMapper()).name("sm-events");

        // member-keyed debounce-dedup (debounceLost) — gates CREATEs against siblings; DEBOUNCED losers side-out
        SingleOutputStreamOperator<StateEvent> deduped = events
                .keyBy((KeySelector<StateEvent, String>) e -> MemberDedupFn.member(e.key))
                .process(new MemberDedupFn())
                .name("debounce-dedup");
        sinkTo(deduped.getSideOutput(MemberDedupFn.DEBOUNCED_TAG), cfg, cfg.sink(cfg.memberFacts), "sink-debounced");

        // lifecycle state machine, connected to the definitions broadcast so the ThrottleGate has its caps + facts
        BroadcastStream<FactRecord> defs = sourceStream(env, cfg, cfg.definitions, "sm-defs", OffsetsInitializer.earliest())
                .broadcast(StateMachineFn.DEFS_DESC);
        SingleOutputStreamOperator<KafkaOut> states = deduped
                .keyBy((KeySelector<StateEvent, String>) e -> e.key)
                .connect(defs)
                .process(new StateMachineFn(cfg.debounceSeconds, cfg.throttleRecheckSeconds))
                .name("state-machine");
        sinkTo(states, cfg, cfg.sink(cfg.memberFacts), "sink-states");

        // channel_touch: re-key the DISPATCH/CANCEL activations by (nbaId,channel) and run the monotonic touch
        // counter (TouchFn) BEFORE the activations sink, so both the Kafka topic AND the ActionLayerFn (which
        // reads that topic) see the escalated contentKey. Replaces Temporal's Postgres channel_touch UPSERT.
        SingleOutputStreamOperator<KafkaOut> activations = states.getSideOutput(StateMachineFn.ACT_TAG)
                .keyBy((KeySelector<KafkaOut, String>) TouchFn::touchKey)
                .process(new TouchFn())
                .name("channel-touch");
        sinkTo(activations, cfg, cfg.sink(cfg.activations), "sink-activations");
    }
}
