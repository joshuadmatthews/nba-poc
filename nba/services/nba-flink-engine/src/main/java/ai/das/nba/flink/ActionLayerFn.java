package ai.das.nba.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * STAGE 6 — ACTION LAYER (simulated, the Databricks-free analog of ActionLayer.java's delivery walk). Consumes
 * DISPATCH/CANCEL activations (keyed by member,action,channel) and produces the canonical delivery dispositions
 * the state machine awaits: a DISPATCH delivers (after a step delay) as PRESENTED; a CANCEL that catches the
 * send first answers SUPPRESSED, otherwise SUPPRESS_FAILED. Dispositions ride nba.member.facts (kind=disposition).
 */
public class ActionLayerFn extends KeyedProcessFunction<String, FactRecord, KafkaOut> {
    private final long stepMs;
    private transient ValueState<String> pending;   // the DISPATCH activation awaiting delivery
    private transient ValueState<Long> timerAt;

    public ActionLayerFn(long stepMs) { this.stepMs = stepMs; }

    @Override
    public void open(Configuration p) {
        pending = getRuntimeContext().getState(new ValueStateDescriptor<>("al-pending", Types.STRING));
        timerAt = getRuntimeContext().getState(new ValueStateDescriptor<>("al-timer", Types.LONG));
    }

    @Override
    public void processElement(FactRecord act, Context ctx, Collector<KafkaOut> out) throws Exception {
        JsonNode v;
        try { v = SnapshotLogic.M.readTree(act.value); } catch (Exception e) { return; }
        String op = v.path("op").asText("");
        long now = ctx.timerService().currentProcessingTime();
        if ("DISPATCH".equals(op)) {
            pending.update(act.value);
            long t = now + Math.max(1L, stepMs);
            timerAt.update(t);
            ctx.timerService().registerProcessingTimeTimer(t);
        } else if ("CANCEL".equals(op)) {
            String p = pending.value();
            if (p != null) {                                  // caught before delivery -> raw "Cancelled" (SM classifies -> SUPPRESSED)
                out.collect(disposition(p, "Cancelled"));
                Long ta = timerAt.value(); if (ta != null) ctx.timerService().deleteProcessingTimeTimer(ta);
                pending.clear(); timerAt.clear();
            } else {                                          // already delivered -> raw "AlreadySent" (SM -> SUPPRESS_FAILED)
                out.collect(disposition(act.value, "AlreadySent"));
            }
        }
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<KafkaOut> out) throws Exception {
        String p = pending.value();
        if (p != null) {                                      // deliver -> raw "Delivered" (SM classifies -> PRESENTED)
            out.collect(disposition(p, "Delivered"));
            pending.clear(); timerAt.clear();
        }
    }

    /** Build a disposition fact from the activation it answers — keyed by the member, kind=disposition.
     *  Emits the RAW provider status only (`value`); the state machine (StateMachineFn) classifies raw -> canonical
     *  via DispositionClassifier — the action-layer no longer decides the lifecycle state. */
    private static KafkaOut disposition(String activationJson, String raw) {
        try {
            JsonNode a = SnapshotLogic.M.readTree(activationJson);
            String et = a.path("entityType").asText("OPERATOR"), eid = a.path("entityId").asText("");
            ObjectNode o = SnapshotLogic.M.createObjectNode();
            o.put("value", raw);                              // RAW status only; the SM classifies it
            o.put("trackingId", a.path("trackingId").asText(""));
            o.put("entityType", et); o.put("entityId", eid);
            o.put("channel", a.path("channel").asText("")); o.put("source", "action-layer");
            return KafkaOut.of(et + ":" + eid, SnapshotLogic.M.writeValueAsString(o), "disposition");
        } catch (Exception e) { return KafkaOut.of("", "{}", "disposition"); }
    }
}
