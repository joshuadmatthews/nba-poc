package ai.das.nba.flink;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Turns nba.member.facts into keyed {@link StateEvent}s for the state machine: kind=router activations
 * (CREATE/SUPPRESS/SOFT_COMPLETE/HARD_COMPLETE) and kind=disposition deliveries, both keyed by
 * "{nbaId}:{actionId}:{channel}". Everything else (member attributes, scores, state echoes) is dropped.
 */
public class StateEventMapper implements FlatMapFunction<FactRecord, StateEvent> {
    @Override
    public void flatMap(FactRecord rec, Collector<StateEvent> out) throws Exception {
        if (rec.kind == null) return;
        JsonNode v;
        try { v = SnapshotLogic.M.readTree(rec.value); } catch (Exception e) { return; }
        if ("router".equals(rec.kind)) {
            String op = v.path("op").asText("");
            if (op.isEmpty()) return;
            String key = v.path("nbaId").asText("") + ":" + v.path("actionId").asText("") + ":" + v.path("channel").asText("");
            out.collect(new StateEvent(key, op, rec.value));     // op == CREATE | SUPPRESS | SOFT_COMPLETE | HARD_COMPLETE
        } else if ("disposition".equals(rec.kind)) {
            String tracking = v.path("trackingId").asText("");
            String key = trackingKey(tracking);
            if (key == null) return;
            out.collect(new StateEvent(key, "DISPOSITION", rec.value));
        }
    }

    /** "nba-ca:{nbaId}:{actionId}:{channel}|{corr}" -> "{nbaId}:{actionId}:{channel}". */
    static String trackingKey(String tracking) {
        if (tracking == null || !tracking.startsWith("nba-ca:")) return null;
        String body = tracking.substring("nba-ca:".length());
        int bar = body.indexOf('|');
        return bar >= 0 ? body.substring(0, bar) : body;
    }
}
