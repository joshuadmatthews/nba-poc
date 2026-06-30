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
 * The channel_touch counter — the Flink-keyed-state analog of Temporal's {@code touchTemplate} (the Postgres
 * {@code channel_touch} UPSERT, see nba-temporal ActionActivitiesImpl#touchTemplate). Keyed by
 * {@code nbaId + ":" + channel}, it holds a MONOTONIC per-(nbaId,channel) send count in {@link ValueState} that
 * counts EVERY send on the channel (any action) and never resets. On a DISPATCH activation that carries the
 * dispatched action's {@code touchKeys} (channels[].touchKeys), it bumps the count and rewrites the activation's
 * {@code contentKey} to {@code touchKeys[min(n,len)-1]} (escalating touch templates, capped at the last). An
 * activation with no/empty touchKeys passes through unchanged (the base contentKey stands). Self-contained in
 * Flink state — no Postgres, which is the whole point of the Databricks-free port.
 */
public class TouchFn extends KeyedProcessFunction<String, KafkaOut, KafkaOut> {
    private transient ValueState<Long> count;

    @Override
    public void open(Configuration p) {
        count = getRuntimeContext().getState(new ValueStateDescriptor<>("touch-count", Types.LONG));
    }

    /** The keyed-state key for an activation: the monotonic counter is per-(nbaId,channel) across ALL actions. */
    public static String touchKey(KafkaOut act) {
        try {
            JsonNode v = SnapshotLogic.M.readTree(act.value);
            return v.path("nbaId").asText("") + ":" + v.path("channel").asText("");
        } catch (Exception e) { return act.key == null ? "" : act.key; }
    }

    @Override
    public void processElement(KafkaOut act, Context ctx, Collector<KafkaOut> out) throws Exception {
        JsonNode v;
        try { v = SnapshotLogic.M.readTree(act.value); } catch (Exception e) { out.collect(act); return; }
        JsonNode tk = v.get("touchKeys");
        if (!"DISPATCH".equals(v.path("op").asText("")) || tk == null || !tk.isArray() || tk.size() == 0) {
            out.collect(act);                                  // not a touch-templated send -> pass through unchanged
            return;
        }
        long n = (count.value() == null ? 0L : count.value()) + 1;   // monotonic; counts every touch-templated send
        count.update(n);
        int idx = (int) Math.min(n, tk.size()) - 1;            // touchKeys[min(n,len)-1] — caps at the last, never resets
        ObjectNode rewritten = ((ObjectNode) v).deepCopy();
        rewritten.put("contentKey", tk.get(idx).asText(""));
        out.collect(KafkaOut.of(act.key, SnapshotLogic.M.writeValueAsString(rewritten), act.kind));
    }
}
