package ai.das.nba.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * The sibling debounce-dedup, ported from Temporal's ActionActivities.debounceLost — but as a MEMBER-keyed
 * pre-stage (it needs the cross-(action,channel) view of one member, which the per-(member,action,channel)
 * state machine can't see). Temporal discovered siblings via Visibility; here we track them in member-keyed
 * state from the same event stream: every CREATE records the (action,channel) as CREATED+score; every
 * DISPOSITION records its delivery state. On a new CREATE we apply the same verdict:
 *   - a sibling already PRESENTED/DECLINED (a touch reached the member)  -> LOSE  (self-DEBOUNCE, drop)
 *   - a sibling still CREATED with a higher score (tiebreak smaller slug) -> LOSE
 *   - otherwise                                                          -> PROCEED (forward to the lifecycle)
 * Losers get a DEBOUNCED state fact on the side output. (Temporal's WAIT/recheck on an uncertain in-flight
 * sibling is simplified to PROCEED here — the router's per-member single-active-slot upstream already prevents
 * the common race; this closes the residual one.)
 */
public class MemberDedupFn extends KeyedProcessFunction<String, StateEvent, StateEvent> {
    public static final OutputTag<KafkaOut> DEBOUNCED_TAG = new OutputTag<>("debounced", org.apache.flink.api.common.typeinfo.TypeInformation.of(KafkaOut.class));
    private transient MapState<String, String> siblings;   // "action:channel" -> "CREATED|score" | a disposition state

    static String member(String key) { int i = key.indexOf(':'); return i < 0 ? key : key.substring(0, i); }
    static String slug(String key) { int i = key.indexOf(':'); return i < 0 ? key : key.substring(i + 1); }   // action:channel

    @Override
    public void open(Configuration p) {
        siblings = getRuntimeContext().getMapState(new MapStateDescriptor<>("siblings", Types.STRING, Types.STRING));
    }

    @Override
    public void processElement(StateEvent ev, Context ctx, Collector<StateEvent> out) throws Exception {
        String slug = slug(ev.key);
        JsonNode v = null;
        try { v = SnapshotLogic.M.readTree(ev.value); } catch (Exception ignore) {}

        switch (ev.type) {
            case "DISPOSITION" -> {
                if (v != null) siblings.put(slug, v.has("state") ? v.path("state").asText("") : v.path("value").asText(""));
                out.collect(ev);
            }
            case "HARD_COMPLETE", "SUPPRESS", "OPERATOR_SUPPRESS" -> { siblings.remove(slug); out.collect(ev); }   // terminal-ish: stop blocking
            case "SOFT_COMPLETE" -> out.collect(ev);
            case "CREATE" -> {
                double myScore = v == null ? 0 : v.path("score").asDouble(0);
                String verdict = debounceLost(slug, myScore);
                if ("LOSE".equals(verdict)) {
                    if (v != null) ctx.output(DEBOUNCED_TAG, debouncedFact(v, slug));   // visible terminal, not forwarded
                    return;
                }
                siblings.put(slug, "CREATED|" + myScore);
                out.collect(ev);
            }
            default -> out.collect(ev);
        }
    }

    /** LOSE if a sibling reached the member, or a higher CREATED sibling is racing; else PROCEED. */
    private String debounceLost(String mySlug, double myScore) throws Exception {
        for (var e : siblings.entries()) {
            String sib = e.getKey(), st = e.getValue();
            if (sib.equals(mySlug)) continue;
            if ("PRESENTED".equals(st) || "DECLINED".equals(st)) return "LOSE";
            if (st != null && st.startsWith("CREATED|")) {
                double sc = 0; try { sc = Double.parseDouble(st.substring(8)); } catch (Exception ignore) {}
                if (sc > myScore || (sc == myScore && sib.compareTo(mySlug) < 0)) return "LOSE";
            }
        }
        return "PROCEED";
    }

    private static KafkaOut debouncedFact(JsonNode createAct, String slug) {
        String et = createAct.path("entityType").asText("OPERATOR"), eid = createAct.path("entityId").asText("");
        ObjectNode o = SnapshotLogic.M.createObjectNode();
        o.put("entityType", et); o.put("entityId", eid);
        o.put("nbaId", createAct.path("nbaId").asText(""));
        o.put("key", "nba.actionstate." + slug.replace(':', '.'));
        o.put("value", "DEBOUNCED"); o.put("valueType", "STRING");
        o.put("eventTs", createAct.path("eventTs").asLong(0)); o.put("source", "member-dedup");
        try { return KafkaOut.of(et + ":" + eid, SnapshotLogic.M.writeValueAsString(o), "state"); }
        catch (Exception e) { return KafkaOut.of(et + ":" + eid, "{}", "state"); }
    }
}
