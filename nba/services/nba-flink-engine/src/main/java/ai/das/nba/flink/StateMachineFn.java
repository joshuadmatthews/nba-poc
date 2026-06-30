package ai.das.nba.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Set;

/**
 * STAGE 5 — the disposition-driven LIFECYCLE STATE MACHINE, replacing Temporal's ChannelActionWorkflow, NOW
 * feature-complete with the THROTTLE GATE (the per-channel token-bucket rate limiter — {@link ThrottleGate}).
 * Keyed by (member,action,channel); the nba.definitions stream is broadcast in to feed the per-operator-instance
 * ThrottleGate its rate caps + lake rate (the Flink analog of Temporal's worker-shared ThrottleGate).
 *
 *   CREATE -> CREATED --[debounce timer]--> THROTTLE GATE:
 *       SEND     -> DISPATCH -> IN_PROCESS --[dispositions]--> PRESENTED -> SOFT_COMPLETED -> HARD_COMPLETED|...
 *       WAIT     -> hold in THROTTLED (enterBacklog) --[recheck timer]--> re-admit (trickle as the bucket refills)
 *       SUPPRESS -> SUPPRESSED(reason=throttle)            ; --[TTL timer, no hard completion]--> EXPIRED
 *
 * Sibling debounce-dedup (debounceLost) is handled UPSTREAM by {@link MemberDedupFn} (a member-keyed pre-stage),
 * since it needs the member-level view across (member,action,channel) keys.
 */
public class StateMachineFn extends KeyedBroadcastProcessFunction<String, StateEvent, FactRecord, KafkaOut> {
    public static final OutputTag<KafkaOut> ACT_TAG = new OutputTag<>("activations", TypeInformation.of(KafkaOut.class));
    /** Broadcast: raw nba.definitions (key="TYPE:id" -> def JSON) — feeds the ThrottleGate caps + throttle facts. */
    public static final MapStateDescriptor<String, String> DEFS_DESC =
            new MapStateDescriptor<>("sm-definitions", Types.STRING, Types.STRING);
    private static final Set<String> TERMINAL = Set.of("FAILED", "SUPPRESSED", "HARD_COMPLETED", "EXPIRED", "DEBOUNCED");

    private final long debounceMs;
    private final long throttleRecheckMs;
    private transient ValueState<SmState> state;
    private transient ThrottleGate throttle;   // per-operator-instance shared object (fed by the broadcast)

    public StateMachineFn(long debounceSeconds, long throttleRecheckSeconds) {
        this.debounceMs = debounceSeconds * 1000L;
        this.throttleRecheckMs = throttleRecheckSeconds * 1000L;
    }

    @Override
    public void open(Configuration p) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("sm", TypeInformation.of(SmState.class)));
        throttle = new ThrottleGate();
    }

    /** Definitions broadcast -> feed the ThrottleGate (THROTTLE facts + CHANNEL_RULE caps) + keep the raw store. */
    @Override
    public void processBroadcastElement(FactRecord def, Context ctx, Collector<KafkaOut> out) throws Exception {
        if (def.key == null) return;
        var st = ctx.getBroadcastState(DEFS_DESC);
        if (def.value == null || def.value.isEmpty()) { st.remove(def.key); return; }
        st.put(def.key, def.value);
        int i = def.key.indexOf(':'); if (i < 0) return;
        String type = def.key.substring(0, i), id = def.key.substring(i + 1);
        try {
            JsonNode v = SnapshotLogic.M.readTree(def.value);
            if ("THROTTLE".equals(type)) throttle.onFact(v);
            else if ("CHANNEL_RULE".equals(type)) throttle.onChannelRule(id, v, false);
        } catch (Exception ignore) { /* skip bad def */ }
    }

    @Override
    public void processElement(StateEvent ev, ReadOnlyContext ctx, Collector<KafkaOut> out) throws Exception {
        JsonNode v;
        try { v = SnapshotLogic.M.readTree(ev.value); } catch (Exception e) { return; }
        SmState st = state.value();
        long now = ctx.timerService().currentProcessingTime();

        switch (ev.type) {
            case "CREATE" -> {
                if (st != null && !"DONE".equals(st.phase)) return;          // dedup: already live
                st = new SmState();
                st.nbaId = v.path("nbaId").asText(""); st.entityType = v.path("entityType").asText("OPERATOR");
                st.entityId = v.path("entityId").asText(""); st.actionId = v.path("actionId").asText("");
                st.channel = v.path("channel").asText(""); st.name = v.path("name").asText("");
                st.contentKey = v.path("contentKey").asText(""); st.ttlSeconds = v.path("ttlSeconds").asLong(0);
                st.correlationId = v.path("correlationId").asText(""); st.score = v.path("score").asDouble(0);
                st.phase = "DEBOUNCE";
                emit(st, "CREATED", out, ctx);
                st.debounceAt = now + debounceMs;
                ctx.timerService().registerProcessingTimeTimer(st.debounceAt);
                state.update(st);
            }
            case "SUPPRESS", "OPERATOR_SUPPRESS" -> {
                if (st == null || "DONE".equals(st.phase)) return;
                if ("DEBOUNCE".equals(st.phase) || "THROTTLE".equals(st.phase)) {   // pre-send: terminal now
                    boolean operator = "OPERATOR_SUPPRESS".equals(ev.type);
                    if ("THROTTLE".equals(st.phase) && st.inBacklog) { throttle.exitBacklog(st.channel); st.inBacklog = false; }
                    emit(st, operator ? "SUPPRESSED" : "DEBOUNCED", out, ctx);
                    terminal(st, ctx);
                } else if ("TRACKING".equals(st.phase) && !st.cancelSent) {         // post-send: ask the layer
                    st.cancelSent = true;
                    emit(st, "SUPPRESSING", out, ctx);
                    emitActivation(st, "CANCEL", ctx);
                    state.update(st);
                }
            }
            case "SOFT_COMPLETE" -> {
                if (st == null || "DONE".equals(st.phase) || st.emittedSoft) return;
                if ("TRACKING".equals(st.phase)) { st.emittedSoft = true; emit(st, "SOFT_COMPLETED", out, ctx); state.update(st); }
            }
            case "HARD_COMPLETE" -> {
                if (st == null || "DONE".equals(st.phase)) return;
                st.hardCompleted = true; emit(st, "HARD_COMPLETED", out, ctx); terminal(st, ctx);
            }
            case "DISPOSITION" -> {
                if (st == null || !"TRACKING".equals(st.phase)) return;
                String d = v.has("state") ? v.path("state").asText("") : v.path("value").asText("");
                if (d.isEmpty()) return;
                if ("SUPPRESS_FAILED".equals(d)) { st.cancelSent = false; state.update(st); return; }
                emit(st, d, out, ctx);
                if (TERMINAL.contains(d)) terminal(st, ctx); else state.update(st);
            }
            default -> { /* ignore */ }
        }
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<KafkaOut> out) throws Exception {
        SmState st = state.value();
        if (st == null) return;
        if ("DEBOUNCE".equals(st.phase) && st.debounceAt > 0 && ts >= st.debounceAt) {
            if (st.suppressRequested) { emit(st, st.operatorSuppress ? "SUPPRESSED" : "DEBOUNCED", out, ctx); terminal(st, ctx); return; }
            st.debounceAt = 0;
            gate(st, ctx, out);                                              // first throttle admission
        } else if ("THROTTLE".equals(st.phase) && st.throttleAt > 0 && ts >= st.throttleAt) {
            st.throttleAt = 0;
            gate(st, ctx, out);                                              // recheck (trickle as the bucket refills)
        } else if ("TRACKING".equals(st.phase) && st.ttlAt > 0 && ts >= st.ttlAt) {
            if (!st.hardCompleted) { emit(st, "EXPIRED", out, ctx); terminal(st, ctx); }
        }
    }

    /** The throttle gate: SEND -> dispatch; WAIT -> hold + recheck (trickle); SUPPRESS -> reroute. */
    private void gate(SmState st, ReadOnlyContext ctx, Collector<KafkaOut> out) throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        String decision = throttle.admit(st.channel, now);
        if (ThrottleGate.SEND.equals(decision)) {
            if (st.inBacklog) { throttle.exitBacklog(st.channel); st.inBacklog = false; }
            emitActivation(st, "DISPATCH", ctx);
            emit(st, "IN_PROCESS", out, ctx);
            st.phase = "TRACKING";
            st.ttlAt = now + Math.max(1L, st.ttlSeconds) * 1000L;
            ctx.timerService().registerProcessingTimeTimer(st.ttlAt);
            state.update(st);
        } else if (ThrottleGate.WAIT.equals(decision)) {
            if (!st.inBacklog) { throttle.enterBacklog(st.channel); st.inBacklog = true; }
            st.phase = "THROTTLE";
            st.throttleAt = now + throttleRecheckMs;
            ctx.timerService().registerProcessingTimeTimer(st.throttleAt);
            if (!"THROTTLED".equals(st.currentState)) emit(st, "THROTTLED", out, ctx);   // visible hold state (once)
            state.update(st);
        } else {   // SUPPRESS — saturated for the day
            if (st.inBacklog) { throttle.exitBacklog(st.channel); st.inBacklog = false; }
            emitReason(st, "SUPPRESSED", "throttle", out, ctx);
            terminal(st, ctx);
        }
    }

    private void emit(SmState st, String stateName, Collector<KafkaOut> out, ReadOnlyContext ctx) {
        st.currentState = stateName;
        out.collect(KafkaOut.of(st.memberKey(), stateFact(st, stateName, ctx.timerService().currentProcessingTime(), null), "state"));
    }

    private void emitReason(SmState st, String stateName, String reason, Collector<KafkaOut> out, ReadOnlyContext ctx) {
        st.currentState = stateName;
        String kind = "throttle".equals(reason) ? "throttle-suppress" : "state";
        out.collect(KafkaOut.of(st.memberKey(), stateFact(st, stateName, ctx.timerService().currentProcessingTime(), reason), kind));
    }

    private void emitActivation(SmState st, String op, ReadOnlyContext ctx) {
        ObjectNode a = SnapshotLogic.M.createObjectNode();
        a.put("op", op); a.put("nbaId", st.nbaId); a.put("entityType", st.entityType); a.put("entityId", st.entityId);
        a.put("memberId", st.entityId); a.put("actionId", st.actionId); a.put("channel", st.channel);
        a.put("contentKey", st.contentKey); a.put("ttlSeconds", st.ttlSeconds); a.put("trackingId", st.trackingId());
        a.put("correlationId", st.correlationId); a.put("source", "state-machine");
        a.put("eventTs", ctx.timerService().currentProcessingTime());
        // On the SEND: carry the dispatched action's per-touch templates (channels[].touchKeys) so the downstream
        // TouchFn can pick touchKeys[min(n,len)-1] for the monotonic (nbaId,channel) send count. Absent/empty -> the
        // base contentKey stands. CANCEL doesn't send, so no touchKeys.
        if ("DISPATCH".equals(op)) {
            ArrayNode tk = touchKeysFor(ctx, st.actionId, st.channel);
            if (tk != null && tk.size() > 0) a.set("touchKeys", tk);
        }
        try { ctx.output(ACT_TAG, KafkaOut.of(st.nbaId + ":" + st.actionId + ":" + st.channel, SnapshotLogic.M.writeValueAsString(a), null)); }
        catch (Exception ignore) {}
    }

    /** The dispatched action's channels[].touchKeys for this channel, from the broadcast ACTION def — or null. */
    private static ArrayNode touchKeysFor(ReadOnlyContext ctx, String actionId, String channel) {
        try {
            ReadOnlyBroadcastState<String, String> defs = ctx.getBroadcastState(DEFS_DESC);
            String json = defs.get("ACTION:" + actionId);
            if (json == null) return null;
            JsonNode def = SnapshotLogic.M.readTree(json);
            for (JsonNode ch : def.path("channels")) {
                if (!channel.equals(ch.path("channel").asText())) continue;
                JsonNode tk = ch.get("touchKeys");
                if (tk != null && tk.isArray() && tk.size() > 0) return (ArrayNode) tk;
                return null;                                    // channel found, no touchKeys -> base contentKey
            }
        } catch (Exception ignore) { /* missing/bad def -> base contentKey */ }
        return null;
    }

    private static String stateFact(SmState st, String stateName, long now, String reason) {
        ObjectNode o = SnapshotLogic.M.createObjectNode();
        o.put("entityType", st.entityType); o.put("entityId", st.entityId);
        if (st.nbaId != null && !st.nbaId.isEmpty()) o.put("nbaId", st.nbaId);
        o.put("key", st.stateKey()); o.put("value", stateName); o.put("valueType", "STRING");
        o.put("eventTs", now); o.put("source", "state-machine");
        if (reason != null) o.put("reason", reason);
        try { return SnapshotLogic.M.writeValueAsString(o); } catch (Exception e) { return "{}"; }
    }

    private void terminal(SmState st, ReadOnlyContext ctx) {
        if (st.debounceAt > 0) ctx.timerService().deleteProcessingTimeTimer(st.debounceAt);
        if (st.throttleAt > 0) ctx.timerService().deleteProcessingTimeTimer(st.throttleAt);
        if (st.ttlAt > 0) ctx.timerService().deleteProcessingTimeTimer(st.ttlAt);
        try { state.clear(); } catch (Exception ignore) {}
    }
}
