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
import org.apache.flink.runtime.state.KeyedStateFunction;
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
    /** Hoisted (not inlined in open) so the broadcast handler can fan a fleet-wide suppress across ALL keyed
     *  instances via {@code applyToKeyedState} — the Flink analog of Temporal's suppressMatching Batch Operation. */
    private static final ValueStateDescriptor<SmState> STATE_DESC =
            new ValueStateDescriptor<>("sm", TypeInformation.of(SmState.class));

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
        state = getRuntimeContext().getState(STATE_DESC);
        throttle = new ThrottleGate();
    }

    /** Definitions broadcast -> feed the ThrottleGate (THROTTLE facts + CHANNEL_RULE caps) + keep the raw store. */
    @Override
    public void processBroadcastElement(FactRecord def, Context ctx, Collector<KafkaOut> out) throws Exception {
        if (def.key == null) return;
        int i = def.key.indexOf(':');
        String type = i < 0 ? def.key : def.key.substring(0, i);
        // Explicit one-shot fleet-suppress command (value = {"actionId","channel"?}); NOT stored in broadcast state.
        if ("OPSUPPRESS".equals(type)) { fleetSuppress(suppressTarget(def.value), ctx, out); return; }
        var st = ctx.getBroadcastState(DEFS_DESC);
        if (def.value == null || def.value.isEmpty()) { st.remove(def.key); return; }
        // ACTION_SUPPRESS:{target} is the operator's PERSISTENT suppressed-flag, published by the existing
        // POST /suppress (the same producer that drives classic's eligibility + SuppressionWorkflow). On the
        // false->true TRANSITION, ALSO fan a fleet cancel across the in-flight instances — so the existing operator
        // action drives the Flink in-flight fan-out too. Compacted redeliveries (same value) don't re-fire.
        if ("ACTION_SUPPRESS".equals(type)) {
            boolean prevOn = onFlag(st.get(def.key)), curOn = onFlag(def.value);
            st.put(def.key, def.value);
            if (curOn && !prevOn) fleetSuppress(suppressTarget(def.value), ctx, out);
            return;
        }
        st.put(def.key, def.value);
        if (i < 0) return;
        String id = def.key.substring(i + 1);
        try {
            JsonNode v = SnapshotLogic.M.readTree(def.value);
            if ("THROTTLE".equals(type)) throttle.onFact(v);
            else if ("CHANNEL_RULE".equals(type)) throttle.onChannelRule(id, v, false);
        } catch (Exception ignore) { /* skip bad def */ }
    }

    /** Parse {actionId, channel} out of a command/flag value -> [actionId, channel] ("" channel = all channels). */
    private static String[] suppressTarget(String json) {
        try { JsonNode v = SnapshotLogic.M.readTree(json);
              return new String[]{ v.path("actionId").asText(""), v.path("channel").asText("") }; }
        catch (Exception e) { return new String[]{"", ""}; }
    }

    /** The boolean "value" of an ACTION_SUPPRESS flag fact (false when absent/unparseable). */
    private static boolean onFlag(String json) {
        if (json == null || json.isEmpty()) return false;
        try { return SnapshotLogic.M.readTree(json).path("value").asBoolean(false); } catch (Exception e) { return false; }
    }

    /** Fan ONE operator-suppress command across every matching LIVE keyed instance via {@code applyToKeyedState} —
     *  the Flink analog of Temporal's suppressMatching Batch Operation, but a local memory-speed keyed-state scan
     *  instead of O(N) durable per-workflow signals. The per-key transition mirrors the OPERATOR_SUPPRESS branch in
     *  {@link #processElement} exactly: pre-send (DEBOUNCE/THROTTLE) instances go terminal SUPPRESSED; post-send
     *  (TRACKING) instances emit SUPPRESSING + a CANCEL activation (the activation layer then replies with a
     *  SUPPRESSED/SUPPRESS_FAILED disposition, just like the per-key path). Idempotent — already terminal/cleared
     *  keys are skipped, so re-broadcasting the same command emits nothing new. target = [actionId, channel]
     *  (empty channel = all channels for the action). */
    private void fleetSuppress(String[] target, Context ctx, Collector<KafkaOut> out) throws Exception {
        final String actionId = target[0], channel = target[1];
        if (actionId.isEmpty()) return;
        final long now = ctx.currentProcessingTime();
        final java.util.List<KafkaOut> facts = new java.util.ArrayList<>();
        final java.util.List<KafkaOut> cancels = new java.util.ArrayList<>();
        ctx.applyToKeyedState(STATE_DESC, (KeyedStateFunction<String, ValueState<SmState>>) (key, vs) -> {
            SmState st = vs.value();
            if (st == null || "DONE".equals(st.phase)) return;
            if (!actionId.equals(st.actionId)) return;
            if (!channel.isEmpty() && !channel.equals(st.channel)) return;
            if ("DEBOUNCE".equals(st.phase) || "THROTTLE".equals(st.phase)) {          // pre-send -> terminal now
                if (st.inBacklog) { throttle.exitBacklog(st.channel); st.inBacklog = false; }
                st.currentState = "SUPPRESSED";
                facts.add(KafkaOut.of(st.memberKey(), stateFact(st, "SUPPRESSED", now, null), "state"));
                vs.clear();                                                            // dangling timers no-op on null state
            } else if ("TRACKING".equals(st.phase) && !st.cancelSent) {               // post-send -> ask the layer to cancel
                st.cancelSent = true;
                st.currentState = "SUPPRESSING";
                facts.add(KafkaOut.of(st.memberKey(), stateFact(st, "SUPPRESSING", now, null), "state"));
                cancels.add(activationKafka(st, activationNode(st, "CANCEL", now)));
                vs.update(st);
            }
        });
        for (KafkaOut f : facts) out.collect(f);
        for (KafkaOut c : cancels) ctx.output(ACT_TAG, c);
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
                // PRE-DISPATCHED create (an INBOUND serve — the action-library already presented the action to
                // the member, so there is nothing to debounce/throttle/dispatch): go straight to TRACKING at
                // IN_PROCESS and watch dispositions to soft/hard completion or TTL -> EXPIRED. Mirrors the
                // Temporal ChannelActionWorkflowImpl preDispatched path — the inbound rides the SAME journey.
                if (v.path("preDispatched").asBoolean(false)) {
                    emit(st, "IN_PROCESS", out, ctx);
                    st.phase = "TRACKING";
                    st.ttlAt = now + Math.max(1L, st.ttlSeconds) * 1000L;
                    ctx.timerService().registerProcessingTimeTimer(st.ttlAt);
                    state.update(st);
                    return;
                }
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
                // RAW provider status rides `value`; the state machine classifies it (the action-layer no longer
                // decides state). Fall back to a legacy `state` field for facts produced before this change.
                String raw = v.has("value") ? v.path("value").asText("") : v.path("state").asText("");
                String d = DispositionClassifier.classify(raw);
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

    /** Is (actionId, channel) operator-suppressed right now, per the LIVE ACTION_SUPPRESS flags in the definitions
     *  broadcast state? Action-wide flag OR the action-channel flag. Bidirectional — an unsuppress (value=false)
     *  clears it, so the action dispatches again. In-stream read; no external call. */
    private static boolean isOperatorSuppressed(ReadOnlyContext ctx, String actionId, String channel) {
        try {
            var defs = ctx.getBroadcastState(DEFS_DESC);
            if (onFlag(defs.get("ACTION_SUPPRESS:" + actionId))) return true;
            return channel != null && !channel.isEmpty() && onFlag(defs.get("ACTION_SUPPRESS:" + actionId + "." + channel));
        } catch (Exception e) { return false; }
    }

    /** The throttle gate: SEND -> dispatch; WAIT -> hold + recheck (trickle); SUPPRESS -> reroute. */
    private void gate(SmState st, ReadOnlyContext ctx, Collector<KafkaOut> out) throws Exception {
        // FINAL GATE before send: honor a LIVE operator suppression. The broadcast fleetSuppress only cancels
        // instances that already existed when the flag flipped; one that reaches the gate AFTER (a backlogged CREATE,
        // or one that sat in the throttle backlog through the pull) self-suppresses here. Bidirectional via the flag.
        if (isOperatorSuppressed(ctx, st.actionId, st.channel)) {
            if (st.inBacklog) { throttle.exitBacklog(st.channel); st.inBacklog = false; }
            emit(st, "SUPPRESSED", out, ctx);
            terminal(st, ctx);
            return;
        }
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
        ObjectNode a = activationNode(st, op, ctx.timerService().currentProcessingTime());
        // On the SEND: carry the dispatched action's per-touch templates (channels[].touchKeys) so the downstream
        // TouchFn can pick touchKeys[min(n,len)-1] for the monotonic (nbaId,channel) send count. Absent/empty -> the
        // base contentKey stands. CANCEL doesn't send, so no touchKeys.
        if ("DISPATCH".equals(op)) {
            ArrayNode tk = touchKeysFor(ctx, st.actionId, st.channel);
            if (tk != null && tk.size() > 0) a.set("touchKeys", tk);
        }
        ctx.output(ACT_TAG, activationKafka(st, a));
    }

    /** The activation JSON (DISPATCH/CANCEL) minus the per-touch templates. Timestamp explicit so it builds from
     *  both {@link #processElement} (timer time) and the broadcast handler ({@code ctx.currentProcessingTime}). */
    private static ObjectNode activationNode(SmState st, String op, long now) {
        ObjectNode a = SnapshotLogic.M.createObjectNode();
        a.put("op", op); a.put("nbaId", st.nbaId); a.put("entityType", st.entityType); a.put("entityId", st.entityId);
        a.put("memberId", st.entityId); a.put("actionId", st.actionId); a.put("channel", st.channel);
        a.put("contentKey", st.contentKey); a.put("ttlSeconds", st.ttlSeconds); a.put("trackingId", st.trackingId());
        a.put("correlationId", st.correlationId); a.put("source", "state-machine"); a.put("eventTs", now);
        return a;
    }

    private static KafkaOut activationKafka(SmState st, ObjectNode a) {
        String k = st.nbaId + ":" + st.actionId + ":" + st.channel;
        try { return KafkaOut.of(k, SnapshotLogic.M.writeValueAsString(a), null); }
        catch (Exception e) { return KafkaOut.of(k, "{}", null); }
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
