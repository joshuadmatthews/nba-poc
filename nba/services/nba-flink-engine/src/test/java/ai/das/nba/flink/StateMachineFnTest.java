package ai.das.nba.flink;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Covers the two pieces that make the Flink state machine feature-complete with Temporal: the per-channel
 *  ThrottleGate (rate limiter) and the member-keyed debounce-dedup (debounceLost). */
class StateMachineFnTest {
    static JsonNode j(String s) throws Exception { return SnapshotLogic.M.readTree(s); }

    // ── ThrottleGate (pure) ──
    @Test void throttleSendsUpToRateThenWaits() throws Exception {
        ThrottleGate g = new ThrottleGate();
        g.onChannelRule("r1", j("{\"channel\":\"email\",\"logic\":{\"conditions\":[{\"fact\":\"nba.throttle.email.rate\",\"cmp\":\"lt\",\"value\":2}]}}"), false);
        long t = 1_000_000L;
        assertEquals(ThrottleGate.SEND, g.admit("email", t));   // bucket 0 < 2
        assertEquals(ThrottleGate.SEND, g.admit("email", t));   // bucket 1 < 2
        String third = g.admit("email", t);                    // window full -> WAIT (backlog clears) or SUPPRESS
        assertTrue(third.equals(ThrottleGate.WAIT) || third.equals(ThrottleGate.SUPPRESS), "full window: " + third);
    }

    @Test void throttleUncappedChannelAlwaysSends() {
        ThrottleGate g = new ThrottleGate();
        for (int i = 0; i < 5; i++) assertEquals(ThrottleGate.SEND, g.admit("push", 1_000L));
    }

    @Test void throttleWindowRefills() throws Exception {
        ThrottleGate g = new ThrottleGate();
        g.onChannelRule("r1", j("{\"channel\":\"sms\",\"logic\":{\"conditions\":[{\"fact\":\"nba.throttle.sms.rate\",\"cmp\":\"lt\",\"value\":1}]}}"), false);
        assertEquals(ThrottleGate.SEND, g.admit("sms", 0L));            // token
        assertNotEquals(ThrottleGate.SEND, g.admit("sms", 1000L));      // full
        assertEquals(ThrottleGate.SEND, g.admit("sms", 301_000L));      // > windowSeconds(300) later -> refilled
    }

    // ── MemberDedupFn (harness) ──
    static String create(String aid, String ch, double score) {
        return "{\"op\":\"CREATE\",\"nbaId\":\"nbaM\",\"entityType\":\"OPERATOR\",\"entityId\":\"m\",\"actionId\":\""
                + aid + "\",\"channel\":\"" + ch + "\",\"score\":" + score + ",\"eventTs\":1}";
    }

    @Test void dedupLowerScoringSiblingLoses() throws Exception {
        var h = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new MemberDedupFn(), (KeySelector<StateEvent, String>) e -> MemberDedupFn.member(e.key), Types.STRING);
        h.open();
        h.processElement(new StateEvent("nbaM:action_a:email", "CREATE", create("action_a", "email", 18.0)), 0L);
        h.processElement(new StateEvent("nbaM:action_b:email", "CREATE", create("action_b", "email", 9.0)), 1L);
        // winner (higher score) forwarded; loser dropped from main output
        assertEquals(1, h.extractOutputValues().size(), "only the higher-scoring CREATE proceeds");
        assertEquals("action_a:email", MemberDedupFn.slug(h.extractOutputValues().get(0).key));
        // loser gets a DEBOUNCED state on the side output
        var deb = h.getSideOutput(MemberDedupFn.DEBOUNCED_TAG);
        assertNotNull(deb);
        boolean debounced = false;
        for (Object o : deb) { KafkaOut k = ((StreamRecord<KafkaOut>) o).getValue();
            if (SnapshotLogic.M.readTree(k.value).path("value").asText().equals("DEBOUNCED")) debounced = true; }
        assertTrue(debounced, "lower-scoring sibling self-DEBOUNCEs");
        h.close();
    }

    // ── StateMachineFn operator-suppress: per-key (existing branch) + the broadcast fleet fan-out (new) ──
    //    A KeyedBroadcastProcessFunction harness drives StateEvents on the keyed input and OPSUPPRESS commands on
    //    the broadcast input, advancing processing time to fire debounce/dispatch timers.

    static KeyedBroadcastOperatorTestHarness<String, StateEvent, FactRecord, KafkaOut> smHarness(int debounceSec) throws Exception {
        var h = ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(
                new StateMachineFn(debounceSec, 600),
                (KeySelector<StateEvent, String>) e -> e.key, Types.STRING,
                StateMachineFn.DEFS_DESC);
        h.open();
        return h;
    }

    static String createFact(String nbaId, String entityId, String actionId, String channel) {
        return "{\"op\":\"CREATE\",\"nbaId\":\"" + nbaId + "\",\"entityType\":\"OPERATOR\",\"entityId\":\"" + entityId
                + "\",\"actionId\":\"" + actionId + "\",\"channel\":\"" + channel
                + "\",\"name\":\"n\",\"contentKey\":\"c\",\"ttlSeconds\":86400,\"score\":5.0,\"eventTs\":1}";
    }

    /** An operator-suppress broadcast command (channel empty/null = all channels for the action). */
    static FactRecord opSuppress(String actionId, String channel) {
        boolean scoped = channel != null && !channel.isEmpty();
        String v = scoped ? "{\"actionId\":\"" + actionId + "\",\"channel\":\"" + channel + "\"}"
                          : "{\"actionId\":\"" + actionId + "\"}";
        return new FactRecord("OPSUPPRESS:" + actionId + (scoped ? "." + channel : ""), v, "definition", 1L);
    }

    /** Count main-output state facts whose lifecycle value == name. */
    static int countState(KeyedBroadcastOperatorTestHarness<String, StateEvent, FactRecord, KafkaOut> h, String name) throws Exception {
        int n = 0;
        for (KafkaOut k : h.extractOutputValues())
            if (name.equals(SnapshotLogic.M.readTree(k.value).path("value").asText(""))) n++;
        return n;
    }

    /** Count CANCEL activations on the ACT side output. */
    @SuppressWarnings("unchecked")
    static int cancelCount(KeyedBroadcastOperatorTestHarness<String, StateEvent, FactRecord, KafkaOut> h) throws Exception {
        var side = h.getSideOutput(StateMachineFn.ACT_TAG);
        if (side == null) return 0;
        int n = 0;
        for (Object o : side) {
            KafkaOut k = ((StreamRecord<KafkaOut>) o).getValue();
            if ("CANCEL".equals(SnapshotLogic.M.readTree(k.value).path("op").asText(""))) n++;
        }
        return n;
    }

    @Test void perKeyOperatorSuppressTerminatesPreSend() throws Exception {
        var h = smHarness(300);                       // long debounce: the action sits pre-send in DEBOUNCE
        h.processElement(new StateEvent("nbaA:act1:email", "CREATE", createFact("nbaA", "mA", "act1", "email")), 1L);
        assertEquals(0, countState(h, "SUPPRESSED"));
        h.processElement(new StateEvent("nbaA:act1:email", "OPERATOR_SUPPRESS", "{}"), 2L);
        assertEquals(1, countState(h, "SUPPRESSED"), "per-key operator suppress terminates a pre-send action");
        h.close();
    }

    @Test void broadcastFleetSuppressFansAcrossMatchingActionOnly() throws Exception {
        var h = smHarness(300);
        h.processElement(new StateEvent("nbaA:act1:email", "CREATE", createFact("nbaA", "mA", "act1", "email")), 1L);
        h.processElement(new StateEvent("nbaB:act1:email", "CREATE", createFact("nbaB", "mB", "act1", "email")), 2L);
        h.processElement(new StateEvent("nbaC:act2:email", "CREATE", createFact("nbaC", "mC", "act2", "email")), 3L);
        h.processBroadcastElement(opSuppress("act1", null), 4L);
        assertEquals(2, countState(h, "SUPPRESSED"), "ONE broadcast suppresses BOTH act1 instances");
        // act2 was left live: a per-key suppress of it still fires (proves the broadcast didn't touch it)
        h.processElement(new StateEvent("nbaC:act2:email", "OPERATOR_SUPPRESS", "{}"), 5L);
        assertEquals(3, countState(h, "SUPPRESSED"), "act2 untouched by the act1-scoped broadcast");
        h.close();
    }

    @Test void broadcastFleetSuppressRespectsChannelScope() throws Exception {
        var h = smHarness(300);
        h.processElement(new StateEvent("nbaA:act1:email", "CREATE", createFact("nbaA", "mA", "act1", "email")), 1L);
        h.processElement(new StateEvent("nbaA:act1:sms",   "CREATE", createFact("nbaA", "mA", "act1", "sms")),   2L);
        h.processBroadcastElement(opSuppress("act1", "sms"), 3L);
        assertEquals(1, countState(h, "SUPPRESSED"), "channel-scoped broadcast suppresses only the sms instance");
        h.processElement(new StateEvent("nbaA:act1:email", "OPERATOR_SUPPRESS", "{}"), 4L);
        assertEquals(2, countState(h, "SUPPRESSED"), "the email instance was left live");
        h.close();
    }

    @Test void broadcastFleetSuppressIsIdempotent() throws Exception {
        var h = smHarness(300);
        h.processElement(new StateEvent("nbaA:act1:email", "CREATE", createFact("nbaA", "mA", "act1", "email")), 1L);
        h.processBroadcastElement(opSuppress("act1", null), 2L);
        assertEquals(1, countState(h, "SUPPRESSED"));
        h.processBroadcastElement(opSuppress("act1", null), 3L);
        assertEquals(1, countState(h, "SUPPRESSED"), "re-suppressing already-terminal keys emits nothing new");
        h.close();
    }

    /** The headline case: a BIG in-process (post-send, TRACKING) queue for one action, suppressed by a single
     *  operator broadcast, then drained — SUPPRESSING + a CANCEL activation per instance, and a SUPPRESSED terminal
     *  once the activation layer's cancel dispositions come back. */
    @Test void broadcastFleetSuppressDrainsBigInProcessQueue() throws Exception {
        var h = smHarness(1);                          // 1s debounce: CREATEs register a debounce timer at t=1000
        final int N = 1000;
        for (int i = 0; i < N; i++)
            h.processElement(new StateEvent("nba" + i + ":act1:push", "CREATE", createFact("nba" + i, "m" + i, "act1", "push")), i + 1);
        // fire the debounce timers -> push is uncapped -> every instance dispatches into TRACKING (the big queue)
        h.setProcessingTime(60_000L);
        assertEquals(N, countState(h, "IN_PROCESS"), "all " + N + " actions are sent and in-process (tracking)");
        // ONE operator-suppress broadcast moves the whole in-process queue to SUPPRESSING + a CANCEL apiece
        h.processBroadcastElement(opSuppress("act1", null), 2_000L);
        assertEquals(N, countState(h, "SUPPRESSING"), "one broadcast cancels every in-process action");
        assertEquals(N, cancelCount(h), "and asks the activation layer to CANCEL each one");
        // the layer replies with SUPPRESSED dispositions -> the queue terminates
        for (int i = 0; i < N; i++)
            h.processElement(new StateEvent("nba" + i + ":act1:push", "DISPOSITION", "{\"state\":\"SUPPRESSED\"}"), 3_000 + i);
        assertEquals(N, countState(h, "SUPPRESSED"), "every cancelled action terminates SUPPRESSED");
        h.close();
    }
}
