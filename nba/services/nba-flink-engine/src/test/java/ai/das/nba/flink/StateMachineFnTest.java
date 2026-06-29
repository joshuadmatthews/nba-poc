package ai.das.nba.flink;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
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
}
