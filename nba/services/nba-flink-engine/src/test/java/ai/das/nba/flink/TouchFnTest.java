package ai.das.nba.flink;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** The channel_touch counter (Temporal touchTemplate analog): the monotonic per-(nbaId,channel) send count picks
 *  the escalating touch template touchKeys[min(n,len)-1], capped at the last; no touchKeys -> base contentKey. */
class TouchFnTest {
    static KafkaOut dispatch(String nbaId, String channel, String contentKey, String touchKeysJson) {
        String tk = touchKeysJson == null ? "" : ",\"touchKeys\":" + touchKeysJson;
        String v = "{\"op\":\"DISPATCH\",\"nbaId\":\"" + nbaId + "\",\"actionId\":\"a1\",\"channel\":\"" + channel
                + "\",\"contentKey\":\"" + contentKey + "\"" + tk + "}";
        return KafkaOut.of(nbaId + ":a1:" + channel, v, null);
    }

    static String contentKey(KafkaOut k) throws Exception {
        JsonNode v = SnapshotLogic.M.readTree(k.value);
        return v.path("contentKey").asText("");
    }

    static KeyedOneInputStreamOperatorTestHarness<String, KafkaOut, KafkaOut> harness() throws Exception {
        var h = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new TouchFn(), (KeySelector<KafkaOut, String>) TouchFn::touchKey, Types.STRING);
        h.open();
        return h;
    }

    @Test void escalatesAcrossSendsAndCapsAtLast() throws Exception {
        var h = harness();
        String tk = "[\"t0\",\"t1\"]";
        h.processElement(dispatch("nbaM", "email", "base", tk), 0L);   // 1st -> touchKeys[0]
        h.processElement(dispatch("nbaM", "email", "base", tk), 1L);   // 2nd -> touchKeys[1]
        h.processElement(dispatch("nbaM", "email", "base", tk), 2L);   // 3rd -> capped at last (touchKeys[1])
        List<KafkaOut> outs = h.extractOutputValues();
        assertEquals(3, outs.size());
        assertEquals("t0", contentKey(outs.get(0)), "1st send -> touchKeys[0]");
        assertEquals("t1", contentKey(outs.get(1)), "2nd send -> touchKeys[1]");
        assertEquals("t1", contentKey(outs.get(2)), "3rd send -> capped at the last touchKey");
        h.close();
    }

    @Test void noTouchKeysKeepsBaseContentKey() throws Exception {
        var h = harness();
        h.processElement(dispatch("nbaM", "sms", "base-sms", null), 0L);   // no touchKeys -> unchanged
        h.processElement(dispatch("nbaM", "sms", "base-sms", "[]"), 1L);   // empty touchKeys -> unchanged
        List<KafkaOut> outs = h.extractOutputValues();
        assertEquals(2, outs.size());
        assertEquals("base-sms", contentKey(outs.get(0)), "no touchKeys -> base contentKey");
        assertEquals("base-sms", contentKey(outs.get(1)), "empty touchKeys -> base contentKey");
        h.close();
    }

    @Test void counterIsPerChannelAndCountsEverySend() throws Exception {
        var h = harness();
        String tk = "[\"t0\",\"t1\",\"t2\"]";
        // two channels for the same member advance independently
        h.processElement(dispatch("nbaM", "email", "base", tk), 0L);   // email 1st -> t0
        h.processElement(dispatch("nbaM", "push", "base", tk), 1L);    // push  1st -> t0
        h.processElement(dispatch("nbaM", "email", "base", tk), 2L);   // email 2nd -> t1
        List<KafkaOut> outs = h.extractOutputValues();
        assertEquals("t0", contentKey(outs.get(0)));
        assertEquals("t0", contentKey(outs.get(1)), "push counter is independent of email");
        assertEquals("t1", contentKey(outs.get(2)), "email counter advanced to its 2nd send");
        h.close();
    }
}
