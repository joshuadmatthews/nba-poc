package ai.das.nba.flink;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RulesLogicTest {
    static JsonNode j(String s) throws Exception { return SnapshotLogic.M.readTree(s); }

    @Test void condNumericAndBooleanCoercion() throws Exception {
        Map<String, Object> f = Map.of("count", 5L, "flag", true);
        assertTrue(RulesLogic.condPass(j("{\"fact\":\"count\",\"cmp\":\"gte\",\"value\":1}"), f));
        assertFalse(RulesLogic.condPass(j("{\"fact\":\"count\",\"cmp\":\"lt\",\"value\":3}"), f));
        assertTrue(RulesLogic.condPass(j("{\"fact\":\"flag\",\"cmp\":\"eq\",\"value\":true}"), f));
        assertTrue(RulesLogic.condPass(j("{\"fact\":\"flag\",\"cmp\":\"gte\",\"value\":1}"), f));   // boolean->1 coercion
        assertTrue(RulesLogic.condPass(j("{\"fact\":\"missing\",\"cmp\":\"eq\",\"value\":0}"), f));  // missing numeric -> 0
    }

    @Test void treeAllAnySemantics() throws Exception {
        Map<String, Object> f = Map.of("a", 1L, "b", 0L);
        assertTrue(RulesLogic.treePass(j("{\"op\":\"all\",\"conditions\":[{\"fact\":\"a\",\"cmp\":\"eq\",\"value\":1}]}"), f));
        assertFalse(RulesLogic.treePass(j("{\"op\":\"all\",\"conditions\":[{\"fact\":\"a\",\"cmp\":\"eq\",\"value\":1},{\"fact\":\"b\",\"cmp\":\"eq\",\"value\":1}]}"), f));
        assertTrue(RulesLogic.treePass(j("{\"op\":\"any\",\"conditions\":[{\"fact\":\"a\",\"cmp\":\"eq\",\"value\":1},{\"fact\":\"b\",\"cmp\":\"eq\",\"value\":1}]}"), f));
    }

    @Test void eligibleHitsRespectsInclusionAndChannel() throws Exception {
        RulesLogic.Defs d = new RulesLogic.Defs();
        d.actions.put("a1", j("{\"id\":\"a1\",\"name\":\"A1\",\"channels\":[{\"channel\":\"email\"}],"
                + "\"inclusion\":{\"op\":\"all\",\"conditions\":[{\"fact\":\"operator.profile.diabetic\",\"cmp\":\"eq\",\"value\":true}]}}"));
        assertEquals(Set.of("a1::email"), RulesLogic.eligibleHits(d, Map.of("operator.profile.diabetic", true)));
        assertTrue(RulesLogic.eligibleHits(d, Map.of("operator.profile.diabetic", false)).isEmpty());
    }

    @Test void evaluateEmitsEligibleChannelAction() throws Exception {
        RulesLogic.Defs d = new RulesLogic.Defs();
        d.actions.put("a1", j("{\"id\":\"a1\",\"name\":\"A1\",\"ttlSeconds\":3600,\"channels\":[{\"channel\":\"email\",\"contentKey\":\"welcome\"}],"
                + "\"inclusion\":{\"op\":\"all\",\"conditions\":[{\"fact\":\"operator.profile.diabetic\",\"cmp\":\"eq\",\"value\":true}]}}"));
        String snap = "{\"nbaId\":\"n1\",\"entityType\":\"OPERATOR\",\"entityId\":\"m1\",\"facts\":"
                + "{\"operator.profile.diabetic\":{\"value\":true,\"valueType\":\"BOOLEAN\",\"eventTs\":1}}}";
        RulesLogic.Result r = RulesLogic.evaluate(snap, d, null, null);
        assertNotNull(r.evalJson);
        JsonNode eval = SnapshotLogic.M.readTree(r.evalJson);
        JsonNode ca = eval.path("channelActions").get(0);
        assertEquals("a1", ca.path("actionId").asText());
        assertEquals("email", ca.path("channel").asText());
        assertTrue(ca.path("eligible").asBoolean());
        // change-detect: re-evaluating with the prior signatures yields no emit
        RulesLogic.Result again = RulesLogic.evaluate(snap, d, r.fullHash, r.eligHash);
        assertNull(again.evalJson, "unchanged -> skip emit");
    }
}
