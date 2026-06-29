package ai.das.nba.flink;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotLogicTest {
    static final SnapshotLogic.NbaIdResolver RES = (et, id) -> "nba_test";

    static SnapshotLogic.Parsed parse(String json, String kind) throws Exception {
        JsonNode v = SnapshotLogic.M.readTree(json);
        return new SnapshotLogic.Parsed("OPERATOR:m1", v, kind, json);
    }

    @Test void throttleFactRoutesToDefsNotSnapshot() throws Exception {
        String f = "{\"entityType\":\"OPERATOR\",\"entityId\":\"m1\",\"key\":\"nba.throttle.email.daily\",\"value\":5,\"valueType\":\"LONG\",\"eventTs\":1}";
        SnapshotLogic.Classified c = SnapshotLogic.classifyOne(parse(f, null), Set.of(), RES);
        assertNull(c.snap());
        assertEquals(1, c.forwards().size());
        assertEquals(SnapshotLogic.Route.DEFS, c.forwards().get(0).route());
    }

    @Test void memberRuleFactBecomesSnapFact() throws Exception {
        String f = "{\"entityType\":\"OPERATOR\",\"entityId\":\"m1\",\"key\":\"operator.profile.diabetic\",\"value\":true,\"valueType\":\"BOOLEAN\",\"eventTs\":10}";
        SnapshotLogic.Classified c = SnapshotLogic.classifyOne(parse(f, null), Set.of("operator.profile.diabetic"), RES);
        assertNotNull(c.snap());
        assertEquals("nba_test", c.snap().nbaId());
        assertEquals("operator.profile.diabetic", c.snap().factKey());
    }

    @Test void nonRuleFactDroppedWhenLeanFilterNonEmpty() throws Exception {
        String f = "{\"entityType\":\"OPERATOR\",\"entityId\":\"m1\",\"key\":\"operator.profile.unused\",\"value\":1,\"valueType\":\"LONG\",\"eventTs\":10}";
        SnapshotLogic.Classified c = SnapshotLogic.classifyOne(parse(f, null), Set.of("operator.profile.diabetic"), RES);
        assertNull(c.snap());
    }

    @Test void lwwNewerWinsOlderDrops() {
        Map<String, String> hash = new HashMap<>();
        SnapshotLogic.SnapFact older = sf("operator.x", 100);
        SnapshotLogic.SnapFact newer = sf("operator.x", 200);
        assertTrue(SnapshotLogic.applyLww(hash, older, 1));
        assertFalse(SnapshotLogic.applyLww(hash, sf("operator.x", 50), 2));   // stale -> drop
        assertTrue(SnapshotLogic.applyLww(hash, newer, 3));                    // newer -> win
        assertTrue(hash.get("fact:operator.x").contains("\"eventTs\":200"));
    }

    static SnapshotLogic.SnapFact sf(String key, long ts) {
        var fv = SnapshotLogic.M.createObjectNode();
        fv.put("value", ts); fv.put("valueType", "LONG"); fv.put("eventTs", ts); fv.put("source", "t");
        return new SnapshotLogic.SnapFact("nba_test", "OPERATOR", "m1", key, ts, fv);
    }
}
