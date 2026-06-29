package ai.das.nba.flink;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ScoreFnTest {
    static ObjectNode ca(String state, boolean active, boolean soft, boolean hard) {
        ObjectNode o = SnapshotLogic.M.createObjectNode();
        o.put("actionId", "a1"); o.put("channel", "email");
        if (state != null) o.put("workflowState", state);
        o.put("active", active); o.put("softCompleted", soft); o.put("hardCompleted", hard);
        return o;
    }

    @Test void freshActionScoresTop() {
        double s = ScoreStage.ScoreFn.scoreAction("nbaX", "a1", "email", ca(null, false, false, false));
        assertTrue(s >= 10.0 && s < 20.0, "fresh in [10,20): " + s);
    }

    @Test void hardCompletedSinks() {
        double s = ScoreStage.ScoreFn.scoreAction("nbaX", "a1", "email", ca(null, false, false, true));
        assertTrue(s < -90.0, "hard < -90: " + s);
    }

    @Test void inflightSinks() {
        double s = ScoreStage.ScoreFn.scoreAction("nbaX", "a1", "email", ca("IN_PROCESS", true, false, false));
        assertTrue(s < -40.0 && s > -60.0, "in-flight ~ -50: " + s);
    }

    @Test void deterministicPerTuple() {
        double a = ScoreStage.ScoreFn.scoreAction("nbaX", "a1", "email", ca(null, false, false, false));
        double b = ScoreStage.ScoreFn.scoreAction("nbaX", "a1", "email", ca(null, false, false, false));
        assertEquals(a, b, "stable hash -> reproducible");
    }
}
