package ai.das.nba.temporal;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the PURE worker-side throttle math in {@link ThrottleGate} — the token-bucket
 * admission predictor. No Kafka/Redis: every method here is fed plain JSON / millis and produces a
 * deterministic verdict, which is exactly the surface a ChannelAction asks before it hands off.
 */
class ThrottleGateTest {

    static JsonNode j(String raw) {
        try { return ThrottleGate.M.readTree(raw); } catch (Exception e) { throw new RuntimeException(e); }
    }

    // a channel rule whose logic gates on nba.throttle.{ch}.rate {cmp} {value}
    static JsonNode rule(String channel, String cmp, long value) {
        return j("{\"channel\":\"" + channel + "\",\"logic\":{\"conditions\":[{\"fact\":\"nba.throttle."
                + channel + ".rate\",\"cmp\":\"" + cmp + "\",\"value\":" + value + "}]}}");
    }

    // ---- extractRateCap: lt -> N, lte -> N+1, else null --------------------------------------

    @Test
    void extractRateCapLtIsValueAsIs() {
        assertEquals(5L, ThrottleGate.extractRateCap(rule("email", "lt", 5).get("logic")));
    }

    @Test
    void extractRateCapLteIsValuePlusOne() {
        assertEquals(6L, ThrottleGate.extractRateCap(rule("email", "lte", 5).get("logic")),
                "<= N admits up to N, i.e. a cap of N+1");
    }

    @Test
    void extractRateCapNonRateFactIsNull() {
        // a condition on a non-.rate fact is not a rate cap
        JsonNode logic = j("{\"conditions\":[{\"fact\":\"nba.throttle.email.daily\",\"cmp\":\"lt\",\"value\":9}]}");
        assertNull(ThrottleGate.extractRateCap(logic));
        assertNull(ThrottleGate.extractRateCap(null), "no logic -> null");
    }

    // ---- onChannelRule: most-restrictive cap wins across rules --------------------------------

    @Test
    void mostRestrictiveCapWinsAcrossRules() {
        ThrottleGate g = new ThrottleGate();
        g.onChannelRule("r1", rule("email", "lt", 10), false);
        g.onChannelRule("r2", rule("email", "lt", 3), false);   // stricter
        // admit() returns SEND until the cap is hit; the stricter cap (3) governs
        assertEquals(ThrottleGate.SEND, g.admit("email", 0));
        assertEquals(ThrottleGate.SEND, g.admit("email", 0));
        assertEquals(ThrottleGate.SEND, g.admit("email", 0));   // 3rd token (cap=3)
        // 4th within the same window -> no token; backlog empty + capacity left -> WAIT
        assertEquals(ThrottleGate.WAIT, g.admit("email", 0));
    }

    @Test
    void removingARuleReDerivesTheCap() {
        ThrottleGate g = new ThrottleGate();
        g.onChannelRule("r1", rule("email", "lt", 3), false);
        g.onChannelRule("r1", null, true);   // removed -> no cap -> unthrottled
        for (int i = 0; i < 50; i++) assertEquals(ThrottleGate.SEND, g.admit("email", 0),
                "no rule -> unthrottled channel always SENDs");
    }

    // ---- admit: token bucket + window roll ---------------------------------------------------

    @Test
    void unthrottledChannelAlwaysSends() {
        ThrottleGate g = new ThrottleGate();
        assertEquals(ThrottleGate.SEND, g.admit("push", 0), "no cap configured -> SEND");
    }

    @Test
    void windowRollRefillsTheBucket() {
        ThrottleGate g = new ThrottleGate();
        g.onChannelRule("r1", rule("email", "lt", 1), false);   // 1 per window
        assertEquals(ThrottleGate.SEND, g.admit("email", 0));   // first token
        assertEquals(ThrottleGate.WAIT, g.admit("email", 0));   // full this window
        // windowSeconds defaults to 300 -> roll past it and the bucket refills
        long nextWindow = 301_000L;
        assertEquals(ThrottleGate.SEND, g.admit("email", nextWindow), "bucket refills on window roll");
    }

    @Test
    void lakeRateLevelCountsAgainstTheCap() {
        ThrottleGate g = new ThrottleGate();
        g.onChannelRule("r1", rule("email", "lt", 3), false);
        // the lake reports 3 already committed this window (external sends) -> bucket already full
        g.onFact(j("{\"key\":\"nba.throttle.email.rate\",\"value\":3,\"windowSeconds\":300}"));
        assertNotEquals(ThrottleGate.SEND, g.admit("email", 0),
                "rateCommitted = max(lakeRate, inflight); lake already at cap -> no SEND");
    }

    // ---- saturation: backlog vs capacity-left-today ------------------------------------------

    @Test
    void windowsLeftTodayIsAtLeastOne() {
        // 1ms before midnight: < one full window remains, but it floors at 1
        long justBeforeMidnight = ThrottleGate.DAY_MS - 1;
        assertEquals(1L, ThrottleGate.windowsLeftToday(justBeforeMidnight, 300));
        // start of day: 86400s / 300s = 288 windows
        assertEquals(288L, ThrottleGate.windowsLeftToday(0, 300));
    }

    @Test
    void saturatedWhenBacklogExceedsCapacityLeftToday() {
        ThrottleGate g = new ThrottleGate();
        g.onChannelRule("r1", rule("email", "lt", 1), false);   // 1 per window
        // near midnight -> capacityLeftToday = 1*1 = 1; push backlog to 2 -> can't clear today
        long nearMidnight = ThrottleGate.DAY_MS - 1;
        g.enterBacklog("email");
        g.enterBacklog("email");
        assertTrue(g.saturated("email", nearMidnight), "backlog 2 > capacity 1 -> saturated");
    }

    @Test
    void notSaturatedWithAmpleTimeLeft() {
        ThrottleGate g = new ThrottleGate();
        g.onChannelRule("r1", rule("email", "lt", 1), false);
        g.enterBacklog("email");   // backlog 1
        // start of day -> 288 windows * cap 1 = 288 capacity, comfortably clears 1
        assertFalse(g.saturated("email", 0));
    }

    @Test
    void uncappedChannelIsNeverSaturated() {
        ThrottleGate g = new ThrottleGate();
        g.enterBacklog("push");
        assertFalse(g.saturated("push", 0), "no rate cap -> never saturated");
    }

    @Test
    void exitBacklogDecrementsAndClearsToZero() {
        ThrottleGate g = new ThrottleGate();
        g.onChannelRule("r1", rule("email", "lt", 1), false);
        g.enterBacklog("email");
        g.exitBacklog("email");
        // back to empty backlog -> not saturated even near midnight
        assertFalse(g.saturated("email", ThrottleGate.DAY_MS - 1));
    }

    // ---- onFact: parse channel + metric from the fact key ------------------------------------

    @Test
    void onFactRateUpdatesWindowSeconds() {
        ThrottleGate g = new ThrottleGate();
        g.onFact(j("{\"key\":\"nba.throttle.sms.rate\",\"value\":2,\"windowSeconds\":600}"));
        // a 600s window -> 86400/600 = 144 windows at start of day
        assertEquals(144L, ThrottleGate.windowsLeftToday(0, 600));
        // and the snapshot reflects the learned windowSeconds
        g.onChannelRule("r1", rule("sms", "lt", 5), false);
        assertEquals(600L, g.snapshot(0).get("sms")[3], "windowSeconds learned from the rate fact");
    }

    @Test
    void onFactShortKeyIsIgnored() {
        ThrottleGate g = new ThrottleGate();
        // key with < 4 dotted parts -> ignored, no NPE/throw
        g.onFact(j("{\"key\":\"nba.throttle.email\",\"value\":9}"));
        assertTrue(g.snapshot(0).isEmpty(), "malformed key produces no channel state");
    }
}
