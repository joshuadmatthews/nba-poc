package ai.das.nba.temporal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Worker-side SMART channel throttle — a token-bucket rate limiter with a deadline (end-of-day)
 * admission predictor. The orchestration owns the throttle; the rules engine only learns the RESULT.
 *
 * ONE authored cap per channel — the RATE (gate-only channel rule, e.g. nba.throttle.email.rate < 5
 * per rolling window). Everything else is emergent:
 *   - the window is a token bucket that refills each period, so sends TRICKLE out and the level bounces;
 *   - the effective DAILY ceiling = rateCap × windows-per-day — never hand-set;
 *   - "this channel can't clear its backlog before midnight" = SATURATED, computed here from
 *     backlog vs rateCap × windows-LEFT-today. Saturation is a FACT the worker emits back to the rules
 *     engine (nba.throttle.{ch}.saturated), which marks the action-channel ineligible -> ML re-scores.
 *
 * A ChannelAction asks to be admitted right before it hands off:
 *   - rate window has room                         -> SEND   (reserve a window slot)
 *   - window full, backlog still clears today      -> WAIT   (hold in THROTTLED; trickle as the bucket refills)
 *   - window full, backlog can't clear today       -> SUPPRESS (reroute now — eligibility will also catch up)
 *
 * Rate level = max(lakeRate, windowInflight): the lake folds in EXTERNAL sends; windowInflight is this
 * worker's admits this window (ages out on roll). backlog = workflows currently THROTTLED on the channel.
 */
public class ThrottleGate {
    static final ObjectMapper M = new ObjectMapper();
    static final long DAY_MS = 86_400_000L;
    public static final String SEND = "SEND", WAIT = "WAIT", SUPPRESS = "SUPPRESS";

    final Map<String, Long> dailyLevel = new ConcurrentHashMap<>();   // from the lake (info / display)
    final Map<String, Long> rateLevel  = new ConcurrentHashMap<>();   // from the lake (windowed)
    final Map<String, Long> rateCap    = new ConcurrentHashMap<>();   // the ONE authored cap (gate-only rule)
    volatile long windowSeconds = 300;                               // learned from the lake's rate fact

    final Map<String, JsonNode> rules = new ConcurrentHashMap<>();    // ruleId -> channel rule
    final Map<String, long[]> windowInflight = new ConcurrentHashMap<>();   // {count, windowStartMs}
    final Map<String, Integer> backlog = new ConcurrentHashMap<>();   // channel -> # workflows THROTTLED (waiting)

    /** A forwarded throttle fact — parse channel + metric from its key (nba.throttle.{ch}.{metric}). */
    public void onFact(JsonNode t) {
        String fkey = t.path("key").asText("");
        String[] p = fkey.split("\\.");
        if (p.length < 4) return;
        String ch = p[2], metric = p[3];
        if ("daily".equals(metric)) dailyLevel.put(ch, t.path("value").asLong(0));
        else if ("rate".equals(metric)) {
            rateLevel.put(ch, t.path("value").asLong(0));
            long ws = t.path("windowSeconds").asLong(0);
            if (ws > 0) windowSeconds = ws;
        }
    }

    /** A channel rule changed — store/remove by id and re-derive each channel's rate cap from ALL rules. */
    public synchronized void onChannelRule(String ruleId, JsonNode rule, boolean removed) {
        if (removed || rule == null) rules.remove(ruleId); else rules.put(ruleId, rule);
        Map<String, Long> nr = new java.util.HashMap<>();
        for (JsonNode r : rules.values()) {
            String ch = r.path("channel").asText("");
            Long cap = extractRateCap(r.get("logic"));
            if (!ch.isEmpty() && cap != null) nr.merge(ch, cap, Math::min);   // most-restrictive wins
        }
        rateCap.clear(); rateCap.putAll(nr);
    }

    /** nba.throttle.{ch}.rate < N -> N ;  <= N -> N+1 ;  else (not a rate rule) -> null. */
    static Long extractRateCap(JsonNode logic) {
        JsonNode conds = logic == null ? null : logic.get("conditions");
        if (conds != null && conds.isArray()) for (JsonNode c : conds) {
            String fact = c.path("fact").asText("");
            if (!fact.endsWith(".rate")) continue;
            String cmp = c.path("cmp").asText(""); long v = c.path("value").asLong(0);
            if ("lt".equals(cmp)) return v;
            if ("lte".equals(cmp)) return v + 1;
        }
        return null;
    }

    static long windowsLeftToday(long nowMs, long windowSeconds) {
        long secsLeft = (DAY_MS - (nowMs % DAY_MS)) / 1000;
        return Math.max(1, (long) Math.ceil((double) secsLeft / windowSeconds));   // ≥ the current window
    }

    /** rateCap × windows-left-today — how many MORE can still go out today on this channel. */
    long capacityLeftToday(String channel, long nowMs) {
        Long rc = rateCap.get(channel);
        return rc == null ? Long.MAX_VALUE : rc * windowsLeftToday(nowMs, windowSeconds);
    }

    /** Backlog can't clear before midnight -> the channel is full for the day. */
    public synchronized boolean saturated(String channel, long nowMs) {
        if (!rateCap.containsKey(channel)) return false;
        return backlog.getOrDefault(channel, 0) >= capacityLeftToday(channel, nowMs);
    }

    public synchronized String admit(String channel, long nowMs) {
        Long rCap = rateCap.get(channel);
        if (rCap == null) return SEND;                                     // unthrottled channel

        long[] wi = windowInflight.computeIfAbsent(channel, k -> new long[]{0, nowMs});
        if (nowMs - wi[1] >= windowSeconds * 1000) { wi[0] = 0; wi[1] = nowMs; }   // roll the window (refill)
        long rateCommitted = Math.max(rateLevel.getOrDefault(channel, 0L), wi[0]);
        if (rateCommitted < rCap) { wi[0]++; return SEND; }                // bucket has a token

        // window full — can the current backlog still drain before midnight?
        return backlog.getOrDefault(channel, 0) < capacityLeftToday(channel, nowMs) ? WAIT : SUPPRESS;
    }

    public synchronized void enterBacklog(String channel) { backlog.merge(channel, 1, Integer::sum); }
    public synchronized void exitBacklog(String channel) {
        backlog.computeIfPresent(channel, (k, v) -> v <= 1 ? null : v - 1);
    }

    /** Current saturation per capped channel — the worker diffs this and emits the changes. */
    public synchronized Map<String, Boolean> saturationSnapshot(long nowMs) {
        Map<String, Boolean> out = new java.util.TreeMap<>();
        for (String ch : rateCap.keySet()) out.put(ch, saturated(ch, nowMs));
        return out;
    }

    /** Observability: {dailyLevel, rateLevel, rateCap, windowSeconds, backlog, saturated?1:0} per channel. */
    public synchronized Map<String, long[]> snapshot(long nowMs) {
        Map<String, long[]> out = new java.util.TreeMap<>();
        java.util.Set<String> chans = new java.util.HashSet<>(rateCap.keySet());
        chans.addAll(rateLevel.keySet()); chans.addAll(dailyLevel.keySet());
        for (String ch : chans) out.put(ch, new long[]{
            dailyLevel.getOrDefault(ch, 0L), rateLevel.getOrDefault(ch, 0L), rateCap.getOrDefault(ch, -1L),
            windowSeconds, backlog.getOrDefault(ch, 0), saturated(ch, nowMs) ? 1 : 0});
        return out;
    }
}
