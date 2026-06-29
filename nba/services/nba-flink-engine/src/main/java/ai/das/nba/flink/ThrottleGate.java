package ai.das.nba.flink;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-channel SMART throttle — a token-bucket rate limiter with an end-of-day saturation predictor. FAITHFUL
 * port of nba-temporal/ThrottleGate (same admit() SEND/WAIT/SUPPRESS logic, same lake-rate folding). In Flink
 * it lives as a per-operator-INSTANCE shared object inside the lifecycle operator — the analog of Temporal's
 * worker-shared ThrottleGate. At parallelism 1 one instance sees the whole channel; at parallelism>1 the
 * windowInflight is per-instance and the LAKE rate (rateLevel, the aggregate of ALL sends fed from the lake on
 * nba.throttle.{ch}.rate) coordinates across instances — exactly how Temporal coordinates across workers.
 */
public class ThrottleGate {
    static final long DAY_MS = 86_400_000L;
    public static final String SEND = "SEND", WAIT = "WAIT", SUPPRESS = "SUPPRESS";

    final Map<String, Long> dailyLevel = new ConcurrentHashMap<>();
    final Map<String, Long> rateLevel  = new ConcurrentHashMap<>();   // from the lake (windowed, cross-instance aggregate)
    final Map<String, Long> rateCap    = new ConcurrentHashMap<>();   // the authored cap (gate-only channel rule)
    volatile long windowSeconds = 300;

    final Map<String, JsonNode> rules = new ConcurrentHashMap<>();
    final Map<String, long[]> windowInflight = new ConcurrentHashMap<>();   // {count, windowStartMs}
    final Map<String, Integer> backlog = new ConcurrentHashMap<>();         // channel -> # workflows WAITing

    /** A forwarded throttle fact nba.throttle.{ch}.{metric} (daily|rate). */
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

    /** A channel rule changed — re-derive each channel's rate cap from ALL rules (most-restrictive wins). */
    public synchronized void onChannelRule(String ruleId, JsonNode rule, boolean removed) {
        if (removed || rule == null) rules.remove(ruleId); else rules.put(ruleId, rule);
        Map<String, Long> nr = new HashMap<>();
        for (JsonNode r : rules.values()) {
            String ch = r.path("channel").asText("");
            Long cap = extractRateCap(r.get("logic"));
            if (!ch.isEmpty() && cap != null) nr.merge(ch, cap, Math::min);
        }
        rateCap.clear(); rateCap.putAll(nr);
    }

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
        return Math.max(1, (long) Math.ceil((double) secsLeft / windowSeconds));
    }

    long capacityLeftToday(String channel, long nowMs) {
        Long rc = rateCap.get(channel);
        return rc == null ? Long.MAX_VALUE : rc * windowsLeftToday(nowMs, windowSeconds);
    }

    public synchronized boolean saturated(String channel, long nowMs) {
        if (!rateCap.containsKey(channel)) return false;
        return backlog.getOrDefault(channel, 0) >= capacityLeftToday(channel, nowMs);
    }

    /** Admit a send: SEND (token reserved) | WAIT (window full, backlog still clears today) | SUPPRESS (can't clear). */
    public synchronized String admit(String channel, long nowMs) {
        Long rCap = rateCap.get(channel);
        if (rCap == null) return SEND;
        long[] wi = windowInflight.computeIfAbsent(channel, k -> new long[]{0, nowMs});
        if (nowMs - wi[1] >= windowSeconds * 1000) { wi[0] = 0; wi[1] = nowMs; }
        long rateCommitted = Math.max(rateLevel.getOrDefault(channel, 0L), wi[0]);
        if (rateCommitted < rCap) { wi[0]++; return SEND; }
        return backlog.getOrDefault(channel, 0) < capacityLeftToday(channel, nowMs) ? WAIT : SUPPRESS;
    }

    public synchronized void enterBacklog(String channel) { backlog.merge(channel, 1, Integer::sum); }
    public synchronized void exitBacklog(String channel) { backlog.computeIfPresent(channel, (k, v) -> v <= 1 ? null : v - 1); }
}
