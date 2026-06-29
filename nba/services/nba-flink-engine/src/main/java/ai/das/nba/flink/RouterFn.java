package ai.das.nba.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import redis.clients.jedis.JedisPooled;

/**
 * STAGE 4 — ROUTER (faithful port of action-router): per-member single-active-action slot. From an evaluation's
 * channelActions it picks the OCCUPANT (the in-flight, active action) and the CANDIDATE (top-scored free
 * eligible action); CREATEs the candidate into the slot, or SUPPRESSes a still-cancellable occupant a better
 * candidate out-scores. Bridges completion (hardCompleted -> HARD_COMPLETE, softCompleted -> SOFT_COMPLETE) and
 * the eligibility-drop SUPPRESS, and publishes the durable nba.completion.* / nba.milestone.* facts for the
 * transitions the eval flagged. All outputs ride nba.member.facts (kind=router|completion|milestone). Stateless
 * per eval (the eval self-describes active/eligible/cancellable). Optionally write-throughs nba:eligibility.
 */
public class RouterFn extends RichFlatMapFunction<FactRecord, KafkaOut> {
    private final String redisHost; private final int redisPort; private final boolean writeThrough;
    private transient JedisPooled redis;

    public RouterFn(String redisHost, int redisPort, boolean writeThrough) {
        this.redisHost = redisHost; this.redisPort = redisPort; this.writeThrough = writeThrough;
    }

    @Override public void open(Configuration p) { if (writeThrough) redis = new JedisPooled(redisHost, redisPort); }
    @Override public void close() { if (redis != null) redis.close(); }

    @Override
    public void flatMap(FactRecord rec, Collector<KafkaOut> out) throws Exception {
        JsonNode eval;
        try { eval = SnapshotLogic.M.readTree(rec.value); } catch (Exception e) { return; }
        String nbaId = eval.path("nbaId").asText("");
        String et = eval.path("entityType").asText("OPERATOR");
        String eid = eval.path("entityId").asText("");
        String corr = eval.path("correlationId").asText("");
        JsonNode cas = eval.path("channelActions");
        if (!cas.isArray() || eid.isEmpty()) return;
        String memberKey = et + ":" + eid;
        long now = System.currentTimeMillis();

        // occupant = best-scored ACTIVE (in-flight) action; candidate = best-scored free eligible action.
        JsonNode occupant = null, candidate = null;
        double occScore = -1e18, candScore = -1e18;
        String candSlug = null;
        for (JsonNode ca : cas) {
            double score = ca.path("score").isNumber() ? ca.get("score").asDouble() : Double.NEGATIVE_INFINITY;
            String slug = ca.path("actionId").asText() + "::" + ca.path("channel").asText();
            if (ca.path("active").asBoolean(false)) {
                if (score > occScore) { occScore = score; occupant = ca; }
            }
            boolean free = ca.path("eligible").asBoolean(false) && !ca.path("active").asBoolean(false)
                    && !ca.path("hardCompleted").asBoolean(false) && ca.path("score").isNumber();
            if (free && (score > candScore || (score == candScore && candSlug != null && slug.compareTo(candSlug) < 0))) {
                candScore = score; candidate = ca; candSlug = slug;
            }
        }

        // slot decision
        if (occupant != null && candidate != null && candScore > occScore && occupant.path("cancellable").asBoolean(false))
            out.collect(router(memberKey, "SUPPRESS", occupant, nbaId, et, eid, corr, now));
        else if (occupant == null && candidate != null)
            out.collect(router(memberKey, "CREATE", candidate, nbaId, et, eid, corr, now));

        // per-action: completion bridge + eligibility-drop suppress
        for (JsonNode ca : cas) {
            if (ca.path("hardCompleted").asBoolean(false))
                out.collect(router(memberKey, "HARD_COMPLETE", ca, nbaId, et, eid, corr, now));
            else if (ca.path("softCompleted").asBoolean(false))
                out.collect(router(memberKey, "SOFT_COMPLETE", ca, nbaId, et, eid, corr, now));
            if (!ca.path("eligible").asBoolean(false) && ca.path("cancellable").asBoolean(false))
                out.collect(router(memberKey, "SUPPRESS", ca, nbaId, et, eid, corr, now));
        }

        // durable completion / milestone facts for the transitions the eval flagged
        for (JsonNode aid : eval.path("newCompleted"))
            out.collect(fact(memberKey, et, eid, nbaId, "nba.completion." + aid.asText(), "completion", now));
        for (JsonNode m : eval.path("newMilestones"))
            out.collect(fact(memberKey, et, eid, nbaId, "nba.milestone." + m.path("id").asText(), "milestone", now));

        if (redis != null) {   // write-through the eligibility object (hot path read store) — stripped of transitions
            ObjectNode e = (ObjectNode) eval.deepCopy();
            e.remove("newCompleted"); e.remove("newMilestones");
            redis.set("nba:eligibility:" + nbaId, SnapshotLogic.M.writeValueAsString(e));
        }
    }

    private static KafkaOut router(String memberKey, String op, JsonNode ca, String nbaId, String et, String eid, String corr, long now) {
        ObjectNode a = SnapshotLogic.M.createObjectNode();
        a.put("op", op); a.put("nbaId", nbaId); a.put("entityType", et); a.put("entityId", eid); a.put("memberId", eid);
        a.put("actionId", ca.path("actionId").asText()); a.put("channel", ca.path("channel").asText());
        a.put("name", ca.path("name").asText("")); a.put("contentKey", ca.path("contentKey").asText(""));
        a.put("ttlSeconds", ca.path("ttlSeconds").asLong(0));
        if (ca.path("score").isNumber()) a.put("score", ca.get("score").asDouble());
        a.put("correlationId", corr); a.put("source", "action-router"); a.put("eventTs", now);
        try { return KafkaOut.of(memberKey, SnapshotLogic.M.writeValueAsString(a), "router"); }
        catch (Exception e) { return KafkaOut.of(memberKey, "{}", "router"); }
    }

    private static KafkaOut fact(String memberKey, String et, String eid, String nbaId, String key, String kind, long now) {
        ObjectNode o = SnapshotLogic.M.createObjectNode();
        o.put("entityType", et); o.put("entityId", eid); if (!nbaId.isEmpty()) o.put("nbaId", nbaId);
        o.put("key", key); o.put("value", true); o.put("valueType", "BOOLEAN"); o.put("eventTs", now); o.put("source", "action-router");
        try { return KafkaOut.of(memberKey, SnapshotLogic.M.writeValueAsString(o), kind); }
        catch (Exception e) { return KafkaOut.of(memberKey, "{}", kind); }
    }
}
