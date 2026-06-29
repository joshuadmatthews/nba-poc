package ai.das.nba.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Snapshot-stage pure logic — the SAME faithful port used by the Kafka Streams engine (classify / lean-filter +
 * NBAID-resolve / event-time LWW / buildSnapshotJson), reused verbatim here so all three reference
 * implementations (classic snapshot-builder, KStreams engine, this Flink job) make byte-for-byte identical
 * snapshot decisions. Only the streaming shell differs.
 */
public final class SnapshotLogic {
    static final ObjectMapper M = new ObjectMapper();

    private SnapshotLogic() {}

    public interface NbaIdResolver extends Serializable { String resolve(String entityType, String entityId); }

    /** A record parsed off the source: kafka key + body + the "kind" header + the raw value. */
    public record Parsed(String kafkaKey, JsonNode value, String kind, String raw) {}

    /** A fact destined for the member snapshot (post lean-filter, NBAID resolved). */
    public record SnapFact(String nbaId, String entityType, String entityId, String factKey, long eventTs, ObjectNode fv) {}

    /** Where a forward goes — DEFS (nba.definitions broadcast) or FACTS (nba.facts firehose). */
    public enum Route { DEFS, FACTS }

    /** A produce destined for another topic. headerKind = the "kind" header value (firehose), or null. */
    public record Forward(Route route, String key, String value, String headerKind) {}

    /** The per-record classification result: 0+ forwards + at most one snapshot fact (null if none). */
    public record Classified(List<Forward> forwards, SnapFact snap) {}

    /** Classify ONE record into routes (defs/firehose) + an optional snapshot fact. Routing uses the fact BODY
     *  + the "kind" header, never the kafka key. */
    public static Classified classifyOne(Parsed p, Set<String> ruleFacts, NbaIdResolver resolver) {
        List<Forward> forwards = new ArrayList<>();
        JsonNode f = p.value();
        String fkey = f.path("key").asText("");

        if (fkey.startsWith("nba.throttle.")) {
            forwards.add(new Forward(Route.DEFS, "THROTTLE:" + fkey.substring("nba.throttle.".length()), p.raw(), null));
            return new Classified(forwards, null);
        }
        if (fkey.startsWith("nba.actionsuppress.")) {
            forwards.add(new Forward(Route.DEFS, "ACTION_SUPPRESS:" + fkey.substring("nba.actionsuppress.".length()), p.raw(), null));
            return new Classified(forwards, null);
        }

        String kind = p.kind();
        if (kind != null) {
            if ("throttle-suppress".equals(kind)) {
                String channel = fkey.substring(fkey.lastIndexOf('.') + 1);
                if (!channel.isEmpty()) forwards.add(new Forward(Route.DEFS, "THROTTLE_HOT:" + channel, p.raw(), null));
            }
            forwards.add(new Forward(Route.FACTS, p.kafkaKey(), p.raw(), kind));
            if ("router".equals(kind)) return new Classified(forwards, null);
        }

        SnapFact sf = toSnapFact(f, ruleFacts, resolver);
        return new Classified(forwards, sf);
    }

    /** Build the snapshot fact (lean-filter + NBAID resolve + value flattening), or null if irrelevant. */
    public static SnapFact toSnapFact(JsonNode f, Set<String> ruleFacts, NbaIdResolver resolver) {
        String entityType = f.path("entityType").asText("");
        String entityId = f.path("entityId").asText("");
        String key = f.path("key").asText("");
        if (entityType.isEmpty() || entityId.isEmpty() || key.isEmpty()) return null;

        boolean isScore = key.startsWith("nba.score.");
        boolean isState = key.startsWith("nba.actionstate.");
        boolean isDisposition = key.startsWith("nba.disposition.");
        boolean isCompletion = key.startsWith("nba.completion.");
        boolean isMilestone = key.startsWith("nba.milestone.");
        boolean alwaysAttach = isScore || isState || isDisposition || isCompletion || isMilestone;
        if (!alwaysAttach && !ruleFacts.isEmpty() && !ruleFacts.contains(key)) return null;

        long eventTs = f.path("eventTs").asLong(0);
        String nbaId = f.hasNonNull("nbaId") ? f.get("nbaId").asText() : resolver.resolve(entityType, entityId);
        if (nbaId == null || nbaId.isEmpty()) return null;

        ObjectNode fv = M.createObjectNode();
        if (isScore) {
            JsonNode v = f.get("value");
            fv.set("value", (v != null && v.has("score")) ? v.get("score") : v);
            fv.put("valueType", "DOUBLE");
        } else {
            fv.set("value", f.get("value"));
            fv.put("valueType", f.path("valueType").asText("STRING"));
        }
        fv.put("eventTs", eventTs);
        fv.put("source", f.path("source").asText(""));
        return new SnapFact(nbaId, entityType, entityId, key, eventTs, fv);
    }

    /** Event-time last-writer-wins for ONE fact into the member's snapshot hash. Returns true if the hash changed. */
    public static boolean applyLww(Map<String, String> hash, SnapFact sf, long nowMs) {
        String field = "fact:" + sf.factKey();
        String cur = hash.get(field);
        if (cur != null) {
            long curTs = -1;
            try { curTs = M.readTree(cur).path("eventTs").asLong(-1); } catch (Exception ignore) {}
            if (sf.eventTs() <= curTs) return false;
        }
        hash.put(field, fvJson(sf.fv()));
        hash.put("__entityType", sf.entityType());
        hash.put("__entityId", sf.entityId());
        hash.put("__nbaId", sf.nbaId());
        hash.put("__updatedTs", String.valueOf(nowMs));
        return true;
    }

    /** Build the full snapshot JSON for a member from its hash. */
    public static String buildSnapshotJson(String nbaId, Map<String, String> all) throws Exception {
        ObjectNode snap = M.createObjectNode();
        snap.put("nbaId", nbaId);
        snap.put("entityType", all.getOrDefault("__entityType", ""));
        snap.put("entityId", all.getOrDefault("__entityId", ""));
        snap.put("correlationId", UUID.randomUUID().toString());
        snap.put("updatedTs", Long.parseLong(all.getOrDefault("__updatedTs", "0")));
        ObjectNode facts = snap.putObject("facts");
        for (Map.Entry<String, String> e : all.entrySet())
            if (e.getKey().startsWith("fact:")) facts.set(e.getKey().substring(5), M.readTree(e.getValue()));
        return M.writeValueAsString(snap);
    }

    public static String snapKey(String nbaId) { return "nba:snapshot:" + nbaId; }
    public static String fvJson(ObjectNode fv) { try { return M.writeValueAsString(fv); } catch (Exception e) { throw new RuntimeException(e); } }
}
