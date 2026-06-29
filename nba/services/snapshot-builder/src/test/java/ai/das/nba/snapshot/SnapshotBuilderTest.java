package ai.das.nba.snapshot;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the batched snapshot builder's PURE planning core (no Kafka/Redis).
 *
 * The headline test is {@link #wholeBatchRetryReEmitsSnapshotsWithoutOutbox()} — it proves the
 * additive-retry contract the design depends on: re-running a committed batch (Redis already holds
 * the facts) produces ZERO new writes yet STILL re-emits a snapshot for every member, so a failed
 * Kafka commit can be retried whole with no outbox.
 */
class SnapshotBuilderTest {
    static final String FACTS = "nba.facts";
    static final String DEFS = "nba.definitions";

    // deterministic, dependency-free NBAID resolver: entityType:entityId -> nba_{entityId padded}
    static final SnapshotBuilder.NbaIdResolver RESOLVER =
            (et, id) -> "nba_" + ("000000000000" + id).substring(Math.max(0, ("000000000000" + id).length() - 12));

    static SnapshotBuilder.Parsed mf(String entityType, String entityId, String key, long ts, String value, String kind) {
        String raw = "{\"entityType\":\"" + entityType + "\",\"entityId\":\"" + entityId + "\",\"key\":\"" + key
                + "\",\"value\":" + value + ",\"valueType\":\"LONG\",\"eventTs\":" + ts + ",\"source\":\"test\"}";
        try {
            JsonNode v = SnapshotBuilder.M.readTree(raw);
            return new SnapshotBuilder.Parsed(entityType + ":" + entityId, v, kind, raw);
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    static Map<String, Long> curTsFrom(List<SnapshotBuilder.SnapFact> facts) {
        Map<String, Long> cur = new HashMap<>();
        for (SnapshotBuilder.SnapFact sf : facts) cur.put(SnapshotBuilder.ref(sf.nbaId(), sf.factKey()), sf.eventTs());
        return cur;
    }

    @Test
    void wholeBatchRetryReEmitsSnapshotsWithoutOutbox() {
        // A batch with two members' facts (rulefacts fail-open: empty set -> snapshot all).
        List<SnapshotBuilder.Parsed> batch = List.of(
                mf("Lead", "1", "operator.x", 100, "5", null),
                mf("Lead", "2", "operator.y", 100, "7", null));
        SnapshotBuilder.BatchPlan plan = SnapshotBuilder.classify(batch, Set.of(), RESOLVER, FACTS, DEFS);

        // both members are scheduled for (re)emit, regardless of LWW outcome
        assertEquals(2, plan.emitMembers.size(), "every member in the batch must be emitted");
        assertEquals(2, plan.snapFacts.size());

        // ATTEMPT 1 — Redis empty -> both facts win and would be written
        SnapshotBuilder.Winners first = SnapshotBuilder.selectWinners(plan.snapFacts, new HashMap<>());
        assertEquals(2, first.byKey.size(), "first attempt writes both members");

        // ATTEMPT 2 (retry after a Kafka-commit failure) — Redis ALREADY holds the facts at the same
        // eventTs, so LWW drops everything: ZERO writes...
        SnapshotBuilder.Winners retry = SnapshotBuilder.selectWinners(plan.snapFacts, curTsFrom(plan.snapFacts));
        assertEquals(0, retry.byKey.size(), "retry must not re-write already-applied facts");

        // ...yet the emit set is unchanged, so the snapshots are STILL re-emitted on the retry.
        // This is the property that lets us retry the whole batch with no outbox.
        assertEquals(2, plan.emitMembers.size(), "snapshots re-emit on retry even with no new writes");
        assertTrue(plan.emitMembers.contains("nba_000000000001"));
        assertTrue(plan.emitMembers.contains("nba_000000000002"));
    }

    @Test
    void lwwKeepsOnlyTheNewestFactPerKeyWithinABatch() {
        List<SnapshotBuilder.Parsed> batch = List.of(
                mf("Lead", "1", "operator.x", 100, "5", null),
                mf("Lead", "1", "operator.x", 200, "9", null),   // newer — wins
                mf("Lead", "1", "operator.x", 150, "7", null));  // out-of-order older — dropped
        SnapshotBuilder.BatchPlan plan = SnapshotBuilder.classify(batch, Set.of(), RESOLVER, FACTS, DEFS);
        assertEquals(1, plan.emitMembers.size());

        SnapshotBuilder.Winners w = SnapshotBuilder.selectWinners(plan.snapFacts, new HashMap<>());
        assertEquals(1, w.byKey.size());
        String fv = w.byKey.get(SnapshotBuilder.snapKey("nba_000000000001")).get("fact:operator.x");
        assertTrue(fv.contains("\"eventTs\":200"), "the newest fact in the batch wins: " + fv);
    }

    @Test
    void staleFactBelowStoredIsDropped() {
        List<SnapshotBuilder.Parsed> batch = List.of(mf("Lead", "1", "operator.x", 100, "5", null));
        SnapshotBuilder.BatchPlan plan = SnapshotBuilder.classify(batch, Set.of(), RESOLVER, FACTS, DEFS);
        Map<String, Long> cur = new HashMap<>();
        cur.put(SnapshotBuilder.ref("nba_000000000001", "operator.x"), 500L);   // stored is newer
        SnapshotBuilder.Winners w = SnapshotBuilder.selectWinners(plan.snapFacts, cur);
        assertEquals(0, w.byKey.size(), "an older event must not overwrite a newer stored value");
    }

    @Test
    void throttleRoutesToDefinitionsAndNeverSnapshots() {
        List<SnapshotBuilder.Parsed> batch = List.of(
                mf("SYSTEM", "throttle", "nba.throttle.email.daily", 100, "153", null));
        SnapshotBuilder.BatchPlan plan = SnapshotBuilder.classify(batch, Set.of(), RESOLVER, FACTS, DEFS);
        assertEquals(0, plan.snapFacts.size());
        assertEquals(0, plan.emitMembers.size());
        assertEquals(1, plan.forwards.size());
        assertEquals(DEFS, plan.forwards.get(0).topic);
        assertEquals("THROTTLE:email.daily", plan.forwards.get(0).key);
    }

    @Test
    void routerDecisionFirehosesButDoesNotSnapshot() {
        // router decisions carry a kind=router header; they firehose to nba.facts but are not member attributes
        List<SnapshotBuilder.Parsed> batch = List.of(
                mf("Lead", "1", "nba.decision", 100, "1", "router"));
        SnapshotBuilder.BatchPlan plan = SnapshotBuilder.classify(batch, Set.of(), RESOLVER, FACTS, DEFS);
        assertEquals(0, plan.emitMembers.size(), "router decisions are not snapshotted");
        assertEquals(1, plan.forwards.size());
        assertEquals(FACTS, plan.forwards.get(0).topic, "router decision is firehosed to nba.facts");
    }

    @Test
    void leanFilterSkipsFactsNoRuleUses() {
        List<SnapshotBuilder.Parsed> batch = List.of(
                mf("Lead", "1", "operator.unused", 100, "5", null),
                mf("Lead", "1", "operator.used", 100, "5", null));
        // only operator.used is referenced by a rule
        SnapshotBuilder.BatchPlan plan = SnapshotBuilder.classify(batch, Set.of("operator.used"), RESOLVER, FACTS, DEFS);
        assertEquals(1, plan.snapFacts.size());
        assertEquals("operator.used", plan.snapFacts.get(0).factKey());
    }

    @Test
    void dispositionAndCompletionFactsAlwaysAttachEvenWhenNotARuleFact() {
        // ruleFacts is non-empty and does NOT list these keys, yet disposition (soft-completion funnel) and
        // completion (hard-completion signal) facts must still snapshot — the rules engine reads them.
        List<SnapshotBuilder.Parsed> batch = List.of(
                mf("Lead", "1", "nba.disposition.try_chat.email", 100, "\"Read\"", null),
                mf("Lead", "1", "nba.completion.try_chat", 100, "\"completed\"", null),
                mf("Lead", "1", "operator.unused", 100, "5", null));   // not in ruleFacts -> still lean-filtered out
        SnapshotBuilder.BatchPlan plan = SnapshotBuilder.classify(batch, Set.of("operator.something"), RESOLVER, FACTS, DEFS);
        java.util.Set<String> keys = new java.util.HashSet<>();
        for (SnapshotBuilder.SnapFact sf : plan.snapFacts) keys.add(sf.factKey());
        assertTrue(keys.contains("nba.disposition.try_chat.email"), "disposition always attaches");
        assertTrue(keys.contains("nba.completion.try_chat"), "completion always attaches");
        assertFalse(keys.contains("operator.unused"), "a non-rule fact is still lean-filtered out");
    }

    @Test
    void buildsSnapshotJsonFromHash() throws Exception {
        Map<String, String> hash = new HashMap<>();
        hash.put("__nbaId", "nba_000000000001");
        hash.put("__entityType", "Lead");
        hash.put("__entityId", "1");
        hash.put("__updatedTs", "12345");
        hash.put("fact:operator.x", "{\"value\":5,\"valueType\":\"LONG\",\"eventTs\":100,\"source\":\"test\"}");
        String json = SnapshotBuilder.buildSnapshotJson("nba_000000000001", hash);
        JsonNode snap = SnapshotBuilder.M.readTree(json);
        assertEquals("nba_000000000001", snap.get("nbaId").asText());
        assertEquals("Lead", snap.get("entityType").asText());
        assertTrue(snap.get("facts").has("operator.x"));
        assertEquals(5, snap.get("facts").get("operator.x").get("value").asInt());
        assertFalse(snap.get("correlationId").asText().isEmpty(), "a fresh correlationId is born on each emit");
    }

    @Test
    void dlqEnvelopeCapturesSourceCoordinatesAndIsReplayable() throws Exception {
        // a poison record off the SOURCE topic, with a header
        ConsumerRecord<String, String> r =
                new ConsumerRecord<>("nba.member.facts", 3, 42L, "Lead:1", "{\"key\":\"operator.x\",\"value\":5}");
        r.headers().add("kind", "disposition".getBytes(StandardCharsets.UTF_8));

        ProducerRecord<String, String> pr = SnapshotBuilder.dlqEnvelope("nba.dlq.snapshot-builder", "snapshot-builder", r, "boom");
        assertEquals("nba.dlq.snapshot-builder", pr.topic(), "envelope goes to the per-consumer DLQ topic");
        assertEquals("Lead:1", pr.key(), "DLQ record keeps the original key so a replay keeps partition affinity");

        JsonNode env = SnapshotBuilder.M.readTree(pr.value());
        assertEquals("snapshot-builder", env.get("consumer").asText());
        assertEquals("nba.member.facts", env.get("topic").asText(), "SOURCE topic recorded -> replay re-produces there");
        assertEquals(3, env.get("partition").asInt());
        assertEquals(42, env.get("offset").asLong());
        assertEquals("Lead:1", env.get("key").asText());
        assertEquals("{\"key\":\"operator.x\",\"value\":5}", env.get("value").asText(), "raw value preserved verbatim -> exact replay");
        assertEquals("disposition", env.get("headers").get("kind").asText(), "headers carried for faithful replay");
        assertEquals("boom", env.get("error").asText());
        assertTrue(env.get("dlqTs").asLong() > 0, "dlqTs stamped");
    }

    @Test
    void dlqEnvelopeHandlesNullKey() throws Exception {
        ConsumerRecord<String, String> r = new ConsumerRecord<>("nba.evaluations", 0, 7L, null, "POISON-not-json");
        ProducerRecord<String, String> pr = SnapshotBuilder.dlqEnvelope("nba.dlq.x", "ml-scorer-scorer", r, "parse failed");
        assertNull(pr.key(), "a null-keyed source record yields a null-keyed DLQ record");
        JsonNode env = SnapshotBuilder.M.readTree(pr.value());
        assertTrue(env.get("key").isNull());
        assertEquals("POISON-not-json", env.get("value").asText(), "even unparseable payloads are preserved for inspection/replay");
    }
}
