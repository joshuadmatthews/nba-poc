package ai.das.nba.router;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Component-level unit tests for the Action Router's PURE decision core + the Kafka emit path.
 *
 * The router is the BRIDGE from the rules-engine eval to the state machine. It emits exactly FOUR
 * router decisions (op = CREATE / SUPPRESS / SOFT_COMPLETE / HARD_COMPLETE, kind=router header) and NEVER
 * inspects a disposition state — it reads coarse flags the rules engine projected onto each ChannelAction:
 * {@code active} (a workflow is in flight), {@code cancellable} (CREATED, not sent yet),
 * {@code softCompleted}, {@code hardCompleted}. These tests pin that contract.
 */
class ActionRouterTest {

    // ---- builders: a ChannelAction carries score + the coarse router flags ----------------
    static JsonNode ca(String actionId, String channel, Double score) { return ca(actionId, channel, score, false, false); }
    static JsonNode ca(String actionId, String channel, Double score, boolean active, boolean cancellable) {
        ObjectNode o = ActionRouter.M.createObjectNode();
        o.put("actionId", actionId);
        o.put("channel", channel);
        o.put("name", actionId + " name");
        o.put("contentKey", "content:" + actionId);
        o.put("ttlSeconds", 3600L);
        if (score != null) o.put("score", score.doubleValue());
        o.put("eligible", true);          // a ChannelAction on the unified list; default eligible (passed the rules)
        o.put("active", active);
        o.put("cancellable", cancellable);
        return o;
    }
    /** add soft/hard completion flags to a ChannelAction. */
    static JsonNode withCompletion(JsonNode ca, boolean soft, boolean hard) {
        ((ObjectNode) ca).put("softCompleted", soft); ((ObjectNode) ca).put("hardCompleted", hard); return ca;
    }

    static MockProducer<String, String> mock() { return new MockProducer<>(true, new StringSerializer(), new StringSerializer()); }
    static String headerKind(ProducerRecord<String, String> r) {
        Header h = r.headers().lastHeader("kind"); return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
    static ArrayNode arr(JsonNode... nodes) { ArrayNode a = ActionRouter.M.createArrayNode(); for (JsonNode n : nodes) a.add(n); return a; }
    static String op(ProducerRecord<String, String> r) { try { return ActionRouter.M.readTree(r.value()).get("op").asText(); } catch (Exception e) { throw new RuntimeException(e); } }
    static List<String> ops(MockProducer<String, String> p) { return p.history().stream().map(ActionRouterTest::op).sorted().toList(); }

    // ==== isSuppressed + slug (unchanged key formats) ======================================
    @Test void wholeActionSuppressionByActionId() {
        assertTrue(ActionRouter.isSuppressed(Set.of("A1"), ca("A1", "email", 5.0)));
        assertTrue(ActionRouter.isSuppressed(Set.of("A1"), ca("A1", "sms", 5.0)));
    }
    @Test void actionChannelSuppressionUsesDotKeyFormat() {
        JsonNode a = ca("A1", "email", 5.0);
        assertTrue(ActionRouter.isSuppressed(Set.of("A1.email"), a));
        assertFalse(ActionRouter.isSuppressed(Set.of("A1.sms"), a));
        assertFalse(ActionRouter.isSuppressed(Set.of("A1::email"), a), "the '::' slug format is NOT the suppression key format");
    }
    @Test void nonSuppressedActionPasses() {
        assertFalse(ActionRouter.isSuppressed(Set.of("OTHER", "B2.sms"), ca("A1", "email", 5.0)));
        assertFalse(ActionRouter.isSuppressed(Set.of(), ca("A1", "email", 5.0)));
    }
    @Test void slugIsActionIdDoubleColonChannel() { assertEquals("A1::email", ActionRouter.slug(ca("A1", "email", 5.0))); }
    @Test void distinctActionChannelPairsGetDistinctSlugs() {
        assertNotEquals(ActionRouter.slug(ca("A1", "email", 1.0)), ActionRouter.slug(ca("A1", "sms", 1.0)));
        assertNotEquals(ActionRouter.slug(ca("A1", "email", 1.0)), ActionRouter.slug(ca("A2", "email", 1.0)));
    }

    // ==== pickCandidate: top free (not active, not hard-completed, scored) ==================
    @Test void topScoredFreeCandidateIsChosen() {
        JsonNode cand = ActionRouter.pickCandidate(arr(ca("A1", "email", 3.0), ca("A2", "email", 9.0), ca("A3", "sms", 7.0)), Set.of());
        assertEquals("A2", cand.path("actionId").asText());
    }
    @Test void candidateTieBreaksOnSmallestSlug() {
        JsonNode cand = ActionRouter.pickCandidate(arr(ca("A2", "email", 9.0), ca("A1", "email", 9.0)), Set.of());
        assertEquals("A1", cand.path("actionId").asText());
    }
    @Test void candidateSkipsSuppressedActiveHardCompletedAndUnscored() {
        ArrayNode eligible = arr(
                ca("SUP", "email", 100.0),                       // operator-suppressed -> skipped
                ca("ACTIVE", "email", 50.0, true, false),        // a workflow in flight -> skipped
                withCompletion(ca("DONE", "email", 40.0), false, true),  // already converted -> skipped
                ca("NOSCORE", "email", null),                    // unscored -> skipped
                ca("OK", "email", 5.0));                         // the only free, scored candidate
        assertEquals("OK", ActionRouter.pickCandidate(eligible, Set.of("SUP")).path("actionId").asText());
    }
    @Test void noFreeCandidateReturnsNull() {
        assertNull(ActionRouter.pickCandidate(arr(ca("A", "email", 50.0, true, false), ca("B", "email", 40.0, true, true)), Set.of()));
    }

    // ==== selectBatch: top-N free on the winning channel ===================================
    @Test void batchSelectsTopNOnWinningChannelOrderedByScore() {
        ArrayNode eligible = arr(ca("A1", "email", 3.0), ca("A2", "email", 9.0), ca("A3", "email", 7.0),
                ca("A4", "sms", 100.0), ca("A5", "email", 1.0));
        List<JsonNode> batch = ActionRouter.selectBatch(eligible, "email", Set.of(), 2);
        assertEquals(List.of("A2", "A3"), batch.stream().map(n -> n.path("actionId").asText()).toList());
    }
    @Test void batchExcludesSuppressedActiveHardCompletedUnscoredAndOtherChannels() {
        ArrayNode eligible = arr(ca("OK1", "email", 9.0), ca("SUP", "email", 99.0),
                ca("ACTIVE", "email", 8.0, true, false), withCompletion(ca("DONE", "email", 7.0), false, true),
                ca("NOSCORE", "email", null), ca("SMS", "sms", 50.0), ca("OK2", "email", 4.0));
        List<JsonNode> batch = ActionRouter.selectBatch(eligible, "email", Set.of("SUP"), 10);
        assertEquals(List.of("OK1", "OK2"), batch.stream().map(n -> n.path("actionId").asText()).toList());
    }

    // ==== activate: the 4-event bridge =====================================================
    static redis.clients.jedis.JedisPooled deadRedis() { return new redis.clients.jedis.JedisPooled("127.0.0.1", 1); }
    static String evalJson(String body) {
        return "{\"nbaId\":\"nba_1\",\"entityType\":\"Lead\",\"entityId\":\"42\",\"correlationId\":\"corr-1\"," + body + "}";
    }

    @Test void activateEmitsSingleCreateForTopFreeCandidate() throws Exception {
        MockProducer<String, String> p = mock();
        String eval = evalJson("\"channelActions\":[" +
                "{\"actionId\":\"A1\",\"channel\":\"email\",\"score\":9.0,\"eligible\":true,\"active\":false}," +
                "{\"actionId\":\"A2\",\"channel\":\"email\",\"score\":3.0,\"eligible\":true,\"active\":false}]");
        ActionRouter.activate(eval, deadRedis(), p, "nba.member.facts");
        assertEquals(1, p.history().size());
        ProducerRecord<String, String> rec = p.history().get(0);
        assertEquals("router", headerKind(rec));
        assertEquals("nba_1:A1:email", rec.key());
        JsonNode v = ActionRouter.M.readTree(rec.value());
        assertEquals("CREATE", v.get("op").asText());
        assertEquals("A1", v.get("actionId").asText());
        assertEquals("corr-1", v.get("correlationId").asText());
    }

    @Test void activateBridgesSoftAndHardCompletion() throws Exception {
        MockProducer<String, String> p = mock();
        // Both soft and hard are RULE-decided (rules engine) and bridged here. hardCompleted wins over soft on
        // the same ChannelAction. Both active (in flight) so neither is a free candidate -> only bridges fire.
        String eval = evalJson("\"channelActions\":[" +
                "{\"actionId\":\"S\",\"channel\":\"email\",\"score\":5.0,\"eligible\":true,\"active\":true,\"softCompleted\":true,\"hardCompleted\":false}," +
                "{\"actionId\":\"H\",\"channel\":\"sms\",\"score\":4.0,\"eligible\":true,\"active\":true,\"softCompleted\":true,\"hardCompleted\":true}]");
        ActionRouter.activate(eval, deadRedis(), p, "nba.member.facts");
        // H is hardCompleted -> HARD_COMPLETE (hard wins over soft); S -> SOFT_COMPLETE. No CREATE (none free).
        assertEquals(List.of("HARD_COMPLETE", "SOFT_COMPLETE"), ops(p));
    }

    @Test void activateHardCompletesEvenWhenNoLongerEligible() throws Exception {
        MockProducer<String, String> p = mock();
        // an auto-excluded completion is eligible:false but still ON the list (workflow walking) -> HARD_COMPLETE
        // still bridges (per-channel) so the workflow reaches HARD_COMPLETED. Not cancellable -> no SUPPRESS.
        String eval = evalJson("\"channelActions\":[" +
                "{\"actionId\":\"DONE\",\"channel\":\"push\",\"eligible\":false,\"active\":true,\"cancellable\":false,\"workflowState\":\"PRESENTED\",\"hardCompleted\":true}]");
        ActionRouter.activate(eval, deadRedis(), p, "nba.member.facts");
        assertEquals(List.of("HARD_COMPLETE"), ops(p));
        JsonNode v = ActionRouter.M.readTree(p.history().get(0).value());
        assertEquals("DONE", v.get("actionId").asText());
        assertEquals("push", v.get("channel").asText());
    }

    @Test void activateSuppressesNotSentYetInFlightThatFellOutOfEligibility() throws Exception {
        MockProducer<String, String> p = mock();
        // an in-flight action that's still cancellable (CREATED) but no longer eligible (eligible:false) -> SUPPRESS.
        String eval = evalJson("\"channelActions\":[" +
                "{\"actionId\":\"OLD\",\"channel\":\"email\",\"score\":2.0,\"eligible\":false,\"active\":true,\"cancellable\":true}]");
        ActionRouter.activate(eval, deadRedis(), p, "nba.member.facts");
        assertEquals(List.of("SUPPRESS"), ops(p));
        assertEquals("OLD", ActionRouter.M.readTree(p.history().get(0).value()).get("actionId").asText());
    }

    @Test void activateSupersedesCancellableOccupantWhenOutscored() throws Exception {
        MockProducer<String, String> p = mock();
        // a higher-scored FREE candidate beats a still-cancellable (not sent) occupant -> SUPPRESS the occupant,
        // hold the CREATE for next round (the occupant -> SUPPRESSED then frees the slot).
        String eval = evalJson("\"channelActions\":[" +
                "{\"actionId\":\"WIN\",\"channel\":\"email\",\"score\":9.0,\"eligible\":true,\"active\":false}," +
                "{\"actionId\":\"OCC\",\"channel\":\"email\",\"score\":2.0,\"eligible\":true,\"active\":true,\"cancellable\":true}]");
        ActionRouter.activate(eval, deadRedis(), p, "nba.member.facts");
        assertEquals(List.of("SUPPRESS"), ops(p), "supersede emits SUPPRESS and holds the CREATE this round");
        assertEquals("OCC", ActionRouter.M.readTree(p.history().get(0).value()).get("actionId").asText());
    }

    @Test void activateHoldsWhenSentOccupantOwnsTheSlot() throws Exception {
        MockProducer<String, String> p = mock();
        // the occupant is active but NOT cancellable (already sent) -> it owns the slot, no SUPPRESS, no CREATE.
        String eval = evalJson("\"channelActions\":[" +
                "{\"actionId\":\"WIN\",\"channel\":\"email\",\"score\":9.0,\"eligible\":true,\"active\":false}," +
                "{\"actionId\":\"OCC\",\"channel\":\"email\",\"score\":2.0,\"eligible\":true,\"active\":true,\"cancellable\":false}]");
        ActionRouter.activate(eval, deadRedis(), p, "nba.member.facts");
        assertTrue(p.history().isEmpty(), "a sent occupant holds the slot — nothing is emitted");
    }

    @Test void activateSkipsOperatorSuppressedTopAndCreatesNext() throws Exception {
        ActionRouter.SUPPRESSED.clear(); ActionRouter.SUPPRESSED.add("TOP");
        try {
            MockProducer<String, String> p = mock();
            String eval = evalJson("\"channelActions\":[" +
                    "{\"actionId\":\"TOP\",\"channel\":\"email\",\"score\":100.0,\"eligible\":true,\"active\":false}," +
                    "{\"actionId\":\"NEXT\",\"channel\":\"email\",\"score\":5.0,\"eligible\":true,\"active\":false}]");
            ActionRouter.activate(eval, deadRedis(), p, "nba.member.facts");
            assertEquals(1, p.history().size());
            JsonNode v = ActionRouter.M.readTree(p.history().get(0).value());
            assertEquals("CREATE", v.get("op").asText());
            assertEquals("NEXT", v.get("actionId").asText());
        } finally { ActionRouter.SUPPRESSED.clear(); }
    }

    // ==== emit / emitBatch shape ===========================================================
    @Test void emitProducesRouterFactWithExpectedKeyAndShape() throws Exception {
        MockProducer<String, String> p = mock();
        ActionRouter.emit(p, "nba.member.facts", "SUPPRESS", "nba_1", "Lead", "42", "corr-9", ca("A7", "voice", 6.5));
        ProducerRecord<String, String> rec = p.history().get(0);
        assertEquals("nba_1:A7:voice", rec.key());
        assertEquals("router", headerKind(rec));
        JsonNode v = ActionRouter.M.readTree(rec.value());
        assertEquals("SUPPRESS", v.get("op").asText());
        assertEquals("A7", v.get("actionId").asText());
        assertEquals(6.5, v.get("score").asDouble());
        assertTrue(v.get("eventTs").asLong() > 0);
    }
    @Test void emitBatchProducesOneCreateCarryingActionsArray() throws Exception {
        MockProducer<String, String> p = mock();
        ActionRouter.emitBatch(p, "nba.member.facts", "nba_1", "Lead", "42", "corr-3", "email",
                List.of(ca("A2", "email", 9.0), ca("A3", "email", 7.0)));
        ProducerRecord<String, String> rec = p.history().get(0);
        assertEquals("nba_1:email:batch", rec.key());
        JsonNode v = ActionRouter.M.readTree(rec.value());
        assertEquals("CREATE_BATCH", v.get("op").asText(), "batch is an explicit CREATE_BATCH event");
        JsonNode actions = v.get("actions");
        assertEquals(2, actions.size());
        assertEquals("A2", actions.get(0).get("actionId").asText());
    }
    @Test void emitBatchEmptyBatchProducesNothing() {
        MockProducer<String, String> p = mock();
        ActionRouter.emitBatch(p, "nba.member.facts", "nba_1", "Lead", "42", "corr-3", "email", List.of());
        assertEquals(0, p.history().size());
    }

    // ==== dlqEnvelope ======================================================================
    @Test void dlqEnvelopeCapturesSourceCoordinatesAndIsReplayable() throws Exception {
        ConsumerRecord<String, String> r = new ConsumerRecord<>("nba.evaluations", 2, 17L, "nba_1", "{\"nbaId\":\"nba_1\"}");
        r.headers().add("kind", "eval".getBytes(StandardCharsets.UTF_8));
        ProducerRecord<String, String> pr = ActionRouter.dlqEnvelope("nba.dlq.action-router", "action-router", r, "boom");
        assertEquals("nba.dlq.action-router", pr.topic());
        assertEquals("nba_1", pr.key());
        JsonNode env = ActionRouter.M.readTree(pr.value());
        assertEquals("nba.evaluations", env.get("topic").asText());
        assertEquals(17, env.get("offset").asLong());
        assertEquals("{\"nbaId\":\"nba_1\"}", env.get("value").asText());
        assertEquals("boom", env.get("error").asText());
    }
    @Test void dlqEnvelopeHandlesNullKey() throws Exception {
        ConsumerRecord<String, String> r = new ConsumerRecord<>("nba.evaluations", 0, 1L, null, "POISON");
        ProducerRecord<String, String> pr = ActionRouter.dlqEnvelope("nba.dlq.action-router", "action-router", r, "parse failed");
        assertNull(pr.key());
        assertEquals("POISON", ActionRouter.M.readTree(pr.value()).get("value").asText());
    }
}
