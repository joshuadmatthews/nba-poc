package ai.das.nba.actionlayer;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Component-level unit tests for the NBA action-layer's disposition-emission core (no Kafka broker).
 *
 * The action-layer consumes nba.activations and, on DISPATCH/CANCEL, emits a disposition MEMBER FACT
 * (nba.disposition.{actionId}.{channel} = sent|suppressed|failed) back onto nba.member.facts. These
 * tests pin the exact shape/keying of that fact and the BATCH fan-out (one disposition per action),
 * using Kafka's in-memory {@link MockProducer} to capture the emitted records.
 *
 * The cancel-vs-send race (CANCEL before the scheduled send WINS -> suppressed; after it fired LOSES
 * -> sent) is NOT covered here as a pure unit: in production it is an emergent property of a
 * ConcurrentHashMap claim race between the consumer loop and the background sender thread (see
 * ActionLayer.main / PENDING), not a single time-comparison function. Extracting a fake time helper
 * would not be behavior-preserving, so per the brief that race is left to integration-level testing.
 * What IS unit-tested here is that BOTH outcomes of the race route through disposition(...) and
 * therefore emit a correctly-shaped fact with the right status string.
 */
class ActionLayerTest {
    static final String FACTS = "nba.member.facts";

    static final String MEMBER_FACTS = FACTS;

    static MockProducer<String, String> newProducer() {
        // autoComplete=true: send() futures resolve immediately so emitDisposition's fire-and-forget
        // send returns without a broker; records land in history() for inspection.
        return new MockProducer<>(true, new StringSerializer(), new StringSerializer());
    }

    static JsonNode parse(String raw) {
        try { return ActionLayer.M.readTree(raw); } catch (Exception e) { throw new RuntimeException(e); }
    }

    /** A single (non-batch) DISPATCH-style activation. */
    static JsonNode singleActivation() {
        return parse("{"
                + "\"op\":\"DISPATCH\","
                + "\"entityType\":\"Lead\","
                + "\"memberId\":\"42\","
                + "\"nbaId\":\"nba_000000000042\","
                + "\"channel\":\"email\","
                + "\"actionId\":\"act-A\","
                + "\"contentKey\":\"welcome.v1\","
                + "\"trackingId\":\"trk-A\","
                + "\"correlationId\":\"corr-xyz\""
                + "}");
    }

    /** A batch activation: top-level addressing fields + an actions[] array, each action self-contained. */
    static JsonNode batchActivation() {
        return parse("{"
                + "\"op\":\"DISPATCH\","
                + "\"entityType\":\"Contact\","
                + "\"memberId\":\"7\","
                + "\"nbaId\":\"nba_000000000007\","
                + "\"channel\":\"sms\","
                + "\"correlationId\":\"corr-batch\","
                + "\"actions\":["
                + "  {\"actionId\":\"act-1\",\"contentKey\":\"ck-1\",\"trackingId\":\"trk-1\"},"
                + "  {\"actionId\":\"act-2\",\"contentKey\":\"ck-2\",\"trackingId\":\"trk-2\"},"
                + "  {\"actionId\":\"act-3\",\"contentKey\":\"ck-3\",\"trackingId\":\"trk-3\"}"
                + "]"
                + "}");
    }

    // ------------------------------------------------------------------------------------------------
    // emitDisposition — single fact shape / keying / header
    // ------------------------------------------------------------------------------------------------

    @Test
    void emitDispositionProducesAFullyAddressedMemberFact() throws Exception {
        MockProducer<String, String> p = newProducer();
        JsonNode a = singleActivation();

        // (producer, topic, act, actionId, contentKey, trackingId, canonicalState, rawProviderStatus)
        ActionLayer.emitDisposition(p, MEMBER_FACTS, a, "act-A", "welcome.v1", "trk-A", "PRESENTED", "Opened");

        assertEquals(1, p.history().size(), "exactly one fact emitted");
        ProducerRecord<String, String> rec = p.history().get(0);
        assertEquals(MEMBER_FACTS, rec.topic());
        assertEquals("Lead:42", rec.key(), "record key is entityType:memberId for member partition affinity");

        JsonNode fact = parse(rec.value());
        assertEquals("nba.disposition.act-A.email", fact.get("key").asText());
        assertEquals("Opened", fact.get("value").asText(), "VALUE = the raw provider status (rules engine reads this)");
        assertEquals("PRESENTED", fact.get("state").asText(), "STATE = the canonical delivery state (the state machine walks this)");
        assertEquals("STRING", fact.get("valueType").asText());
        assertEquals("action-layer", fact.get("source").asText());

        assertEquals("Lead", fact.get("entityType").asText());
        assertEquals("42", fact.get("entityId").asText(), "entityId == memberId (layer never knew nbaId)");
        assertEquals("42", fact.get("memberId").asText());
        assertEquals("email", fact.get("channel").asText());
        assertEquals("welcome.v1", fact.get("contentKey").asText());
        assertEquals("trk-A", fact.get("trackingId").asText());
        assertEquals("corr-xyz", fact.get("correlationId").asText());
        assertTrue(fact.get("eventTs").asLong() > 0);

        byte[] kind = rec.headers().lastHeader("kind").value();
        assertEquals("disposition", new String(kind, StandardCharsets.UTF_8));
    }

    @Test
    void emitDispositionCarriesStateAndRawForEachDeliveryStep() {
        // (canonicalState, rawProviderStatus) pairs across the email funnel + a failure/decline. (No SENT: the
        // workflow emits IN_PROCESS on dispatch; delivery dispositions all map to PRESENTED with the raw riding
        // along, so the rules engine can decide soft-completion off the raw — soft is NOT an activation state.)
        String[][] steps = {{"PRESENTED", "Delivered"}, {"PRESENTED", "LinkClicked"},
                {"FAILED", "Bounced"}, {"DECLINED", "Unsubscribe"}};
        for (String[] s : steps) {
            MockProducer<String, String> p = newProducer();
            ActionLayer.emitDisposition(p, MEMBER_FACTS, singleActivation(), "act-A", "welcome.v1", "trk-A", s[0], s[1]);
            JsonNode fact = parse(p.history().get(0).value());
            assertEquals(s[1], fact.get("value").asText(), "raw " + s[1] + " in value");
            assertEquals(s[0], fact.get("state").asText(), "canonical " + s[0] + " in state");
        }
    }

    @Test
    void emitDispositionToleratesMissingActivationFields() {
        MockProducer<String, String> p = newProducer();
        JsonNode a = parse("{\"op\":\"DISPATCH\",\"memberId\":\"9\"}");
        ActionLayer.emitDisposition(p, MEMBER_FACTS, a, "act-Z", "", "", "FAILED", "Bounced");

        assertEquals(1, p.history().size());
        ProducerRecord<String, String> rec = p.history().get(0);
        assertEquals(":9", rec.key(), "missing entityType -> blank, key is :memberId");
        JsonNode fact = parse(rec.value());
        assertEquals("nba.disposition.act-Z.", fact.get("key").asText(), "missing channel -> trailing dot");
        assertEquals("Bounced", fact.get("value").asText());
        assertEquals("FAILED", fact.get("state").asText());
    }

    // ------------------------------------------------------------------------------------------------
    // disposition — batch vs single fan-out
    // ------------------------------------------------------------------------------------------------

    @Test
    void dispositionFansOutOneFactPerActionInABatch() {
        MockProducer<String, String> p = newProducer();
        JsonNode batch = batchActivation();

        ActionLayer.disposition(p, MEMBER_FACTS, batch, "PRESENTED", "Delivered");

        assertEquals(3, p.history().size(), "a batch emits ONE disposition per action");

        // every fact shares the member's record key (partition affinity) ...
        for (ProducerRecord<String, String> rec : p.history())
            assertEquals("Contact:7", rec.key(), "all batch facts share the member record key");

        // ... but each carries its own actionId/contentKey/trackingId in the fact body.
        assertEquals("nba.disposition.act-1.sms", parse(p.history().get(0).value()).get("key").asText());
        assertEquals("ck-1", parse(p.history().get(0).value()).get("contentKey").asText());
        assertEquals("trk-1", parse(p.history().get(0).value()).get("trackingId").asText());

        assertEquals("nba.disposition.act-2.sms", parse(p.history().get(1).value()).get("key").asText());
        assertEquals("ck-2", parse(p.history().get(1).value()).get("contentKey").asText());
        assertEquals("trk-2", parse(p.history().get(1).value()).get("trackingId").asText());

        assertEquals("nba.disposition.act-3.sms", parse(p.history().get(2).value()).get("key").asText());
        assertEquals("ck-3", parse(p.history().get(2).value()).get("contentKey").asText());
        assertEquals("trk-3", parse(p.history().get(2).value()).get("trackingId").asText());

        // state applied uniformly across the fan-out (raw=Delivered -> canonical PRESENTED)
        for (ProducerRecord<String, String> rec : p.history()) {
            assertEquals("Delivered", parse(rec.value()).get("value").asText());
            assertEquals("PRESENTED", parse(rec.value()).get("state").asText());
        }
    }

    @Test
    void dispositionEmitsExactlyOneForASingleActivation() {
        MockProducer<String, String> p = newProducer();

        ActionLayer.disposition(p, MEMBER_FACTS, singleActivation(), "SUPPRESSED", "Cancelled");

        assertEquals(1, p.history().size(), "a single (non-batch) activation emits exactly one disposition");
        JsonNode fact = parse(p.history().get(0).value());
        assertEquals("nba.disposition.act-A.email", fact.get("key").asText());
        assertEquals("Cancelled", fact.get("value").asText());
        assertEquals("SUPPRESSED", fact.get("state").asText());
        assertEquals("trk-A", fact.get("trackingId").asText());
    }

    @Test
    void dispositionTreatsEmptyActionsArrayAsSingle() {
        // actions present but EMPTY -> production guard (size() > 0) falls through to the single path,
        // emitting one fact from the top-level actionId/contentKey/trackingId.
        MockProducer<String, String> p = newProducer();
        JsonNode a = parse("{"
                + "\"op\":\"DISPATCH\",\"entityType\":\"Lead\",\"memberId\":\"5\",\"channel\":\"email\","
                + "\"actionId\":\"top-act\",\"contentKey\":\"top-ck\",\"trackingId\":\"top-trk\","
                + "\"actions\":[]"
                + "}");

        ActionLayer.disposition(p, MEMBER_FACTS, a, "PRESENTED", "Delivered");

        assertEquals(1, p.history().size(), "empty actions[] is NOT a batch -> single fact");
        JsonNode fact = parse(p.history().get(0).value());
        assertEquals("nba.disposition.top-act.email", fact.get("key").asText());
        assertEquals("top-trk", fact.get("trackingId").asText());
    }

    @Test
    void dispositionBatchFanOutCarriesStateAndRawPerAction() {
        MockProducer<String, String> p = newProducer();
        ActionLayer.disposition(p, MEMBER_FACTS, batchActivation(), "SUPPRESSED", "Cancelled");
        for (int i = 0; i < 3; i++)
            assertEquals("nba.disposition.act-" + (i + 1) + ".sms",
                    parse(p.history().get(i).value()).get("key").asText());
        for (ProducerRecord<String, String> rec : p.history()) {
            assertEquals("Cancelled", parse(rec.value()).get("value").asText());
            assertEquals("SUPPRESSED", parse(rec.value()).get("state").asText());
        }
    }

    // ------------------------------------------------------------------------------------------------
    // dlqEnvelope — replayable poison-record envelope
    // ------------------------------------------------------------------------------------------------

    @Test
    void dlqEnvelopeCapturesSourceCoordinatesAndIsReplayable() throws Exception {
        ConsumerRecord<String, String> r =
                new ConsumerRecord<>("nba.activations", 2, 99L, "Lead:42", "{\"op\":\"DISPATCH\",\"bad\":true}");
        r.headers().add("kind", "activation".getBytes(StandardCharsets.UTF_8));

        ProducerRecord<String, String> pr =
                ActionLayer.dlqEnvelope("nba.dlq.action-layer", "action-layer", r, "boom");

        assertEquals("nba.dlq.action-layer", pr.topic());
        assertEquals("Lead:42", pr.key(), "DLQ keeps the original key for partition affinity on replay");

        JsonNode env = parse(pr.value());
        assertEquals("action-layer", env.get("consumer").asText());
        assertEquals("nba.activations", env.get("topic").asText(), "SOURCE topic recorded for replay");
        assertEquals(2, env.get("partition").asInt());
        assertEquals(99, env.get("offset").asLong());
        assertEquals("Lead:42", env.get("key").asText());
        assertEquals("{\"op\":\"DISPATCH\",\"bad\":true}", env.get("value").asText(), "raw value preserved verbatim");
        assertEquals("activation", env.get("headers").get("kind").asText(), "headers carried for faithful replay");
        assertEquals("boom", env.get("error").asText());
        assertTrue(env.get("dlqTs").asLong() > 0, "dlqTs stamped");
    }

    @Test
    void dlqEnvelopeHandlesNullKey() throws Exception {
        ConsumerRecord<String, String> r = new ConsumerRecord<>("nba.activations", 0, 1L, null, "POISON-not-json");
        ProducerRecord<String, String> pr = ActionLayer.dlqEnvelope("nba.dlq.action-layer", "action-layer", r, "parse failed");
        assertNull(pr.key(), "a null-keyed source record yields a null-keyed DLQ record");
        JsonNode env = parse(pr.value());
        assertTrue(env.get("key").isNull());
        assertEquals("POISON-not-json", env.get("value").asText(), "unparseable payload preserved for inspection");
        assertEquals("parse failed", env.get("error").asText());
    }

    // ------------------------------------------------------------------------------------------------
    // channel-specific disposition vocabulary + realistic funnel drop-off (the disposition rewrite)
    // ------------------------------------------------------------------------------------------------

    @Test
    void channelFunnelIsChannelSpecificAndMatchesTheRulesEngine() {
        // email walks Delivered -> Opened -> LinkClicked; sms has LinkClicked (no Opened); voice answers/completes.
        String[][] email = ActionLayer.CHANNEL_FUNNEL.get("email");
        assertEquals("Delivered", email[0][0]);
        assertEquals("Opened", email[1][0]);
        assertEquals("LinkClicked", email[2][0]);
        assertEquals("LinkClicked", ActionLayer.CHANNEL_FUNNEL.get("sms")[1][0], "sms second stage is the click, not open");
        assertEquals("Answered", ActionLayer.CHANNEL_FUNNEL.get("voice")[0][0]);
        // every funnel stage maps to the canonical PRESENTED delivery state (soft-completion decided off the raw)
        for (String[] step : email) assertEquals("PRESENTED", step[1]);
    }

    @Test
    void declineAndFailRawAreChannelSpecific() {
        // the opt-out / bounce a member experiences is named per channel (drives DECLINED / FAILED states)
        assertEquals("Unsubscribe", ActionLayer.DECLINE_RAW.get("email"));
        assertEquals("STOP", ActionLayer.DECLINE_RAW.get("sms"));
        assertEquals("Declined", ActionLayer.DECLINE_RAW.get("voice"));
        assertEquals("Dismissed", ActionLayer.DECLINE_RAW.get("push"));
        assertEquals("Bounced", ActionLayer.FAIL_RAW.get("email"));
        assertEquals("NoAnswer", ActionLayer.FAIL_RAW.get("voice"));
    }

    @Test
    void stageProbIsRealisticDropOffNotAHappyPath() {
        // The whole point of the realism fix: most touches do NOT reach the soft bar. Every engagement stage has
        // P < 1 (the deterministic happy-path had every stage = 1.0 -> 100% click, which is unrealistic).
        double[] email = ActionLayer.STAGE_PROB.get("email");
        assertTrue(email[0] >= 0.9, "delivery is high (~.97)");
        assertTrue(email[1] < 0.5, "open rate is realistic (<50%), got " + email[1]);
        assertTrue(email[2] < 0.5, "click-of-open is realistic (<50%), got " + email[2]);
        assertTrue(ActionLayer.STAGE_PROB.get("voice")[0] < 0.5, "voice answer rate is realistic (<50%)");
        assertTrue(ActionLayer.STAGE_PROB.get("sms")[1] < 0.5, "sms click rate is realistic (<50%)");
    }

    @Test
    void emitDispositionCarriesChannelSpecificOptOutRaws() {
        // the opt-out a member triggers is the channel's DECLINE_RAW, riding as the disposition value (DECLINED state)
        for (String[] s : new String[][]{{"email", "Unsubscribe"}, {"sms", "STOP"}, {"voice", "Declined"}}) {
            MockProducer<String, String> p = newProducer();
            JsonNode a = parse("{\"op\":\"DISPATCH\",\"entityType\":\"Lead\",\"memberId\":\"1\",\"channel\":\"" + s[0]
                    + "\",\"actionId\":\"act\",\"contentKey\":\"c\",\"trackingId\":\"t\",\"correlationId\":\"x\"}");
            ActionLayer.emitDisposition(p, MEMBER_FACTS, a, "act", "c", "t", "DECLINED", s[1]);
            JsonNode fact = parse(p.history().get(0).value());
            assertEquals(s[1], fact.get("value").asText(), s[0] + " opt-out raw");
            assertEquals("DECLINED", fact.get("state").asText());
        }
    }
}
