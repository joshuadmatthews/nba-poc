package ai.das.nba.temporal;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the PURE/extractable surface of the NBA Temporal worker.
 *
 * Scope: the {@link Activation} value type (workflow-id construction, batch detection), the
 * trackingId deconstruction the disposition consumer uses to route a verdict straight to a
 * workflow, Activation JSON parsing (the shape the bridge feeds the state machine), and the
 * replayable {@link NbaTemporalWorker#dlqEnvelope} helper.
 *
 * OUT OF SCOPE (needs an integration harness, not a unit test): the stateful Temporal workflows
 * ({@link ChannelActionWorkflowImpl} / {@link BatchOrchestratorWorkflowImpl}) and the three Kafka
 * loops (bridge / disposition / throttle feed), all of which talk to Temporal + Redpanda. The
 * Temporal test framework is deliberately NOT pulled in per the task constraints. The ThrottleGate
 * token-bucket math IS pure and is covered in {@link ThrottleGateTest}.
 */
class NbaTemporalWorkerTest {

    // ---- Activation.slug() / batchSlug() / isBatch() / member() -------------------------------

    @Test
    void slugIsNbaIdActionChannel() {
        Activation a = new Activation();
        a.nbaId = "nba_000000000001"; a.actionId = "act-7"; a.channel = "email";
        assertEquals("nba_000000000001:act-7:email", a.slug());
        // the bridge prefixes "nba-ca:" -> the full workflow id
        assertEquals("nba-ca:nba_000000000001:act-7:email", "nba-ca:" + a.slug());
    }

    @Test
    void batchSlugIsNbaIdChannelOnly() {
        Activation a = new Activation();
        a.nbaId = "nba_000000000001"; a.channel = "sms"; a.actionId = "ignored-for-batch";
        // batch id is per (member, channel) — the actionId is intentionally absent
        assertEquals("nba_000000000001:sms", a.batchSlug());
        assertEquals("nba-batch:nba_000000000001:sms", "nba-batch:" + a.batchSlug());
    }

    @Test
    void distinctActionOrChannelYieldDistinctSingleIds() {
        Activation base = new Activation();
        base.nbaId = "nba_1"; base.actionId = "a1"; base.channel = "email";
        Activation otherAction = new Activation();
        otherAction.nbaId = "nba_1"; otherAction.actionId = "a2"; otherAction.channel = "email";
        Activation otherChannel = new Activation();
        otherChannel.nbaId = "nba_1"; otherChannel.actionId = "a1"; otherChannel.channel = "sms";

        assertNotEquals(base.slug(), otherAction.slug(), "different actionId -> different workflow id");
        assertNotEquals(base.slug(), otherChannel.slug(), "different channel -> different workflow id");
        // ...but the batch id collapses across actions on the same channel (that is the point)
        assertEquals(base.batchSlug(), otherAction.batchSlug(),
                "batch id is per (member, channel) — distinct actions share it");
        assertNotEquals(base.batchSlug(), otherChannel.batchSlug());
    }

    @Test
    void singleIdAndBatchIdHaveDifferentShape() {
        Activation a = new Activation();
        a.nbaId = "nba_1"; a.actionId = "a1"; a.channel = "email";
        // single = nbaId:action:channel (3 parts), batch = nbaId:channel (2 parts)
        assertEquals(3, a.slug().split(":").length);
        assertEquals(2, a.batchSlug().split(":").length);
    }

    @Test
    void isBatchTrueOnlyWhenActionsPresentAndNonEmpty() {
        Activation none = new Activation();
        assertFalse(none.isBatch(), "null actions -> not a batch");

        Activation empty = new Activation();
        empty.actions = new java.util.ArrayList<>();
        assertFalse(empty.isBatch(), "empty actions -> not a batch");

        Activation batch = new Activation();
        batch.actions = new java.util.ArrayList<>();
        batch.actions.add(new Activation.BatchAction());
        assertTrue(batch.isBatch(), "non-empty actions -> batch");
    }

    @Test
    void memberPrefersMemberIdFallsBackToEntityId() {
        Activation a = new Activation();
        a.entityId = "lead-99";
        assertEquals("lead-99", a.member(), "no memberId -> entityId");
        a.memberId = "";
        assertEquals("lead-99", a.member(), "blank memberId -> entityId");
        a.memberId = "mbr-1";
        assertEquals("mbr-1", a.member(), "memberId wins when set");
    }

    // ---- trackingId deconstruction (workflowId|correlationId, split on LAST '|') --------------

    @Test
    void splitTrackingIdNormalCase() {
        String[] wc = NbaTemporalWorker.splitTrackingId("nba-ca:nba_1:a1:email|corr-123");
        assertNotNull(wc);
        assertEquals("nba-ca:nba_1:a1:email", wc[0]);
        assertEquals("corr-123", wc[1]);
    }

    @Test
    void splitTrackingIdMissingSeparatorReturnsNull() {
        // no '|' -> the consumer `continue`s (no route)
        assertNull(NbaTemporalWorker.splitTrackingId("nba-ca:nba_1:a1:email"));
        assertNull(NbaTemporalWorker.splitTrackingId(""));
        assertNull(NbaTemporalWorker.splitTrackingId(null));
    }

    @Test
    void splitTrackingIdLeadingBarIsRejected() {
        // bar <= 0 -> rejected: a leading '|' would yield an empty workflowId, which can't be routed
        assertNull(NbaTemporalWorker.splitTrackingId("|corr-1"));
    }

    @Test
    void splitTrackingIdEmptyCorrelationIsAllowed() {
        // trailing '|' -> empty correlationId is permitted; the workflow ignores it
        String[] wc = NbaTemporalWorker.splitTrackingId("wf-1|");
        assertNotNull(wc);
        assertEquals("wf-1", wc[0]);
        assertEquals("", wc[1]);
    }

    @Test
    void splitTrackingIdUsesLastBarSemantics() {
        // lastIndexOf('|'): only the FINAL '|' splits, so a workflowId may itself contain bars
        String[] wc = NbaTemporalWorker.splitTrackingId("wf|with|bars|corr");
        assertNotNull(wc);
        assertEquals("wf|with|bars", wc[0]);
        assertEquals("corr", wc[1]);
    }

    // ---- Activation JSON parsing (M.readValue into Activation) --------------------------------

    @Test
    void parsesSingleActionCreate() throws Exception {
        String raw = "{\"nbaId\":\"nba_1\",\"op\":\"CREATE\",\"actionId\":\"a1\",\"channel\":\"email\","
                + "\"memberId\":\"mbr-9\",\"entityType\":\"Lead\",\"entityId\":\"9\",\"score\":0.75,"
                + "\"ttlSeconds\":3600,\"trackingId\":\"wf-1|corr-1\",\"unknownField\":\"ignored\"}";
        Activation a = NbaTemporalWorker.M.readValue(raw, Activation.class);
        assertEquals("nba_1", a.nbaId);
        assertEquals("CREATE", a.op);
        assertEquals("a1", a.actionId);
        assertEquals("email", a.channel);
        assertEquals("mbr-9", a.member());
        assertEquals(0.75, a.score, 1e-9);
        assertEquals(3600, a.ttlSeconds);
        assertFalse(a.isBatch(), "no actions[] -> single, not a batch");
    }

    @Test
    void parsesBatchCreateWithActions() throws Exception {
        String raw = "{\"nbaId\":\"nba_1\",\"op\":\"CREATE\",\"channel\":\"sms\",\"actions\":["
                + "{\"actionId\":\"a1\",\"contentKey\":\"c1\",\"score\":0.9},"
                + "{\"actionId\":\"a2\",\"contentKey\":\"c2\",\"score\":0.8}]}";
        Activation a = NbaTemporalWorker.M.readValue(raw, Activation.class);
        assertTrue(a.isBatch(), "actions[] present -> batch");
        assertEquals(2, a.actions.size());
        assertEquals("a1", a.actions.get(0).actionId);
        assertEquals(0.8, a.actions.get(1).score, 1e-9);
        // a batch leaves the single-action actionId unset (the bridge takes the batch path)
        assertNull(a.actionId);
        assertEquals("nba_1:sms", a.batchSlug());
    }

    @Test
    void parsesNullNbaIdOrChannelTheBridgeSkips() throws Exception {
        // the bridge does `if (act.nbaId == null || act.channel == null) continue;` — assert the parse
        // leaves those null so the guard fires.
        Activation noNba = NbaTemporalWorker.M.readValue(
                "{\"op\":\"CREATE\",\"channel\":\"email\",\"actionId\":\"a1\"}", Activation.class);
        assertNull(noNba.nbaId);
        assertNotNull(noNba.channel);

        Activation noChannel = NbaTemporalWorker.M.readValue(
                "{\"op\":\"CREATE\",\"nbaId\":\"nba_1\",\"actionId\":\"a1\"}", Activation.class);
        assertNull(noChannel.channel);
        assertNotNull(noChannel.nbaId);
    }

    @Test
    void parseIgnoresUnknownPropertiesAndDefaultsPrimitives() throws Exception {
        Activation a = NbaTemporalWorker.M.readValue("{\"totallyUnknown\":42}", Activation.class);
        assertNotNull(a);
        assertEquals(0L, a.ttlSeconds, "unset long primitive defaults to 0");
        assertEquals(0.0, a.score, 1e-9, "unset double primitive defaults to 0");
        assertFalse(a.preDispatched, "unset boolean defaults to false");
    }

    // ---- dlqEnvelope(...) (replayable poison envelope) ----------------------------------------

    @Test
    void dlqEnvelopeCapturesSourceCoordinatesAndIsReplayable() throws Exception {
        ConsumerRecord<String, String> r =
                new ConsumerRecord<>("nba.member.facts", 5, 99L, "Lead:1", "{\"trackingId\":\"wf|c\"}");
        r.headers().add("kind", "disposition".getBytes(StandardCharsets.UTF_8));

        ProducerRecord<String, String> pr =
                NbaTemporalWorker.dlqEnvelope("nba.dlq.temporal-disposition", "temporal-disposition", r, "boom");
        assertEquals("nba.dlq.temporal-disposition", pr.topic());
        assertEquals("Lead:1", pr.key(), "DLQ keeps original key -> replay keeps partition affinity");

        JsonNode env = NbaTemporalWorker.M.readTree(pr.value());
        assertEquals("temporal-disposition", env.get("consumer").asText());
        assertEquals("nba.member.facts", env.get("topic").asText(), "SOURCE topic recorded for replay");
        assertEquals(5, env.get("partition").asInt());
        assertEquals(99, env.get("offset").asLong());
        assertEquals("Lead:1", env.get("key").asText());
        assertEquals("{\"trackingId\":\"wf|c\"}", env.get("value").asText(), "raw value preserved verbatim");
        assertEquals("disposition", env.get("headers").get("kind").asText(), "headers carried for faithful replay");
        assertEquals("boom", env.get("error").asText());
        assertTrue(env.get("dlqTs").asLong() > 0, "dlqTs stamped");
    }

    @Test
    void dlqEnvelopeHandlesNullKey() throws Exception {
        ConsumerRecord<String, String> r =
                new ConsumerRecord<>("nba.activations", 0, 7L, null, "POISON-not-json");
        ProducerRecord<String, String> pr =
                NbaTemporalWorker.dlqEnvelope("nba.dlq.temporal-bridge", "temporal-bridge", r, "parse failed");
        assertNull(pr.key(), "a null-keyed source record yields a null-keyed DLQ record");
        JsonNode env = NbaTemporalWorker.M.readTree(pr.value());
        assertTrue(env.get("key").isNull());
        assertEquals("POISON-not-json", env.get("value").asText(), "unparseable payload preserved for inspection");
    }

    // ---- operator suppression: the pure decisions extracted from the (otherwise integration-only) state machine ----

    @Test
    void preSendSuppressState_operatorPullIsSuppressed_routerSupersedeIsDebounced() {
        // BEFORE the send handoff: an OPERATOR pull is a real terminal SUPPRESSED (the action was deliberately pulled);
        // a router supersession (a higher-scored sibling out-raced this one, NOTHING was sent) is DEBOUNCED — terminal
        // but free to re-activate. This is the distinction operatorSuppress() vs suppress() drives via this flag.
        assertEquals("SUPPRESSED", ChannelActionWorkflowImpl.preSendSuppressState(true));
        assertEquals("DEBOUNCED", ChannelActionWorkflowImpl.preSendSuppressState(false));
    }

    // ---- suppressMatching visibility query (the server-side Batch Operation fan-out selector) ---------------------

    @Test
    void suppressQuery_actionWide_selectsAllRunningForTheActionAcrossChannels() {
        // no channel -> every RUNNING workflow for the action (all channels). The Running guard is mandatory:
        // closed workflows are already terminal and must not be signalled.
        String q = ActionActivitiesImpl.suppressQuery("act_x", null);
        assertEquals("NbaActionId='act_x' AND ExecutionStatus='Running'", q);
        assertFalse(q.contains("NbaChannel"), "action-wide suppress does not constrain the channel");
    }

    @Test
    void suppressQuery_channelScoped_addsChannelConstraint() {
        String q = ActionActivitiesImpl.suppressQuery("act_x", "email");
        assertEquals("NbaActionId='act_x' AND ExecutionStatus='Running' AND NbaChannel='email'", q);
    }

    @Test
    void suppressQuery_blankChannel_isTreatedAsActionWide() {
        // empty channel collapses to the action-wide form (no dangling "AND NbaChannel=''").
        assertEquals(ActionActivitiesImpl.suppressQuery("act_x", null),
                     ActionActivitiesImpl.suppressQuery("act_x", ""));
    }
}
