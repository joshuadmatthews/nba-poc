package ai.das.nba.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Drives the SNAPSHOT stage through a real Kafka Streams topology with TopologyTestDriver (no broker, no
 * Redis — the test facts carry nbaId so the id-map resolver is never hit, and shadow mode skips the Redis
 * mirror). Because the processor reuses snapshot-builder's pure functions verbatim, asserting the topology's
 * outputs here is equivalent to asserting the classic builder's classify / LWW / buildSnapshotJson behavior.
 */
class SnapshotStageTest {
    static final ObjectMapper M = new ObjectMapper();

    TopologyTestDriver driver;
    TestInputTopic<String, String> facts, evals;
    TestOutputTopic<String, String> snapshots, defs, firehose, dlq;

    static String fact(String nbaId, String key, Object value, String valueType, long eventTs) {
        return "{\"entityType\":\"OPERATOR\",\"entityId\":\"m1\",\"nbaId\":\"" + nbaId + "\",\"key\":\"" + key
                + "\",\"value\":" + (value instanceof String ? "\"" + value + "\"" : value)
                + ",\"valueType\":\"" + valueType + "\",\"eventTs\":" + eventTs + ",\"source\":\"test\"}";
    }

    void pipe(String key, String value, String kind, long ts) {
        RecordHeaders h = new RecordHeaders();
        if (kind != null) h.add("kind", kind.getBytes(StandardCharsets.UTF_8));
        facts.pipeInput(new TestRecord<>(key, value, h, Instant.ofEpochMilli(ts)));
    }

    @BeforeEach
    void setup() {
        // static lean-filter state is process-wide — reset so tests don't leak RULE_FACTS / prune queue into each other.
        SnapshotProcessor.RULE_FACTS = java.util.Set.of();
        SnapshotProcessor.PENDING_PRUNE.clear();
        // shadow mode -> outputs land on the *.shadow topics; redis host unused (facts carry nbaId, no resolver hit).
        DecisionEngine.Conf cfg = new DecisionEngine.Conf(false, "nba.member.facts", "nba.snapshots",
                "nba.evaluations", "nba.definitions", "nba.facts", "nba.dlq.decision-engine", "localhost", 6379);
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "snapshot-stage-test");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        driver = new TopologyTestDriver(SpineTopology.build(cfg), p);
        facts = driver.createInputTopic("nba.member.facts", new StringSerializer(), new StringSerializer());
        evals = driver.createInputTopic("nba.evaluations", new StringSerializer(), new StringSerializer());
        snapshots = driver.createOutputTopic("nba.snapshots.shadow", new StringDeserializer(), new StringDeserializer());
        defs = driver.createOutputTopic("nba.definitions.shadow", new StringDeserializer(), new StringDeserializer());
        firehose = driver.createOutputTopic("nba.facts.shadow", new StringDeserializer(), new StringDeserializer());
        dlq = driver.createOutputTopic("nba.dlq.decision-engine.shadow", new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() { driver.close(); }

    @Test
    void memberFact_isFoldedIntoTheSnapshotAndEmittedKeyedByNbaId() throws Exception {
        pipe("OPERATOR:m1", fact("nba_1", "operator.profile.age", 67, "LONG", 200), null, 200);
        TestRecord<String, String> rec = snapshots.readRecord();
        assertEquals("nba_1", rec.key(), "snapshot is keyed by nbaId");
        JsonNode snap = M.readTree(rec.value());
        assertEquals("nba_1", snap.get("nbaId").asText());
        assertEquals("OPERATOR", snap.get("entityType").asText());
        assertEquals(67, snap.get("facts").get("operator.profile.age").get("value").asInt());
        assertTrue(snapshots.isEmpty(), "exactly one snapshot for one fact");
    }

    @Test
    void eventTimeLww_olderEventIsDroppedAndEmitsNothing() throws Exception {
        pipe("OPERATOR:m1", fact("nba_1", "operator.profile.age", 67, "LONG", 200), null, 200);
        assertEquals(67, M.readTree(snapshots.readRecord().value()).get("facts").get("operator.profile.age").get("value").asInt());
        // an OUT-OF-ORDER older event for the same fact -> LWW drop -> no state change -> no emit
        pipe("OPERATOR:m1", fact("nba_1", "operator.profile.age", 50, "LONG", 100), null, 100);
        assertTrue(snapshots.isEmpty(), "stale (older eventTs) fact is dropped and emits no snapshot");
        // a NEWER event wins
        pipe("OPERATOR:m1", fact("nba_1", "operator.profile.age", 70, "LONG", 300), null, 300);
        assertEquals(70, M.readTree(snapshots.readRecord().value()).get("facts").get("operator.profile.age").get("value").asInt());
    }

    @Test
    void throttleFact_routesToDefinitionsNeverSnapshots() {
        pipe("OPERATOR:sys", fact("nba_x", "nba.throttle.email.daily", 5, "LONG", 100), null, 100);
        TestRecord<String, String> d = defs.readRecord();
        assertEquals("THROTTLE:email.daily", d.key(), "throttle level is broadcast on definitions, re-keyed");
        assertTrue(snapshots.isEmpty(), "a population-wide throttle level is never a member snapshot");
    }

    @Test
    void routerDecisionFact_firehosedButNotSnapshotted() {
        pipe("OPERATOR:m1", fact("nba_1", "nba.router.decision", "CREATE", "STRING", 100), "router", 100);
        assertFalse(firehose.isEmpty(), "kind=router is firehosed to nba.facts");
        assertTrue(snapshots.isEmpty(), "router decisions are nba-internal, not a member attribute");
    }

    @Test
    void interactiveQuery_stateStoreHoldsTheSnapshotTheIqEndpointServes() throws Exception {
        // The snapshot lives in the KStreams state store (NOT Redis); GET /snapshot/{nbaId} reads exactly this
        // store and runs buildSnapshotJson on it. Assert the served shape, proving the Redis read is replaceable.
        pipe("OPERATOR:m1", fact("nba_1", "operator.profile.age", 67, "LONG", 200), null, 200);
        snapshots.readRecord();   // drain the topic emit
        String hashJson = driver.<String, String>getKeyValueStore("nba-snapshot-store").get("nba_1");
        assertNotNull(hashJson, "snapshot lives in the KStreams state store, not Redis");
        java.util.Map<String, String> hash = M.readValue(hashJson, new com.fasterxml.jackson.core.type.TypeReference<java.util.HashMap<String, String>>() {});
        JsonNode served = M.readTree(SnapshotLogic.buildSnapshotJson("nba_1", hash));
        assertEquals("nba_1", served.get("nbaId").asText());
        assertEquals(67, served.get("facts").get("operator.profile.age").get("value").asInt());
    }

    @Test
    void actionStateFact_isBothFirehosedAndSnapshotted() throws Exception {
        // nba.actionstate.* always attaches to the snapshot (alwaysAttach) AND, carrying a kind header, is firehosed.
        pipe("OPERATOR:m1", fact("nba_1", "nba.actionstate.act_7.email", "PRESENTED", "STRING", 100), "state", 100);
        assertFalse(firehose.isEmpty(), "internally-born state fact is firehosed (kind header)");
        JsonNode snap = M.readTree(snapshots.readRecord().value());
        assertEquals("PRESENTED", snap.get("facts").get("nba.actionstate.act_7.email").get("value").asText());
    }

    // ── eligibility materializer (no Drools — just materializes nba.evaluations for IQ serving) ──

    @Test
    void eligibility_materializesLatestEvalStrippingNewFields() throws Exception {
        evals.pipeInput("nba_1", "{\"nbaId\":\"nba_1\",\"channelActions\":[{\"actionId\":\"a1\",\"channel\":\"email\",\"score\":0.5}],\"newCompleted\":[\"x\"]}");
        evals.pipeInput("nba_1", "{\"nbaId\":\"nba_1\",\"channelActions\":[{\"actionId\":\"a1\",\"channel\":\"email\",\"score\":0.9}],\"newMilestones\":[{\"id\":\"m\"}]}");
        String stored = driver.<String, String>getKeyValueStore("nba-eligibility-store").get("nba_1");
        assertNotNull(stored, "eval materialized into the eligibility store (this is what IQ /eligibility serves)");
        JsonNode e = M.readTree(stored);
        assertEquals(0.9, e.get("channelActions").get(0).get("score").asDouble(), 1e-9, "latest eval wins (KTable semantics)");
        assertFalse(e.has("newCompleted"), "newCompleted stripped to byte-match nba:eligibility");
        assertFalse(e.has("newMilestones"), "newMilestones stripped (router's completion-bridge fields, not the hot-path read)");
    }

    // ── parity gaps ──────────────────────────────────────────────────────────────────────────

    @Test
    void unparseableRecord_goesToTheDlqAsAReplayableEnvelope() throws Exception {
        facts.pipeInput(new TestRecord<>("OPERATOR:m1", "NOT-JSON-{{{", new RecordHeaders(), Instant.ofEpochMilli(100)));
        assertTrue(snapshots.isEmpty(), "a poison record produces no snapshot");
        TestRecord<String, String> d = dlq.readRecord();
        assertEquals("OPERATOR:m1", d.key(), "DLQ keeps the original key -> replay keeps partition affinity");
        JsonNode env = M.readTree(d.value());
        assertEquals("nba-decision-engine", env.get("consumer").asText());
        assertEquals("NOT-JSON-{{{", env.get("value").asText(), "raw poison value preserved for inspection");
        assertTrue(env.get("error").asText().contains("deserialize failed"));
    }

    @Test
    void prune_dropsDereferencedFactsFromStoredSnapshotsAndReEmits() throws Exception {
        // two facts on one member (RULE_FACTS empty -> snapshot-all), then one key is de-referenced from rulefacts.
        pipe("OPERATOR:m1", fact("nba_1", "operator.profile.age", 67, "LONG", 100), null, 100);
        pipe("OPERATOR:m1", fact("nba_1", "operator.profile.riskScore", 5, "LONG", 100), null, 100);
        snapshots.readRecord(); snapshots.readRecord();                 // drain the two emits
        SnapshotProcessor.PENDING_PRUNE.add("operator.profile.age");    // the refresh thread would queue this on a rulefacts shrink
        driver.advanceWallClockTime(Duration.ofSeconds(16));            // fire the prune punctuator (scheduled at 15s)
        JsonNode snap = M.readTree(snapshots.readRecord().value());     // the prune re-emit
        assertFalse(snap.get("facts").has("operator.profile.age"), "de-referenced fact is pruned out");
        assertTrue(snap.get("facts").has("operator.profile.riskScore"), "other facts are retained");
    }
}
