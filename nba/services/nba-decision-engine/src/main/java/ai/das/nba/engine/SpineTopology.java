package ai.das.nba.engine;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SCOPE (deliberately minimal): this is the snapshot-builder ported to Kafka Streams so the member snapshot
 * lives in the app's own keyed state — which is what lets us DELETE the nba:snapshot Redis store (the snapshot
 * is served via Interactive Queries instead; see IqServer). It is a faithful port of snapshot-builder: same
 * classify, same event-time LWW, same buildSnapshotJson (reused verbatim from SnapshotLogic), still emitting
 * nba.snapshots so rules-engine is fed exactly as before.
 *
 *   SNAPSHOT (snapshot-builder)  member.facts --classify--> routes to defs/firehose + a per-member state store
 *                                folded by event-time LWW; emits nba.snapshots. NO Redis write.
 *
 * NON-GOALS: rules-engine and action-router are NOT ported here — they don't read nba:snapshot (rules-engine
 * consumes the nba.snapshots topic; action-router works off nba.evaluations + nba:eligibility), so they stay
 * exactly as they are. The dbx scorer + Lakebase are untouched. (rules-engine's own Redis use — its
 * change-detect read — is dropped separately via an in-memory cache, not by porting it into this app.)
 *
 * Built with the low-level Processor API: the snapshot classify naturally fans one input record out to
 * multiple sink topics (defs / firehose / snapshots) + a state store, which the DSL models awkwardly.
 */
final class SpineTopology {
    private static final Logger log = LoggerFactory.getLogger(SpineTopology.class);
    static final String SNAPSHOT_STORE = "nba-snapshot-store";
    static final String ELIGIBILITY_STORE = "nba-eligibility-store";

    private SpineTopology() {}

    static Topology build(DecisionEngine.Conf cfg) {
        Topology t = new Topology();

        // ── STAGE 1: SNAPSHOT (port of snapshot-builder) ──
        // member.facts -> classify (routes to defs/firehose) + LWW fold into the keyed snapshot store -> nba.snapshots.
        // The store's changelog is the durable source of truth (replaces snapshot-builder's Redis-as-truth); the
        // nba:snapshot Redis write is a best-effort mirror inside the processor (authoritative mode only).
        StoreBuilder<KeyValueStore<String, String>> snapshotStore =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(SNAPSHOT_STORE),
                        Serdes.String(), Serdes.String());

        t.addSource("src-facts", cfg.memberFacts)
                .addProcessor("snapshot", () -> new SnapshotProcessor(cfg), "src-facts")
                .addStateStore(snapshotStore, "snapshot")
                .addSink("sink-snapshots", cfg.sink(cfg.snapshots), "snapshot")   // shadow-suffixed unless authoritative
                .addSink("sink-defs", cfg.sink(cfg.definitions), "snapshot")
                .addSink("sink-facts", cfg.sink(cfg.facts), "snapshot")
                .addSink("sink-dlq", cfg.sink(cfg.dlq), "snapshot");        // poison records (unparseable JSON), replayable envelope

        // ── ELIGIBILITY stage: MATERIALIZE-ONLY (no Drools) ──
        // The rules-engine already emits nba.evaluations (keyed by nbaId); we just materialize it into a keyed store
        // (latest eval per member) so it can be served via IQ (GET /eligibility/{nbaId}). Terminal processor — it only
        // populates the store; this is what lets the action-router stop writing nba:eligibility to Redis.
        StoreBuilder<KeyValueStore<String, String>> eligibilityStore =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(ELIGIBILITY_STORE),
                        Serdes.String(), Serdes.String());
        t.addSource("src-evals", cfg.evaluations)
                .addProcessor("eligibility", EligibilityProcessor::new, "src-evals")
                .addStateStore(eligibilityStore, "eligibility");

        log.info("topology assembled: snapshot (in={} -> snapshots={}) + eligibility materialize (in={}); both served via IQ (no Redis)",
                cfg.memberFacts, cfg.sink(cfg.snapshots), cfg.evaluations);
        return t;
    }
}
