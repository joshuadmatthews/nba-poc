package ai.das.nba.engine;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Eligibility materializer — a thin port that does NOT touch Drools. The rules-engine already computes the
 * eval and emits it to nba.evaluations (keyed by nbaId); this stage simply MATERIALIZES that topic into a
 * keyed state store (latest eval per member, a KTable in all but name) so it can be served via IQ
 * (GET /eligibility/{nbaId}) — which is what lets the action-router stop writing nba:eligibility to Redis.
 *
 * Terminal (no downstream) — it only populates the store. We strip newCompleted/newMilestones so the served
 * object byte-matches what the router persisted to nba:eligibility (those fields are the router's completion-
 * bridge inputs, not part of the hot-path read). A null value tombstones the key (KTable delete semantics).
 */
public class EligibilityProcessor implements Processor<String, String, Void, Void> {
    private KeyValueStore<String, String> store;

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.store = context.getStateStore(SpineTopology.ELIGIBILITY_STORE);
    }

    @Override
    public void process(Record<String, String> rec) {
        if (rec.key() == null) return;
        if (rec.value() == null) { store.delete(rec.key()); return; }     // tombstone -> delete (KTable semantics)
        String v = rec.value();
        try {
            ObjectNode eval = (ObjectNode) SnapshotLogic.M.readTree(rec.value());
            eval.remove("newCompleted");                                  // match what the router persisted to nba:eligibility
            eval.remove("newMilestones");
            v = SnapshotLogic.M.writeValueAsString(eval);
        } catch (Exception ignore) { /* not an object / parse fail -> store the raw value */ }
        store.put(rec.key(), v);
        Metrics.counter("nba_engine_eligibility_materialized_total").increment();
    }
}
