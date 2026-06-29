package ai.das.nba.flink;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * STAGE 2 — RULES/ELIGIBILITY as a KeyedBroadcastProcessFunction. The nba.definitions stream is BROADCAST into
 * MapState (raw key->value); snapshots are keyed by nbaId and evaluated against the current definitions via
 * {@link RulesLogic} (the faithful condition-tree port of the classic Drools eval). Change-detect is Flink-native
 * "memory" mode: each member's last [fullHash, eligHash] in keyed ValueState (no Redis read). Emits nba.evaluations.
 */
public class RulesFn extends KeyedBroadcastProcessFunction<String, FactRecord, FactRecord, KafkaOut> {
    /** Broadcast: the raw definitions map (key = "TYPE:id", value = def JSON). */
    public static final MapStateDescriptor<String, String> DEFS_DESC =
            new MapStateDescriptor<>("nba-definitions", Types.STRING, Types.STRING);

    private transient ValueState<String> lastFull;
    private transient ValueState<String> lastElig;

    @Override
    public void open(Configuration p) {
        lastFull = getRuntimeContext().getState(new ValueStateDescriptor<>("lastFull", Types.STRING));
        lastElig = getRuntimeContext().getState(new ValueStateDescriptor<>("lastElig", Types.STRING));
    }

    @Override
    public void processBroadcastElement(FactRecord def, Context ctx, Collector<KafkaOut> out) throws Exception {
        var st = ctx.getBroadcastState(DEFS_DESC);
        if (def.key == null) return;
        if (def.value == null || def.value.isEmpty()) st.remove(def.key);   // tombstone
        else st.put(def.key, def.value);
    }

    @Override
    public void processElement(FactRecord snap, ReadOnlyContext ctx, Collector<KafkaOut> out) throws Exception {
        RulesLogic.Defs d = buildDefs(ctx.getBroadcastState(DEFS_DESC));
        RulesLogic.Result r = RulesLogic.evaluate(snap.value, d, lastFull.value(), lastElig.value());
        lastFull.update(r.fullHash);
        lastElig.update(r.eligHash);
        if (r.evalJson != null) {
            String nbaId = nbaIdOf(snap);
            out.collect(KafkaOut.of(nbaId, r.evalJson, r.eligibilityChanged ? "eligibility" : "score"));
        }
    }

    private static String nbaIdOf(FactRecord snap) {
        if (snap.key != null && !snap.key.isEmpty()) return snap.key;     // nba.snapshots is keyed by nbaId
        try { return SnapshotLogic.M.readTree(snap.value).path("nbaId").asText(""); } catch (Exception e) { return ""; }
    }

    /** Bucket the raw broadcast definitions into the typed {@link RulesLogic.Defs} the evaluator wants. */
    static RulesLogic.Defs buildDefs(ReadOnlyBroadcastState<String, String> st) throws Exception {
        RulesLogic.Defs d = new RulesLogic.Defs();
        long now = System.currentTimeMillis();
        for (Map.Entry<String, String> e : st.immutableEntries()) {
            String key = e.getKey(), val = e.getValue();
            int i = key.indexOf(':'); if (i < 0) continue;
            String type = key.substring(0, i), id = key.substring(i + 1);
            JsonNode v;
            try { v = SnapshotLogic.M.readTree(val); } catch (Exception ex) { continue; }
            switch (type) {
                case "ACTION" -> d.actions.put(id, v);
                case "GLOBAL_RULE" -> d.globalRules.put(id, v);
                case "CHANNEL_RULE" -> d.channelRules.put(id, v);
                case "MILESTONE" -> d.milestones.put(id, v);
                case "THROTTLE" -> { String fk = v.path("key").asText(""); if (!fk.isEmpty()) d.globalThrottle.put(fk, v.path("value").asLong(0)); }
                case "THROTTLE_HOT" -> { long until = ((now / 86_400_000L) + 1) * 86_400_000L; d.channelHotUntil.put(id, until); }   // until midnight
                case "ACTION_SUPPRESS" -> { if (v.path("value").asBoolean(false)) d.suppressed.add(id); }
                default -> { /* ignore */ }
            }
        }
        return d;
    }
}
