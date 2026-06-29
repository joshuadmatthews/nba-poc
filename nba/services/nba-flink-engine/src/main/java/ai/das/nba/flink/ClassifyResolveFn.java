package ai.das.nba.flink;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import redis.clients.jedis.JedisPooled;

import java.util.Set;
import java.util.UUID;

import static ai.das.nba.flink.SnapshotLogic.*;

/**
 * Snapshot STAGE 1a — classify each inbound member fact (the per-record port of snapshot-builder.classify):
 * route throttle/suppress facts to the definitions side-output, re-emit internally-born facts to the firehose
 * side-output, and turn member-attribute facts into a {@link SnapMsg} (NBAID resolved via the shared Redis
 * idmap, same as the classic path) for the keyed LWW fold. Unparseable records go to the DLQ side-output.
 *
 * Side outputs: DEFS_TAG (nba.definitions), FIREHOSE_TAG (nba.facts), DLQ_TAG (nba.dlq).
 */
public class ClassifyResolveFn extends ProcessFunction<FactRecord, SnapMsg> {
    public static final OutputTag<KafkaOut> DEFS_TAG = new OutputTag<>("defs") {};
    public static final OutputTag<KafkaOut> FIREHOSE_TAG = new OutputTag<>("firehose") {};
    public static final OutputTag<KafkaOut> DLQ_TAG = new OutputTag<>("dlq") {};

    private final String redisHost;
    private final int redisPort;
    private transient JedisPooled redis;
    private transient NbaIdResolver resolver;

    /** Lean-filter set; empty => snapshot all (fail-open). Hydrated from nba:rulefacts in open() + refreshed lazily. */
    private transient volatile Set<String> ruleFacts;
    private transient long ruleFactsRefreshedAt;

    public ClassifyResolveFn(String redisHost, int redisPort) { this.redisHost = redisHost; this.redisPort = redisPort; }

    @Override
    public void open(Configuration p) {
        redis = new JedisPooled(redisHost, redisPort);
        resolver = (et, id) -> resolveNbaId(redis, et, id);
        refreshRuleFacts();
    }

    private void refreshRuleFacts() {
        try { Set<String> s = redis.smembers("nba:rulefacts"); ruleFacts = s == null ? Set.of() : s; }
        catch (Exception e) { if (ruleFacts == null) ruleFacts = Set.of(); }
        ruleFactsRefreshedAt = System.currentTimeMillis();
    }

    @Override
    public void processElement(FactRecord rec, Context ctx, Collector<SnapMsg> out) {
        if (System.currentTimeMillis() - ruleFactsRefreshedAt > 10_000) refreshRuleFacts();
        JsonNode value;
        try { value = M.readTree(rec.value); }
        catch (Exception e) { ctx.output(DLQ_TAG, KafkaOut.of(rec.key, dlq(rec, "unparseable JSON"))); return; }

        Classified c = classifyOne(new Parsed(rec.key, value, rec.kind, rec.value), ruleFacts, resolver);
        for (Forward fwd : c.forwards()) {
            if (fwd.route() == Route.DEFS) ctx.output(DEFS_TAG, KafkaOut.of(fwd.key(), fwd.value()));
            else ctx.output(FIREHOSE_TAG, KafkaOut.of(fwd.key(), fwd.value(), fwd.headerKind()));
        }
        SnapFact sf = c.snap();
        if (sf != null)
            out.collect(new SnapMsg(sf.nbaId(), sf.entityType(), sf.entityId(), sf.factKey(), sf.eventTs(), fvJson(sf.fv())));
    }

    private static String dlq(FactRecord rec, String err) {
        return "{\"consumer\":\"nba-flink-engine\",\"key\":" + jsonStr(rec.key) + ",\"value\":" + jsonStr(rec.value)
                + ",\"error\":" + jsonStr(err) + "}";
    }

    private static String jsonStr(String s) {
        if (s == null) return "null";
        try { return M.writeValueAsString(s); } catch (Exception e) { return "\"\""; }
    }

    /** NBAID resolve — read the shared id-map; race-safe setnx mints one when absent (first writer wins). */
    static String resolveNbaId(JedisPooled redis, String entityType, String entityId) {
        String idmapKey = "nba:idmap:" + entityType + ":" + entityId;
        String existing = redis.get(idmapKey);
        if (existing != null) return existing;
        String nbaId = "nba_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        long set = redis.setnx(idmapKey, nbaId);
        return set == 1 ? nbaId : redis.get(idmapKey);
    }

    @Override
    public void close() { if (redis != null) redis.close(); }
}
