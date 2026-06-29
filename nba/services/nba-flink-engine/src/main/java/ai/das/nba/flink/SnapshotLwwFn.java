package ai.das.nba.flink;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.JedisPooled;

import java.util.HashMap;
import java.util.Map;

import static ai.das.nba.flink.SnapshotLogic.*;

/**
 * Snapshot STAGE 1b — the keyed event-time LWW fold (the per-key port of snapshot-builder.selectWinners).
 * Keyed by nbaId; each member's snapshot hash lives in Flink keyed MapState (the durable source of truth, like
 * the KStreams engine's RocksDB store). A newer fact (by eventTs) updates the field and re-emits the snapshot
 * JSON to nba.snapshots; a stale one is dropped. Also write-throughs the hash to Redis (nba:snapshot:{id}) so
 * the synchronous hot path (action-library getSnapshot) keeps reading from Redis unchanged.
 */
public class SnapshotLwwFn extends KeyedProcessFunction<String, SnapMsg, KafkaOut> {
    private final String redisHost;
    private final int redisPort;
    private final boolean redisWriteThrough;
    private transient MapState<String, String> hash;
    private transient JedisPooled redis;

    public SnapshotLwwFn(String redisHost, int redisPort, boolean redisWriteThrough) {
        this.redisHost = redisHost; this.redisPort = redisPort; this.redisWriteThrough = redisWriteThrough;
    }

    @Override
    public void open(Configuration p) {
        hash = getRuntimeContext().getMapState(new MapStateDescriptor<>("nba-snapshot", String.class, String.class));
        if (redisWriteThrough) redis = new JedisPooled(redisHost, redisPort);
    }

    @Override
    public void processElement(SnapMsg msg, Context ctx, Collector<KafkaOut> out) throws Exception {
        String field = "fact:" + msg.factKey;
        String cur = hash.get(field);
        if (cur != null) {
            long curTs = -1;
            try { curTs = M.readTree(cur).path("eventTs").asLong(-1); } catch (Exception ignore) {}
            if (msg.eventTs <= curTs) return;                 // stale -> LWW drop
        }
        hash.put(field, msg.fvJson);
        hash.put("__entityType", msg.entityType);
        hash.put("__entityId", msg.entityId);
        hash.put("__nbaId", msg.nbaId);
        hash.put("__updatedTs", String.valueOf(System.currentTimeMillis()));

        Map<String, String> all = new HashMap<>();
        for (Map.Entry<String, String> e : hash.entries()) all.put(e.getKey(), e.getValue());
        out.collect(KafkaOut.of(msg.nbaId, buildSnapshotJson(msg.nbaId, all)));      // emit nba.snapshots (on change)
        if (redis != null) redis.hset(snapKey(msg.nbaId), all);                      // best-effort hot-path mirror
    }

    @Override
    public void close() { if (redis != null) redis.close(); }
}
