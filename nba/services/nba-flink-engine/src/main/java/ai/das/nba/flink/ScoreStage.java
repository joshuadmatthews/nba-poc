package ai.das.nba.flink;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static ai.das.nba.flink.NbaFlinkApp.sinkTo;
import static ai.das.nba.flink.NbaFlinkApp.sourceStream;

/**
 * STAGE 3 — SCORE: the Databricks-free journey scorer (faithful port of nba-journey-scorer/scorer.py). Consumes
 * nba.evaluations and emits an nba.score.{actionId}.{channel} fact per channel-action onto nba.member.facts
 * (kind=score), which loops back through the snapshot so the router sees fresh scores. Deterministic per
 * (member,action,channel) via a stable hash so a journey is reproducible. Stateless.
 *
 * Scores (base = stable01 in [0,1)): fresh 10 + base*10 (top); in-flight/active -50 + base; soft-completed
 * -10 + base*5; declined/failed/expired -20 + base*3; hard-completed -100 + base (never re-pick).
 */
final class ScoreStage {
    private ScoreStage() {}

    static void wire(StreamExecutionEnvironment env, Conf cfg) {
        SingleOutputStreamOperator<KafkaOut> scores =
                sourceStream(env, cfg, cfg.sink(cfg.evaluations), "score-evals").flatMap(new ScoreFn()).name("score");
        sinkTo(scores, cfg, cfg.sink(cfg.memberFacts), "sink-scores");
    }

    static final class ScoreFn implements FlatMapFunction<FactRecord, KafkaOut> {
        private static final java.util.Set<String> INFLIGHT =
                java.util.Set.of("CREATED", "IN_PROCESS", "PRESENTED", "SUPPRESSING");

        @Override
        public void flatMap(FactRecord rec, Collector<KafkaOut> out) throws Exception {
            JsonNode eval;
            try { eval = SnapshotLogic.M.readTree(rec.value); } catch (Exception e) { return; }
            String nbaId = eval.path("nbaId").asText("");
            String entityType = eval.path("entityType").asText("OPERATOR");
            String entityId = eval.path("entityId").asText("");
            String corr = eval.path("correlationId").asText(null);
            JsonNode cas = eval.path("channelActions");
            if (!cas.isArray() || entityId.isEmpty()) return;
            long now = System.currentTimeMillis();
            for (JsonNode ca : cas) {
                String actionId = ca.path("actionId").asText("");
                String channel = ca.path("channel").asText("");
                if (actionId.isEmpty() || channel.isEmpty()) continue;
                double score = scoreAction(nbaId, actionId, channel, ca);
                String fact = scoreFact(entityType, entityId, nbaId, actionId, channel, score, corr, now);
                out.collect(KafkaOut.of(entityType + ":" + entityId, fact, "score"));
            }
        }

        static double scoreAction(String nbaId, String actionId, String channel, JsonNode ca) {
            double base = stable01(nbaId + "|" + actionId + "|" + channel);
            String ws = ca.path("workflowState").asText("");
            boolean active = ca.path("active").asBoolean(false);
            if (ca.path("hardCompleted").asBoolean(false)) return round(-100.0 + base);
            if (active || INFLIGHT.contains(ws)) return round(-50.0 + base);
            if (ca.path("softCompleted").asBoolean(false)) return round(-10.0 + base * 5);
            if (ws.equals("DECLINED") || ws.equals("FAILED") || ws.equals("EXPIRED")) return round(-20.0 + base * 3);
            return round(10.0 + base * 10);   // FRESH -> top
        }

        static String scoreFact(String et, String eid, String nbaId, String actionId, String channel,
                                double score, String corr, long now) {
            var o = SnapshotLogic.M.createObjectNode();
            o.put("entityType", et); o.put("entityId", eid);
            if (nbaId != null && !nbaId.isEmpty()) o.put("nbaId", nbaId);
            o.put("key", "nba.score." + actionId + "." + channel);
            o.put("value", score); o.put("valueType", "DOUBLE");
            o.put("eventTs", now); o.put("source", "journey-scorer");
            if (corr != null) o.put("correlationId", corr);
            try { return SnapshotLogic.M.writeValueAsString(o); } catch (Exception e) { return "{}"; }
        }

        static double round(double v) { return Math.round(v * 10000.0) / 10000.0; }

        static double stable01(String s) {
            try {
                byte[] d = MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.UTF_8));
                long n = ((long) (d[0] & 0xff) << 24) | ((d[1] & 0xff) << 16) | ((d[2] & 0xff) << 8) | (d[3] & 0xff);
                return (n & 0xFFFFFFFFL) / (double) 0xFFFFFFFFL;
            } catch (Exception e) { return 0.5; }
        }
    }
}
