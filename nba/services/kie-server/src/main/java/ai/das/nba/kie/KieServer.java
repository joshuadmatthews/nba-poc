package ai.das.nba.kie;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kie.api.KieBase;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NBA KIE Server — a dedicated Drools (KIE) DECISION SERVICE.
 *
 * It hosts the compiled KieBase — built from the SAME nba.definitions stream the rules engine reads, and
 * hot-rebuilt whenever a definition changes — and serves stateless eligibility evaluations over HTTP. The
 * rules engine offloads its inline KieSession execution here (NBA_RULES_MODE=kie), so the expensive Drools
 * fireAllRules moves OUT of the Kafka consumer onto a server that can be scaled horizontally (run N replicas
 * behind the nba-kie-server alias) for load tests.
 *
 *   POST /evaluate {nbaId, facts:{key:value,...}} -> {nbaId, hits:["actionId::channel", ...]}
 *
 * STATELESS per request: the rules engine injects the throttle levels as facts and owns all enrichment
 * (action attach), channel-saturation filtering, milestones, dedup and emit. This server only fires the rules.
 */
public class KieServer {
    private static final Logger log = LoggerFactory.getLogger(KieServer.class);
    static final ObjectMapper M = new ObjectMapper();

    // Stored eligibility definitions (the DRL is built from these). THROTTLE/THROTTLE_HOT/ACTION_SUPPRESS/
    // MILESTONE are NOT the KIE server's concern — the rules engine handles those (throttle is injected as a fact).
    static final Map<String, JsonNode> actions = new ConcurrentHashMap<>();
    static final Map<String, JsonNode> globalRules = new ConcurrentHashMap<>();
    static final Map<String, JsonNode> channelRules = new ConcurrentHashMap<>();
    static volatile KieBase kieBase;

    public static void main(String[] args) {
        String bootstrap = env("NBA_BOOTSTRAP", "nba-redpanda:9092");
        String defsTopic = env("NBA_DEFINITIONS_TOPIC", "nba.definitions");
        int port = Integer.parseInt(env("NBA_PORT", "7010"));

        new Thread(() -> runDefsConsumer(bootstrap, defsTopic), "defs-consumer").start();

        Javalin app = Javalin.create().start("0.0.0.0", port);
        log.info("up on :" + port + " (Drools decision service) defs=" + defsTopic);
        app.get("/health", c -> c.result(kieBase == null ? "warming" : "ok"));
        app.get("/metrics", c -> c.contentType("text/plain; version=0.0.4; charset=utf-8").result(Metrics.scrape()));
        app.post("/evaluate", KieServer::evaluate);
    }

    /** Stateless eligibility eval: build a Snap from the posted facts, fire the rules, return the hit slugs. */
    static void evaluate(Context c) throws Exception {
        long t0 = System.nanoTime();
        JsonNode body = M.readTree(c.body());
        String nbaId = body.path("nbaId").asText("");
        Map<String, Object> f = new HashMap<>();
        JsonNode facts = body.get("facts");
        if (facts != null) for (Iterator<Map.Entry<String, JsonNode>> it = facts.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            f.put(e.getKey(), jsonValue(e.getValue()));
        }
        List<String> hits = new ArrayList<>();
        KieBase kb = kieBase;
        if (kb != null) {
            KieSession session = kb.newKieSession();
            try {
                session.setGlobal("results", hits);
                session.insert(new Snap(nbaId, f));
                session.fireAllRules();
            } finally {
                session.dispose();
            }
        }
        c.json(Map.of("nbaId", nbaId, "hits", hits));
        Metrics.timer("nba_kie_eval_seconds").record(System.nanoTime() - t0, java.util.concurrent.TimeUnit.NANOSECONDS);
        Metrics.counter("nba_kie_evaluations_total").increment();
    }

    static Object jsonValue(JsonNode v) {
        if (v == null || v.isNull()) return null;
        if (v.isBoolean()) return v.asBoolean();
        if (v.isIntegralNumber()) return v.asLong();
        if (v.isNumber()) return v.asDouble();
        return v.asText();
    }

    // ── definitions -> DRL -> KieBase (the exact compilation the rules engine uses) ──────────────────
    static void runDefsConsumer(String bootstrap, String defsTopic) {
        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, "kie-server-defs-" + UUID.randomUUID().toString().substring(0, 8));
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");   // compacted -> replays current defs on start
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp);
        consumer.subscribe(List.of(defsTopic));
        log.info("definitions consumer up: " + defsTopic);
        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
            boolean changed = false;
            for (ConsumerRecord<String, String> r : recs) {
                try { if (applyDef(r.key(), r.value())) changed = true; }
                catch (Exception e) { log.warn("bad def " + r.key(), e); }
            }
            if (changed) rebuild();
        }
    }

    static boolean applyDef(String key, String value) throws Exception {
        int i = key.indexOf(':');
        if (i < 0) return false;
        String type = key.substring(0, i), id = key.substring(i + 1);
        Map<String, JsonNode> store = switch (type) {
            case "ACTION" -> actions;
            case "GLOBAL_RULE" -> globalRules;
            case "CHANNEL_RULE" -> channelRules;
            default -> null;   // THROTTLE / THROTTLE_HOT / ACTION_SUPPRESS / MILESTONE handled in the rules engine
        };
        if (store == null) return false;
        if (value == null) store.remove(id); else store.put(id, M.readTree(value));
        return true;
    }

    static void rebuild() {
        String drl = buildDrl();
        try {
            KieHelper helper = new KieHelper();
            helper.addContent(drl, ResourceType.DRL);
            kieBase = helper.build();
            long ruleCount = actions.values().stream()
                    .mapToLong(a -> a.has("channels") ? a.get("channels").size() : 0).sum();
            log.info("KieBase rebuilt: actions=" + actions.size()
                    + " globalRules=" + globalRules.size() + " channelRules=" + channelRules.size()
                    + " eligibility-rules=" + ruleCount);
        } catch (Exception e) {
            log.warn("DRL compile FAILED: " + e + "\n----- DRL -----\n" + drl);
        }
    }

    static String buildDrl() {
        StringBuilder sb = new StringBuilder();
        sb.append("import ai.das.nba.kie.Snap;\n");
        sb.append("global java.util.List results;\n\n");
        String globalExpr = andRules(globalRules.values());
        for (JsonNode a : actions.values()) {
            String aid = a.path("id").asText();
            String incl = exprForTree(a.get("inclusion"));
            String excl = exprForTree(a.get("exclusion"));
            JsonNode channels = a.get("channels");
            if (channels == null || !channels.isArray()) continue;
            for (JsonNode ch : channels) {
                String chName = ch.path("channel").asText("");
                String chExpr = andRules(channelRulesFor(chName));
                List<String> parts = new ArrayList<>();
                if (!incl.isEmpty()) parts.add(incl);
                if (!globalExpr.isEmpty()) parts.add(globalExpr);
                if (!chExpr.isEmpty()) parts.add(chExpr);
                String combined = String.join(" && ", parts);
                sb.append("rule \"elig::").append(aid).append("::").append(chName).append("\"\n");
                sb.append("dialect \"mvel\"\nwhen\n");
                sb.append("  Snap(").append(combined).append(")\n");
                if (!excl.isEmpty()) sb.append("  not Snap(").append(excl).append(")\n");
                sb.append("then\n  results.add(\"").append(aid).append("::").append(chName).append("\");\nend\n\n");
            }
        }
        return sb.toString();
    }

    static String exprForTree(JsonNode tree) {
        if (tree == null || tree.isNull()) return "";
        JsonNode conds = tree.get("conditions");
        if (conds == null || !conds.isArray() || conds.size() == 0) return "";
        String joiner = "any".equals(tree.path("op").asText("all")) ? " || " : " && ";
        List<String> parts = new ArrayList<>();
        for (JsonNode c : conds) {
            String e = exprForCond(c);
            if (!e.isEmpty()) parts.add(e);
        }
        return parts.isEmpty() ? "" : "(" + String.join(joiner, parts) + ")";
    }

    static String exprForCond(JsonNode c) {
        String fact = c.path("fact").asText("");
        if (fact.isEmpty()) return "";
        String cmp = c.path("cmp").asText("eq");
        if ("exists".equals(cmp)) return "get(\"" + fact + "\") != null";
        JsonNode value = c.get("value");
        String lhs = "getOr(\"" + fact + "\", " + defaultFor(value) + ")";
        String val = renderVal(value);
        return switch (cmp) {
            case "ne" -> lhs + " != " + val;
            case "gt" -> lhs + " > " + val;
            case "gte" -> lhs + " >= " + val;
            case "lt" -> lhs + " < " + val;
            case "lte" -> lhs + " <= " + val;
            default -> lhs + " == " + val;
        };
    }

    static String defaultFor(JsonNode v) {
        if (v == null || v.isNull()) return "null";
        if (v.isBoolean()) return "false";
        if (v.isNumber()) return "0";
        return "\"\"";
    }

    static String renderVal(JsonNode v) {
        if (v == null || v.isNull()) return "null";
        if (v.isBoolean()) return String.valueOf(v.asBoolean());
        if (v.isNumber()) return v.asText();
        return "\"" + v.asText().replace("\"", "\\\"") + "\"";
    }

    static String andRules(Collection<JsonNode> rules) {
        List<String> parts = new ArrayList<>();
        for (JsonNode r : rules) {
            String e = exprForTree(r.get("logic"));
            if (!e.isEmpty()) parts.add(e);
        }
        return parts.isEmpty() ? "" : "(" + String.join(" && ", parts) + ")";
    }

    static Collection<JsonNode> channelRulesFor(String ch) {
        List<JsonNode> out = new ArrayList<>();
        for (JsonNode r : channelRules.values()) {
            if (ch.equals(r.path("channel").asText()) && !referencesRate(r)) out.add(r);   // .rate rules are gate-only
        }
        return out;
    }

    static boolean referencesRate(JsonNode rule) {
        JsonNode conds = rule.path("logic").path("conditions");
        if (conds.isArray()) for (JsonNode c : conds) if (c.path("fact").asText("").endsWith(".rate")) return true;
        return false;
    }

    static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? d : v;
    }
}
