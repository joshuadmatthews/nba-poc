package ai.das.nba.actionlib;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import redis.clients.jedis.JedisPooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NBA Action Library API (minimal Java).
 *
 * Stores action + global/channel rule DEFINITIONS as JSONB documents in Postgres. The Command Center
 * authors SIMPLE STRUCTURED LOGIC (condition trees of {fact, cmp, value}); the rules engine compiles
 * that to Drools later. We auto-derive `factsUsed` from the logic.
 *
 * CONSISTENCY: this service writes NOTHING to Kafka directly. EVERY emission — definition upserts/deletes,
 * operator suppressions, and inbound dispositions — is an INSERT into the transactional outbox
 * (`outbox_defs`) inside the same Postgres transaction as any business write, and Debezium (the shared
 * nba-outbox connector) CDC-tails it to Kafka via the Outbox Event Router (aggregatetype -> topic,
 * aggregateid -> key, kind -> header). No manual poller, no dual writes.
 *
 * The Redis caches this service used to write in the relay (nba:rulefacts, nba:suppressed) are now DERIVED
 * by consuming the compacted nba.definitions topic (single source of truth), so they can never drift from
 * what the downstream consumers see.
 */
public class ActionLibrary {
    private static final Logger log = LoggerFactory.getLogger(ActionLibrary.class);
    static final ObjectMapper M = new ObjectMapper();
    static HikariDataSource ds;
    static String DEFS_TOPIC;       // aggregatetype for defs + suppressions (Debezium routes it to this topic)
    static String MEMBER_FACTS;     // aggregatetype for inbound dispositions
    static String ACTIVATIONS;      // inbound TRACKING topic (serve/disposition events) — direct Kafka, no outbox (fire-and-forget)
    static Producer<String, String> PRODUCER;
    static JedisPooled redis;       // shared client (defs cache, suppressions, the nba:facttype catalog gate)

    // Operator-suppressed actions / action-channels, mirrored IN MEMORY so the inbound serve strips them
    // with ZERO I/O. DERIVED from the compacted nba.definitions topic (ACTION_SUPPRESS:{target}).
    static final Set<String> SUPPRESSED = ConcurrentHashMap.newKeySet();

    // ── FAST PATH (synchronous decision): GET /snapshot + POST /disposition short-circuit the async medallion loop —
    //    read the member's snapshot, merge the inbound facts, run eligibility (nba-kie-server OR an in-process floor),
    //    score locally via the nba-model endpoint, and respond with the NBAs for the channel + per-stage timings.
    // HTTP/1.1 forced: the JDK client defaults to HTTP/2 and attempts an h2c upgrade that uvicorn (the nba-model
    // endpoint) rejects ("Unsupported upgrade request" -> 422). 1.1 is fine for both nba-kie-server and nba-model.
    static final java.net.http.HttpClient HTTP = java.net.http.HttpClient.newBuilder()
            .version(java.net.http.HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(2)).build();
    static final String KIE_URL = env("NBA_KIE_URL", "http://nba-kie-server:7010");
    static final String MODEL_URL = env("NBA_MODEL_URL", "http://nba-model:7011");
    static volatile Map<String, JsonNode> ACTION_DOCS = java.util.Collections.emptyMap();   // actionId -> action doc, cached from Postgres
    static volatile long CAT_AT = 0;
    static final java.util.concurrent.ExecutorService WB_POOL = java.util.concurrent.Executors.newFixedThreadPool(2);
    // FEATURE STORE: the rich model features live in gold (Databricks). The hot path reads them STRAIGHT from gold via
    // the SQL warehouse (goldFeatures) — NO Redis cache. (Lakebase would be the fast online store, but its CONTINUOUS
    // synced table is blocked by this account's rootless metastore — see lakebaseFeatures, dormant until that's fixed.)
    static final String DBX_HOST = env("NBA_DBX_HOST", "");
    static final String DBX_CLIENT_ID = env("NBA_DBX_CLIENT_ID", "");
    static final String DBX_CLIENT_SECRET = env("NBA_DBX_CLIENT_SECRET", "");
    static final String DBX_WAREHOUSE = env("NBA_DBX_WAREHOUSE", "");
    static final String LAKE_NS = env("NBA_LAKE_NS", "workspace.nba_poc");
    static volatile String DBX_TOKEN = null;
    static volatile long DBX_TOKEN_EXP = 0;
    // LAKEBASE (Databricks serverless Postgres) = the ONLINE FEATURE STORE proper. Read the member's gold features
    // STRAIGHT from Postgres on the disposition — no Redis warm-on-serve. When the caller is CO-LOCATED with Lakebase
    // (production) this is a ~ms intra-cloud lookup; in this LOCAL/home POC it's a cloud round-trip, so the legacy Redis
    // warm-cache is still faster locally. Pool is rebuilt hourly with a fresh OAuth-minted DB credential.
    static final String LAKEBASE_HOST = env("NBA_LAKEBASE_HOST", "");
    static final String LAKEBASE_USER = env("NBA_LAKEBASE_USER", "");
    static final String LAKEBASE_INSTANCE = env("NBA_LAKEBASE_INSTANCE", "nba-lakebase");
    static volatile String LB_TOKEN = null; static volatile long LB_TOKEN_EXP = 0;
    static volatile com.zaxxer.hikari.HikariDataSource LB_DS = null; static volatile long LB_DS_EXP = 0;
    // scorer=dbx -> call a Databricks MODEL SERVING endpoint (ml_worspace.core.nba_cql) instead of the local nba-model.
    static final String SERVING_URL = env("NBA_SERVING_URL", "");          // {ml_host}/serving-endpoints/nba-cql/invocations
    static final String ML_HOST = env("NBA_ML_HOST", "");
    static final String ML_CLIENT_ID = env("NBA_ML_CLIENT_ID", "");
    static final String ML_CLIENT_SECRET = env("NBA_ML_CLIENT_SECRET", "");
    // Hot-path scorer: "dbx" = the Databricks nba-cql serving endpoint (always serves @champion -> auto-current, no local
    // model to sync) | "local" = the in-process nba-model (lowest latency, but must be synced). Per-request `scorer` overrides.
    static final String DEFAULT_SCORER = env("NBA_SCORER_DEFAULT", "dbx");
    // SNAPSHOT SOURCE: "redis" = read nba:snapshot from Redis (classic snapshot-builder writes it) | "kstreams" =
    // read it from the nba-decision-engine's Interactive-Query endpoint (the snapshot lives in KStreams state, no
    // Redis). Default redis so flipping the flag is the only behavior change. Mirrors the scorer/mode toggles.
    static final String SNAPSHOT_SOURCE = env("NBA_SNAPSHOT_SOURCE", "redis");
    static final String ENGINE_IQ = env("NBA_ENGINE_IQ_URL", "http://nba-decision-engine:7020");
    // ELIGIBILITY SOURCE: "redis" = read nba:eligibility from Redis (action-router writes it) | "kstreams" = read
    // it from the decision-engine's IQ (GET /eligibility, materialized from nba.evaluations). Default redis.
    static final String ELIG_SOURCE = env("NBA_ELIG_SOURCE", "redis");
    static volatile String ML_TOKEN = null;
    static volatile long ML_TOKEN_EXP = 0;

    public static void main(String[] args) throws Exception {
        String dbUrl = env("NBA_DB_URL", "jdbc:postgresql://nba-postgres:5432/actionlib");
        String dbUser = env("NBA_DB_USER", "nba");
        String dbPass = env("NBA_DB_PASS", "nba");
        int port = Integer.parseInt(env("NBA_PORT", "7001"));

        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(dbUrl);
        hc.setUsername(dbUser);
        hc.setPassword(dbPass);
        hc.setMaximumPoolSize(5);
        ds = connectWithRetry(hc);
        initSchema();

        String bootstrap = env("NBA_BOOTSTRAP", "nba-redpanda:9092");
        DEFS_TOPIC = env("NBA_DEFINITIONS_TOPIC", "nba.definitions");
        MEMBER_FACTS = env("NBA_MEMBER_FACTS", "nba.member.facts");
        ACTIVATIONS = env("NBA_ACTIVATIONS", "nba.activations");
        java.util.Properties pp = new java.util.Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pp.put(ProducerConfig.ACKS_CONFIG, "1");
        PRODUCER = new KafkaProducer<>(pp);
        redis = new JedisPooled(env("NBA_REDIS_HOST", "nba-redis"), 6379);

        // Derive nba:suppressed + the in-memory SUPPRESSED set + nba:rulefacts from the definitions topic.
        startDefinitionsCache(bootstrap, DEFS_TOPIC, redis);

        Javalin app = Javalin.create().start("0.0.0.0", port);
        log.info("up on :" + port + " db=" + dbUrl + " outbox->Debezium (" + DEFS_TOPIC + "," + MEMBER_FACTS + ")");

        app.get("/health", c -> c.result("ok"));
        app.get("/metrics", c -> c.contentType("text/plain; version=0.0.4; charset=utf-8").result(Metrics.scrape()));

        // canonical fact-type catalog (nba:facttype): {factKey: BOOL|LONG|DOUBLE|STRING}. The rule-builder UI
        // constrains operator options by it; upsert() validates every rule operator against it.
        app.get("/facts", c -> c.json(M.valueToTree(redis.hgetAll("nba:facttype"))));

        // actions (id, name, ttlSeconds, channels:[{channel,contentKey}], inclusion, exclusion)
        app.post("/actions", c -> upsert(c, "action", "ACTION"));
        app.put("/actions/{id}", c -> upsert(c, "action", "ACTION"));
        app.get("/actions", c -> list(c, "action"));
        app.get("/actions/{id}", c -> get(c, "action"));
        app.delete("/actions/{id}", c -> delete(c, "action", "ACTION"));

        // global rules (id, name, logic)
        app.post("/global-rules", c -> upsert(c, "global_rule", "GLOBAL_RULE"));
        app.put("/global-rules/{id}", c -> upsert(c, "global_rule", "GLOBAL_RULE"));
        app.get("/global-rules", c -> list(c, "global_rule"));
        app.delete("/global-rules/{id}", c -> delete(c, "global_rule", "GLOBAL_RULE"));

        // channel rules (id, channel, name, logic)
        app.post("/channel-rules", c -> upsert(c, "channel_rule", "CHANNEL_RULE"));
        app.put("/channel-rules/{id}", c -> upsert(c, "channel_rule", "CHANNEL_RULE"));
        app.get("/channel-rules", c -> list(c, "channel_rule"));
        app.delete("/channel-rules/{id}", c -> delete(c, "channel_rule", "CHANNEL_RULE"));

        // ---- INBOUND (pull) — serve the next best action(s) from the eval CACHE (Redis).
        app.get("/next-action/{entity}", c -> { Metrics.counter("nba_serve_requests_total", "route", "next-action").increment(); nextAction(c, redis); });
        app.post("/dispositions", c -> inboundDisposition(c, redis));
        // ---- FAST PATH (synchronous): read snapshot -> merge inbound facts -> eligibility -> local model score -> NBAs for the channel.
        app.get("/snapshot/{nbaId}", c -> getSnapshot(c, redis));
        app.post("/disposition", c -> { Metrics.counter("nba_serve_requests_total", "route", "disposition").increment(); postDisposition(c, redis); });
        // ---- HARD COMPLETION (API / partner / lake fallback) — signal the member did the action's goal.
        app.post("/completion", c -> { Metrics.counter("nba_completions_total").increment(); recordCompletion(c, redis); });

        // ---- OPERATOR suppress (Command Center) — pull an ACTION or ACTION-CHANNEL out of rotation.
        app.post("/suppress", c -> suppressAction(c));
        app.get("/suppressed", c -> c.json(M.valueToTree(redis.smembers("nba:suppressed"))));

        // ---- channel CONFIG: max batch per channel (Command Center). Read by the action-router from Redis.
        app.post("/channel-config", c -> {
            JsonNode b = M.readTree(c.body());
            String ch = b.path("channel").asText("");
            int mb = Math.max(1, b.path("maxBatch").asInt(1));
            if (ch.isEmpty()) { c.status(400).result("channel required"); return; }
            redis.hset("nba:channel:maxbatch", ch, String.valueOf(mb));
            log.info("channel-config " + ch + " maxBatch=" + mb);
            c.json(M.createObjectNode().put("channel", ch).put("maxBatch", mb));
        });
        app.get("/channel-config", c -> c.json(M.valueToTree(redis.hgetAll("nba:channel:maxbatch"))));

        // ---- ACTION GROUPS (taxonomy tree). Assign actions to a group, browse a group's actions (the UI
        // rolls up descendants), add groups, delete EMPTY ones. Groups are command-center metadata only.
        app.get("/groups", c -> listGroups(c));                       // [{id,name,parentId}]
        app.post("/groups", c -> createGroup(c));                     // {name, parentId?} -> group
        app.delete("/groups/{id}", c -> deleteGroup(c));              // 409 if it has children or actions
        app.post("/actions/{id}/group", c -> assignGroup(c));         // {groupId?} -> sets/clears doc.groupId (+ outbox)

        // ---- EXPERIENCES (business-journey taxonomy, flat) — same shape as groups but no tree.
        app.get("/experiences", c -> listExperiences(c));             // [{id,name,description}]
        app.post("/experiences", c -> createExperience(c));           // {name, description?} -> experience
        app.delete("/experiences/{id}", c -> deleteExperience(c));    // 409 if any action is assigned
        app.post("/actions/{id}/experience", c -> assignExperience(c)); // {experienceId?} -> sets/clears doc.experienceId (+ outbox)

        // ---- MILESTONES (def: name + structured logic). Reuses the def pipeline: upsert/delete write the
        // doc + an outbox row -> Debezium -> nba.definitions MILESTONE:{id}; the rules engine evaluates + latches.
        app.post("/milestones", c -> upsert(c, "milestone", "MILESTONE"));
        app.put("/milestones/{id}", c -> upsert(c, "milestone", "MILESTONE"));
        app.get("/milestones", c -> list(c, "milestone"));
        app.delete("/milestones/{id}", c -> delete(c, "milestone", "MILESTONE"));
    }

    // ---- experiences (flat taxonomy; an action's experience = doc.experienceId) ----
    static void listExperiences(Context c) throws Exception {
        ArrayNode arr = M.createArrayNode();
        try (Connection conn = ds.getConnection(); Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select id, name, description from experience order by name")) {
            while (rs.next()) {
                ObjectNode x = arr.addObject();
                x.put("id", rs.getString(1)); x.put("name", rs.getString(2));
                String d = rs.getString(3); if (d == null) x.putNull("description"); else x.put("description", d);
            }
        }
        c.json(arr);
    }

    static void createExperience(Context c) throws Exception {
        JsonNode b = M.readTree(c.body());
        String name = b.path("name").asText("").trim();
        String desc = b.hasNonNull("description") ? b.get("description").asText() : null;
        if (name.isEmpty()) { c.status(400).result("name required"); return; }
        String id = "exp_" + UUID.randomUUID().toString().substring(0, 8);
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement("insert into experience(id, name, description) values (?, ?, ?)")) {
            ps.setString(1, id); ps.setString(2, name);
            if (desc == null) ps.setNull(3, java.sql.Types.VARCHAR); else ps.setString(3, desc);
            ps.executeUpdate();
        }
        ObjectNode x = M.createObjectNode().put("id", id).put("name", name);
        if (desc == null) x.putNull("description"); else x.put("description", desc);
        log.info("experience + " + id + " '" + name + "'");
        c.json(x);
    }

    /** Delete an experience only if no action is assigned to it. 409 otherwise. */
    static void deleteExperience(Context c) throws Exception {
        String id = c.pathParam("id");
        try (Connection conn = ds.getConnection()) {
            long actions = scalar(conn, "select count(*) from action where doc->>'experienceId' = ?", id);
            if (actions > 0) { c.status(409).json(M.createObjectNode().put("error", "experience not empty").put("actions", actions)); return; }
            try (PreparedStatement ps = conn.prepareStatement("delete from experience where id = ?")) {
                ps.setString(1, id);
                if (ps.executeUpdate() == 0) { c.status(404).result("experience not found"); return; }
            }
        }
        log.info("experience - " + id);
        c.json(M.createObjectNode().put("deleted", id));
    }

    /** Assign (or clear) an action's experience — updates doc.experienceId + re-emits the def via outbox. */
    static void assignExperience(Context c) throws Exception {
        String actionId = c.pathParam("id");
        JsonNode b = M.readTree(c.body());
        String experienceId = b.hasNonNull("experienceId") ? b.get("experienceId").asText() : null;
        if (experienceId != null && experienceId.isBlank()) experienceId = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            ObjectNode doc;
            try (PreparedStatement ps = conn.prepareStatement("select doc from action where id = ?")) {
                ps.setString(1, actionId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) { c.status(404).result("action not found: " + actionId); return; }
                    doc = (ObjectNode) M.readTree(rs.getString(1));
                }
            }
            if (experienceId != null) {
                try (PreparedStatement ck = conn.prepareStatement("select 1 from experience where id = ?")) {
                    ck.setString(1, experienceId);
                    try (ResultSet rs = ck.executeQuery()) { if (!rs.next()) { c.status(400).result("experience not found: " + experienceId); return; } }
                }
            }
            if (experienceId == null) doc.remove("experienceId"); else doc.put("experienceId", experienceId);
            try (PreparedStatement up = conn.prepareStatement("update action set doc = ?::jsonb, updated_at = now() where id = ?")) {
                up.setString(1, M.writeValueAsString(doc)); up.setString(2, actionId); up.executeUpdate();
            }
            outbox(conn, DEFS_TOPIC, "ACTION:" + actionId, null, M.writeValueAsString(doc));
            conn.commit();
        }
        log.info("action " + actionId + " -> experience " + (experienceId == null ? "(none)" : experienceId));
        c.json(M.createObjectNode().put("id", actionId).put("experienceId", experienceId == null ? "" : experienceId));
    }

    static void listGroups(Context c) throws Exception {
        ArrayNode arr = M.createArrayNode();
        try (Connection conn = ds.getConnection(); Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select id, name, parent_id from action_group order by name")) {
            while (rs.next()) {
                ObjectNode g = arr.addObject();
                g.put("id", rs.getString(1)); g.put("name", rs.getString(2));
                String p = rs.getString(3); if (p == null) g.putNull("parentId"); else g.put("parentId", p);
            }
        }
        c.json(arr);
    }

    static void createGroup(Context c) throws Exception {
        JsonNode b = M.readTree(c.body());
        String name = b.path("name").asText("").trim();
        String parentId = b.hasNonNull("parentId") ? b.get("parentId").asText() : null;
        if (parentId != null && parentId.isBlank()) parentId = null;
        if (name.isEmpty()) { c.status(400).result("name required"); return; }
        String id = "grp_" + UUID.randomUUID().toString().substring(0, 8);
        try (Connection conn = ds.getConnection()) {
            if (parentId != null && !groupExists(conn, parentId)) { c.status(400).result("parent group not found: " + parentId); return; }
            try (PreparedStatement ps = conn.prepareStatement("insert into action_group(id, name, parent_id) values (?, ?, ?)")) {
                ps.setString(1, id); ps.setString(2, name);
                if (parentId == null) ps.setNull(3, java.sql.Types.VARCHAR); else ps.setString(3, parentId);
                ps.executeUpdate();
            }
        }
        ObjectNode g = M.createObjectNode();
        g.put("id", id); g.put("name", name);
        if (parentId == null) g.putNull("parentId"); else g.put("parentId", parentId);
        log.info("group + " + id + " '" + name + "'" + (parentId != null ? " under " + parentId : ""));
        c.json(g);
    }

    /** Delete a group ONLY if empty — no child groups AND no actions assigned to it. 409 otherwise. */
    static void deleteGroup(Context c) throws Exception {
        String id = c.pathParam("id");
        try (Connection conn = ds.getConnection()) {
            long children = scalar(conn, "select count(*) from action_group where parent_id = ?", id);
            long actions = scalar(conn, "select count(*) from action where doc->>'groupId' = ?", id);
            if (children > 0 || actions > 0) {
                c.status(409).json(M.createObjectNode().put("error", "group not empty")
                        .put("childGroups", children).put("actions", actions));
                return;
            }
            try (PreparedStatement ps = conn.prepareStatement("delete from action_group where id = ?")) {
                ps.setString(1, id);
                if (ps.executeUpdate() == 0) { c.status(404).result("group not found"); return; }
            }
        }
        log.info("group - " + id);
        c.json(M.createObjectNode().put("deleted", id));
    }

    /** Assign (or clear, groupId=null) an action's group. Updates the action doc + re-emits the def via the
     *  SAME outbox transaction, so the def stays consistent (groupId rides the doc; the rules engine ignores it). */
    static void assignGroup(Context c) throws Exception {
        String actionId = c.pathParam("id");
        JsonNode b = M.readTree(c.body());
        String groupId = b.hasNonNull("groupId") ? b.get("groupId").asText() : null;
        if (groupId != null && groupId.isBlank()) groupId = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            ObjectNode doc;
            try (PreparedStatement ps = conn.prepareStatement("select doc from action where id = ?")) {
                ps.setString(1, actionId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) { c.status(404).result("action not found: " + actionId); return; }
                    doc = (ObjectNode) M.readTree(rs.getString(1));
                }
            }
            if (groupId != null && !groupExists(conn, groupId)) { c.status(400).result("group not found: " + groupId); return; }
            if (groupId == null) doc.remove("groupId"); else doc.put("groupId", groupId);
            try (PreparedStatement up = conn.prepareStatement("update action set doc = ?::jsonb, updated_at = now() where id = ?")) {
                up.setString(1, M.writeValueAsString(doc)); up.setString(2, actionId); up.executeUpdate();
            }
            outbox(conn, DEFS_TOPIC, "ACTION:" + actionId, null, M.writeValueAsString(doc));   // same tx
            conn.commit();
        }
        log.info("action " + actionId + " -> group " + (groupId == null ? "(none)" : groupId));
        c.json(M.createObjectNode().put("id", actionId).put("groupId", groupId == null ? "" : groupId));
    }

    static boolean groupExists(Connection conn, String id) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("select 1 from action_group where id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }

    static long scalar(Connection conn, String sql, String arg) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, arg);
            try (ResultSet rs = ps.executeQuery()) { return rs.next() ? rs.getLong(1) : 0; }
        }
    }

    // =====================================================================================
    // Derived-cache consumer: tail the compacted nba.definitions topic (single source of truth) and
    // rebuild SUPPRESSED + nba:suppressed + nba:rulefacts. Fresh group + earliest -> replays current
    // state on startup; the broadcast then keeps every action-library instance converged. No writes here
    // are paired with a Kafka emit, so there's nothing to be inconsistent.
    // =====================================================================================
    static void startDefinitionsCache(String bootstrap, String defsTopic, JedisPooled redis) {
        try { SUPPRESSED.addAll(redis.smembers("nba:suppressed")); } catch (Exception ignore) {}   // warm floor
        recomputeRuleFacts(redis);   // authoritative initial load from Postgres (complete even if topic payloads lag)
        Thread t = new Thread(() -> {
            Properties cp = new Properties();
            cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            cp.put(ConsumerConfig.GROUP_ID_CONFIG, "action-library-defs-" + UUID.randomUUID().toString().substring(0, 8));
            cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");   // compacted -> replays latest per key
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp);
            consumer.subscribe(List.of(defsTopic));
            log.info("definitions cache up: " + defsTopic + " (derives suppressed; triggers rulefacts)");
            while (true) {
                ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
                boolean defChanged = false;
                for (ConsumerRecord<String, String> r : recs) {
                    String k = r.key();
                    if (k == null) continue;
                    if (k.startsWith("ACTION_SUPPRESS:")) {
                        String target = k.substring("ACTION_SUPPRESS:".length());
                        boolean on = false;
                        try { on = r.value() != null && M.readTree(r.value()).path("value").asBoolean(false); } catch (Exception ignore) {}
                        if (on) { SUPPRESSED.add(target); redis.sadd("nba:suppressed", target); }
                        else    { SUPPRESSED.remove(target); redis.srem("nba:suppressed", target); }
                    } else if (isDefKey(k)) {
                        defChanged = true;   // a def add/update/delete (incl. from another instance) -> recompute from DB
                    }
                    // THROTTLE: / THROTTLE_HOT: -> not ours (rules engine + temporal handle the throttle level)
                }
                if (defChanged) recomputeRuleFacts(redis);
            }
        }, "definitions-cache");
        t.setDaemon(true);
        t.start();
    }

    static boolean isDefKey(String k) {
        return k.startsWith("ACTION:") || k.startsWith("GLOBAL_RULE:") || k.startsWith("CHANNEL_RULE:");
    }

    /** nba:rulefacts = the union of factsUsed across every current def, queried straight from POSTGRES
     *  (the authoritative store — topic payloads can carry stale factsUsed for defs not re-upserted since
     *  that field was added). Topic def-changes only TRIGGER this recompute; the source is always the DB. */
    static void recomputeRuleFacts(JedisPooled redis) {
        String q = "select distinct f from (" +
                "select jsonb_array_elements_text(doc->'factsUsed') f from action " +
                "union all select jsonb_array_elements_text(doc->'factsUsed') f from global_rule " +
                "union all select jsonb_array_elements_text(doc->'factsUsed') f from channel_rule " +
                "union all select jsonb_array_elements_text(doc->'factsUsed') f from milestone) x";
        try (Connection conn = ds.getConnection(); Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(q)) {
            Set<String> facts = new TreeSet<>();
            while (rs.next()) { String f = rs.getString(1); if (f != null && !f.isBlank()) facts.add(f); }
            redis.del("nba:rulefacts");
            if (!facts.isEmpty()) redis.sadd("nba:rulefacts", facts.toArray(new String[0]));
            log.info("nba:rulefacts -> " + facts);
        } catch (Exception e) {
            log.warn("rulefacts recompute failed", e);
        }
    }

    // =====================================================================================
    // Operator suppress + inbound disposition: outbox-only (Debezium emits), no direct Kafka, no Redis.
    // =====================================================================================

    static void suppressAction(Context c) throws Exception {
        JsonNode b = M.readTree(c.body());
        String actionId = b.path("actionId").asText("");
        String channel = b.path("channel").asText("");          // empty -> whole action; else action-channel
        boolean suppressed = b.path("suppressed").asBoolean(true);
        if (actionId.isEmpty()) { c.status(400).result("actionId required"); return; }
        String target = channel.isEmpty() ? actionId : actionId + "." + channel;
        String key = "nba.actionsuppress." + target;
        ObjectNode fact = M.createObjectNode();
        fact.put("entityType", "SYSTEM"); fact.put("entityId", "__action");
        fact.put("key", key); fact.put("value", suppressed); fact.put("valueType", "BOOL");
        fact.put("actionId", actionId); fact.put("channel", channel);
        fact.put("eventTs", System.currentTimeMillis()); fact.put("source", "operator");
        // Outbox -> Debezium routes ACTION_SUPPRESS:{target} to the definitions topic; the rules engine + this
        // service's definitions cache pick it up. The in-memory SUPPRESSED / nba:suppressed update on that
        // round-trip (single source of truth), so there's no dual write to drift.
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            outbox(conn, DEFS_TOPIC, "ACTION_SUPPRESS:" + target, "action-suppress", M.writeValueAsString(fact));
            // On a SUPPRESS, also drop a ROUTER fact (kind=router) onto member.facts — the temporal bridge already
            // consumes router DECISIONS, so the state machine reacts to a decision, not the definitions broadcast. It
            // fans the pull out to in-flight workflows via the SuppressionWorkflow. Same txn -> no dual-write drift.
            if (suppressed) {
                ObjectNode pull = M.createObjectNode();
                pull.put("op", "SUPPRESS_ACTION"); pull.put("actionId", actionId); pull.put("channel", channel);
                pull.put("eventTs", fact.get("eventTs").asLong()); pull.put("source", "operator");
                outbox(conn, MEMBER_FACTS, "ACTION_SUPPRESS:" + target, "router", M.writeValueAsString(pull));
            }
            conn.commit();
        }
        log.info("operator " + (suppressed ? "SUPPRESS" : "UNSUPPRESS") + " " + target + " (outbox)");
        c.json(fact);
    }

    /** GET /next-action/{entity}?channel=&n=&includeCompleted= — serve a member's actions for a requesting channel/surface
     *  (e.g. website, salesforce, email). DEFAULT returns ALL eligible actions for the channel ranked by score (not just the
     *  top one); pass n to cap. includeCompleted=true also returns already-completed actions (state=completed, sunk to the
     *  bottom) so the surface can show the full portfolio. If the request BODY carries {facts:{...}, mode?, scorer?}, runs the
     *  synchronous HOT PATH (snapshot + those facts -> eligibility -> nba-model score) so the scores reflect the just-given
     *  context; otherwise it reads the loop's cached eligibility object. Each action carries state: eligible | active | completed. */
    static void nextAction(Context c, JedisPooled redis) throws Exception {
        String entity = c.pathParam("entity");
        String channel = c.queryParam("channel");
        boolean includeCompleted = "true".equalsIgnoreCase(c.queryParam("includeCompleted"));
        String nParam = c.queryParam("n");
        int n = nParam == null ? Integer.MAX_VALUE : Math.max(1, parseInt(nParam, 1));   // default: ALL actions for the channel
        boolean chanAll = channel == null || channel.isBlank();
        String nbaId = entity.startsWith("nba_") ? entity : redis.get("nba:idmap:OPERATOR:" + entity);
        Set<String> suppressed = SUPPRESSED;

        ObjectNode resp = M.createObjectNode();
        resp.put("entityId", entity); resp.put("nbaId", nbaId == null ? "" : nbaId); resp.put("channel", chanAll ? "" : channel);
        List<ObjectNode> out = new ArrayList<>();
        boolean hot = false;

        // HOT PATH: facts in the body -> fresh eligible scoring that incorporates the just-given context.
        String body = c.body();
        JsonNode bj = (body != null && !body.isBlank()) ? M.readTree(body) : null;
        JsonNode inFacts = bj == null ? null : bj.get("facts");
        if (inFacts != null && inFacts.isObject() && inFacts.size() > 0) {
            hot = true;
            ObjectNode hp = hotPathDecide(redis, entity, nbaId, channel, null, null, inFacts,
                    System.currentTimeMillis(), bj.path("mode").asText("kie"), bj.path("scorer").asText(DEFAULT_SCORER), Integer.MAX_VALUE);
            for (JsonNode nba : hp.path("nbas")) { ObjectNode o = ((ObjectNode) nba).deepCopy(); o.put("state", "eligible"); out.add(o); }
            resp.set("timings", hp.get("timings")); resp.put("featureSource", hp.path("featureSource").asText());
        }

        // CACHED eligible (when no facts) + COMPLETED (when includeCompleted) from the loop's single eligibility object.
        String evalJson = eligibilityJson(nbaId, redis);
        if (evalJson != null) {
            JsonNode obj = M.readTree(evalJson);
            java.util.Set<String> seen = new java.util.HashSet<>();
            for (ObjectNode o : out) seen.add(o.path("actionId").asText() + "::" + o.path("channel").asText());
            // (a) channelActions: cached eligible (when no facts) + any still-listed active/completed for the channel.
            JsonNode channelActions = obj.get("channelActions");
            if (channelActions != null) for (JsonNode e : channelActions) {
                String aid = e.path("actionId").asText(), ch = e.path("channel").asText();
                if (!chanAll && !channel.equals(ch)) continue;
                if (suppressed.contains(aid) || suppressed.contains(aid + "." + ch) || seen.contains(aid + "::" + ch)) continue;
                boolean done = e.path("hardCompleted").asBoolean(false), elig = e.path("eligible").asBoolean(false);
                if (done) { if (!includeCompleted) continue; }                       // completed -> only when requested
                else { if (hot || !elig) continue; }                                // eligible served by the hot path; else cached
                ObjectNode o = M.createObjectNode();
                o.put("actionId", aid); o.put("channel", ch); o.put("name", e.path("name").asText());
                o.put("contentKey", e.path("contentKey").asText());
                if (e.hasNonNull("score")) o.set("score", e.get("score")); else o.putNull("score");
                o.put("state", done ? "completed" : (e.path("active").asBoolean(false) ? "active" : "eligible"));
                out.add(o); seen.add(aid + "::" + ch);
            }
            // (b) completed[]: completions that PRUNED out of channelActions (completed + auto-excluded). Enrich each from the
            //     action catalog and surface it for the requested channel (or its first channel when none is requested).
            if (includeCompleted) {
                Map<String, JsonNode> cat = catalog();
                for (JsonNode cn : obj.path("completed")) {
                    String aid = cn.asText();
                    if (suppressed.contains(aid)) continue;
                    JsonNode doc = cat.get(aid); if (doc == null) continue;
                    String useCh = null, ckey = "";
                    for (JsonNode chn : doc.path("channels")) {
                        String ch = chn.path("channel").asText();
                        if (chanAll || channel.equals(ch)) { useCh = ch; ckey = chn.path("contentKey").asText(""); break; }
                    }
                    if (useCh == null || seen.contains(aid + "::" + useCh)) continue;
                    ObjectNode o = M.createObjectNode();
                    o.put("actionId", aid); o.put("channel", useCh); o.put("name", doc.path("name").asText());
                    o.put("contentKey", ckey); o.putNull("score"); o.put("state", "completed");
                    out.add(o); seen.add(aid + "::" + useCh);
                }
            }
        }

        // rank: eligible/active by score desc, completed sink to the bottom; cap at n.
        out.sort((a, b) -> {
            boolean ca = "completed".equals(a.path("state").asText()), cb = "completed".equals(b.path("state").asText());
            if (ca != cb) return ca ? 1 : -1;
            return Double.compare(b.path("score").asDouble(-1e9), a.path("score").asDouble(-1e9));
        });
        ArrayNode actions = resp.putArray("actions");
        for (int i = 0; i < Math.min(n, out.size()); i++) actions.add(out.get(i));
        // INBOUND TRACKING: stamp one correlationId on this served set + emit a tracked SERVE event (an inbound attempt), so
        // the surface echoes the id back on disposition and the serve->disposition journey is linkable in the lake/timeline.
        String correlationId = UUID.randomUUID().toString();
        for (JsonNode an : actions) ((ObjectNode) an).put("correlationId", correlationId);
        resp.put("correlationId", correlationId);
        resp.put("count", actions.size()); resp.put("hotpath", hot); resp.put("includeCompleted", includeCompleted);
        c.json(resp);
        if (!actions.isEmpty()) { JsonNode top = actions.get(0);
            emitInbound("INBOUND_SERVE", entity, nbaId, top.path("channel").asText(chanAll ? "" : channel),
                        top.path("actionId").asText(), correlationId, null, top.get("score")); }
    }

    static final java.util.Map<String, List<String>> CHANNEL_FUNNEL = java.util.Map.ofEntries(
        java.util.Map.entry("email", List.of("Delivered", "Opened", "LinkClicked")),
        java.util.Map.entry("sms",   List.of("Delivered", "LinkClicked")),
        java.util.Map.entry("push",  List.of("Delivered", "Opened")),
        java.util.Map.entry("voice", List.of("Answered", "Completed")),
        java.util.Map.entry("mail",  List.of("Delivered")));
    static final List<String> INBOUND_FUNNEL = List.of("Presented", "Accepted", "Completed");
    static List<String> funnelFor(String channel) { return CHANNEL_FUNNEL.getOrDefault(channel, INBOUND_FUNNEL); }

    /** Record a disposition as a member fact via the transactional outbox (kind=disposition); Debezium emits
     *  it to member.facts -> snapshot-builder folds it into the snapshot. Durable + consistent (no direct send). */
    static void inboundDisposition(Context c, JedisPooled redis) throws Exception {
        JsonNode b = M.readTree(c.body());
        String entity = b.path("entityId").asText("");
        String actionId = b.path("actionId").asText("");
        String channel = b.path("channel").asText("");
        List<String> funnel = funnelFor(channel);
        String status = b.path("status").asText(funnel.get(0));
        if (!funnel.contains(status)) { c.status(400).result("status for '" + channel + "' must be one of " + funnel); return; }
        if (entity.isEmpty() || actionId.isEmpty() || channel.isEmpty()) { c.status(400).result("entityId, actionId, channel required"); return; }
        String nbaId = entity.startsWith("nba_") ? entity : redis.get("nba:idmap:OPERATOR:" + entity);
        String contentKey = b.path("contentKey").asText("");
        String key = "nba.disposition." + actionId + "." + channel;
        ObjectNode fact = M.createObjectNode();
        fact.put("entityType", "OPERATOR"); fact.put("entityId", entity);
        if (nbaId != null) fact.put("nbaId", nbaId);
        fact.put("key", key); fact.put("value", status); fact.put("valueType", "STRING");
        if (!contentKey.isEmpty()) fact.put("contentKey", contentKey);
        fact.put("eventTs", System.currentTimeMillis()); fact.put("source", "inbound");
        // keyed by memberId (OPERATOR:{entity}) so it co-locates with the member's facts on one partition.
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            outbox(conn, MEMBER_FACTS, "OPERATOR:" + entity, "disposition", M.writeValueAsString(fact));
            conn.commit();
        }
        log.info("inbound disposition " + entity + " " + actionId + "/" + channel + " = " + status + " (outbox)");
        c.json(fact);
    }

    /** Record a HARD COMPLETION as a member fact via the outbox: nba.completion.{actionId} = "completed".
     *  The rules engine latches it permanently (nba:completed:{nbaId}) and — when the action's
     *  autoExcludeOnCompletion is on — drops the action from eligibility (retired, every channel). This is
     *  the API / partner / lake-fallback path; the lake goal-detector (Phase 2) is the primary source. It is
     *  channel-agnostic (the goal is the action's, not a channel's). source defaults to "api". Mirrors
     *  /dispositions: outbox-only (Debezium emits to member.facts), durable + consistent, no direct send. */
    static void recordCompletion(Context c, JedisPooled redis) throws Exception {
        JsonNode b = M.readTree(c.body());
        String entity = b.path("entityId").asText("");
        String actionId = b.path("actionId").asText("");
        if (entity.isEmpty() || actionId.isEmpty()) { c.status(400).result("entityId, actionId required"); return; }
        String source = b.path("source").asText("api");
        String nbaId = entity.startsWith("nba_") ? entity : redis.get("nba:idmap:OPERATOR:" + entity);
        String key = "nba.completion." + actionId;                 // channel-agnostic: the action's goal
        ObjectNode fact = M.createObjectNode();
        fact.put("entityType", "OPERATOR"); fact.put("entityId", entity);
        if (nbaId != null) fact.put("nbaId", nbaId);
        fact.put("key", key); fact.put("value", "completed"); fact.put("valueType", "STRING");
        fact.put("actionId", actionId);
        fact.put("eventTs", System.currentTimeMillis()); fact.put("source", source);
        // keyed by memberId (OPERATOR:{entity}) so it co-locates with the member's facts on one partition.
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            outbox(conn, MEMBER_FACTS, "OPERATOR:" + entity, "completion", M.writeValueAsString(fact));
            conn.commit();
        }
        log.info("completion " + entity + " " + actionId + " (source=" + source + ", outbox)");
        c.json(fact);
    }

    static int parseInt(String s, int d) { try { return s == null ? d : Integer.parseInt(s); } catch (Exception e) { return d; } }

    // ════════ FAST PATH — synchronous inbound-disposition decision (snapshot -> merge -> eligibility -> model score) ════════
    static double msSince(long startNanos) { return (System.nanoTime() - startNanos) / 1_000_000.0; }
    static double round3(double v) { return Math.round(v * 1000.0) / 1000.0; }

    /** Direct-to-Kafka tracking event (no outbox — fire-and-forget, no DB txn). op = INBOUND_SERVE (the model served actions
     *  to the member inbound = an attempt) | INBOUND_DISPOSITION (the member acted). Lands in nba.activations -> silver_
     *  activations (correlationId column), so the inbound journey links serve->disposition and shows on the member timeline
     *  ALONGSIDE the outbound DISPATCH attempts — one unified attempt log (an action re-dispatched after EXPIRED is another row). */
    static void emitInbound(String op, String entity, String nbaId, String channel, String actionId,
                            String correlationId, String status, JsonNode score) {
        if (PRODUCER == null) return;
        try {
            ObjectNode a = M.createObjectNode();
            a.put("op", op); a.put("entityType", "OPERATOR"); a.put("entityId", entity);
            if (nbaId != null && !nbaId.isEmpty()) a.put("nbaId", nbaId);
            a.put("channel", channel == null ? "" : channel);
            if (actionId != null && !actionId.isEmpty()) a.put("actionId", actionId);
            if (status != null && !status.isEmpty()) a.put("status", status);
            if (score != null && score.isNumber()) a.set("score", score);
            a.put("correlationId", correlationId); a.put("source", "inbound"); a.put("eventTs", System.currentTimeMillis());
            String key = (nbaId != null && !nbaId.isEmpty() ? nbaId : entity) + ":" + (actionId == null ? "" : actionId) + ":" + (channel == null ? "" : channel) + ":inbound";
            PRODUCER.send(new ProducerRecord<>(ACTIVATIONS, key, M.writeValueAsString(a)));
        } catch (Exception e) { log.warn("inbound emit failed", e); }
    }

    /** Durable path for a hot-path fact: publish member-fact shape onto nba.member.facts so the snapshot-builder folds it
     *  (event-time LWW) and the datalake ingests it. Direct producer, no outbox/txn — the API is just the hot path. */
    static void emitMemberFact(String entity, String nbaId, String key, ObjectNode fv) {
        if (PRODUCER == null) return;
        try {
            ObjectNode f = M.createObjectNode();
            f.put("entityType", "OPERATOR"); f.put("entityId", entity);
            if (nbaId != null && !nbaId.isEmpty()) f.put("nbaId", nbaId);
            f.put("key", key); f.set("value", fv.get("value"));
            f.put("valueType", fv.path("valueType").asText("STRING"));
            f.put("eventTs", fv.path("eventTs").asLong(System.currentTimeMillis())); f.put("source", "hotpath");
            PRODUCER.send(new ProducerRecord<>(MEMBER_FACTS, "OPERATOR:" + entity, M.writeValueAsString(f)));
        } catch (Exception e) { log.warn("member-fact emit failed", e); }
    }

    static Object jsonScalar(JsonNode v) {
        if (v == null || v.isNull()) return null;
        if (v.isBoolean()) return v.asBoolean();
        if (v.isIntegralNumber()) return v.asLong();
        if (v.isNumber()) return v.asDouble();
        return v.asText();
    }

    static boolean truthy(Object v) {
        if (v == null) return false;
        if (v instanceof Boolean b) return b;
        if (v instanceof Number n) return n.doubleValue() >= 1;
        String s = v.toString().trim().toLowerCase();
        if (s.equals("true") || s.equals("t") || s.equals("yes")) return true;
        try { return Double.parseDouble(s) >= 1; } catch (Exception e) { return false; }
    }

    /** Operator-suppressed if the WHOLE action ({actionId}) or just this action-channel ({actionId}.{channel}) is in the
     *  in-memory SUPPRESSED set (derived from the compacted nba.definitions ACTION_SUPPRESS stream). Mirrors the rules
     *  engine's isOperatorSuppressed — the hot path strips these so an inbound serve never offers a suppressed action. */
    static boolean isSuppressed(String actionId, String channel) {
        return SUPPRESSED.contains(actionId) || SUPPRESSED.contains(actionId + "." + channel);
    }

    /** LWW-merge a fact into both the structured node (for the model) and the flat map (for KIE), by eventTs. */
    static void mergeFact(ObjectNode struct, Map<String, Object> flat, String key, ObjectNode fv) {
        JsonNode cur = struct.get(key);
        long newTs = fv.path("eventTs").asLong(0);
        if (cur != null && cur.path("eventTs").asLong(0) > newTs) return;   // keep the newer
        struct.set(key, fv);
        flat.put(key, jsonScalar(fv.get("value")));
    }

    /** Action catalog (actionId -> doc), cached 60s from Postgres — used to enumerate inproc candidates + enrich NBAs. */
    static Map<String, JsonNode> catalog() {
        if (System.currentTimeMillis() - CAT_AT > 60_000) {
            try (Connection conn = ds.getConnection(); Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("select id, doc from action")) {
                Map<String, JsonNode> m = new ConcurrentHashMap<>();
                while (rs.next()) m.put(rs.getString(1), M.readTree(rs.getString(2)));
                ACTION_DOCS = m; CAT_AT = System.currentTimeMillis();
                log.info("fast-path catalog loaded " + m.size() + " actions");
            } catch (Exception e) { log.warn("catalog load failed", e); }
        }
        return ACTION_DOCS;
    }

    /** The member's snapshot as the fact:-prefixed hash map (+ __meta), from EITHER Redis (classic) or the KStreams
     *  decision-engine's IQ endpoint (NBA_SNAPSHOT_SOURCE=kstreams). Identical shape either way, so every caller is
     *  source-agnostic. On kstreams: GET {engine}/snapshot/{nbaId} -> {entityType,entityId,updatedTs,facts{}} ->
     *  rebuild the hash (fact:{key} JSON + __meta). 404 -> empty map. Follows the one 307 (cross-pod owner redirect). */
    static Map<String, String> snapshotHash(String nbaId, JedisPooled redis) {
        if (nbaId == null) return null;
        if (!"kstreams".equalsIgnoreCase(SNAPSHOT_SOURCE)) return redis.hgetAll("nba:snapshot:" + nbaId);
        try {
            java.net.http.HttpResponse<String> resp = iqGet(ENGINE_IQ + "/snapshot/" + nbaId, 1);
            if (resp.statusCode() == 404) return new java.util.HashMap<>();
            if (resp.statusCode() != 200) { log.warn("IQ snapshot HTTP " + resp.statusCode()); return new java.util.HashMap<>(); }
            JsonNode snap = M.readTree(resp.body());
            Map<String, String> h = new java.util.LinkedHashMap<>();
            h.put("__entityType", snap.path("entityType").asText(""));
            h.put("__entityId", snap.path("entityId").asText(""));
            h.put("__nbaId", nbaId);
            h.put("__updatedTs", String.valueOf(snap.path("updatedTs").asLong(0)));
            JsonNode facts = snap.get("facts");
            if (facts != null) { var it = facts.fields(); while (it.hasNext()) { var e = it.next(); h.put("fact:" + e.getKey(), M.writeValueAsString(e.getValue())); } }
            return h;
        } catch (Exception e) { log.warn("IQ snapshot read failed", e); return new java.util.HashMap<>(); }
    }

    /** The member's eligibility (the eval) JSON, from Redis (classic) or the decision-engine's IQ
     *  (NBA_ELIG_SOURCE=kstreams). Same shape either way (the engine materializes nba.evaluations, stripped to
     *  match nba:eligibility). null when absent. */
    static String eligibilityJson(String nbaId, JedisPooled redis) {
        if (nbaId == null) return null;
        if (!"kstreams".equalsIgnoreCase(ELIG_SOURCE)) return redis.get("nba:eligibility:" + nbaId);
        try {
            java.net.http.HttpResponse<String> resp = iqGet(ENGINE_IQ + "/eligibility/" + nbaId, 1);
            if (resp.statusCode() == 200) return resp.body();
            if (resp.statusCode() == 404) return null;
            log.warn("IQ eligibility HTTP " + resp.statusCode());
            return null;
        } catch (Exception e) { log.warn("IQ eligibility read failed", e); return null; }
    }

    /** GET an IQ url, following up to `redirects` 307s (the engine redirects a read to the pod owning the member's partition). */
    static java.net.http.HttpResponse<String> iqGet(String url, int redirects) throws Exception {
        java.net.http.HttpRequest req = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(url)).timeout(Duration.ofSeconds(2)).GET().build();
        java.net.http.HttpResponse<String> resp = HTTP.send(req, java.net.http.HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() == 307 && redirects > 0) {
            String loc = resp.headers().firstValue("Location").orElse(null);
            if (loc != null) return iqGet(loc, redirects - 1);
        }
        return resp;
    }

    /** GET /snapshot/{nbaId} — the member's current snapshot as {nbaId, entityType, entityId, updatedTs, facts}. */
    static void getSnapshot(Context c, JedisPooled redis) throws Exception {
        String entity = c.pathParam("nbaId");
        String nbaId = entity.startsWith("nba_") ? entity : redis.get("nba:idmap:OPERATOR:" + entity);
        Map<String, String> h = snapshotHash(nbaId, redis);
        if (h == null || h.isEmpty()) { c.status(404).json(M.createObjectNode().put("error", "no snapshot").put("entityId", entity)); return; }
        ObjectNode snap = M.createObjectNode();
        snap.put("nbaId", nbaId); snap.put("entityType", h.getOrDefault("__entityType", ""));
        snap.put("entityId", h.getOrDefault("__entityId", entity));
        snap.put("updatedTs", Long.parseLong(h.getOrDefault("__updatedTs", "0")));
        ObjectNode facts = snap.putObject("facts");
        for (Map.Entry<String, String> e : h.entrySet())
            if (e.getKey().startsWith("fact:")) facts.set(e.getKey().substring(5), M.readTree(e.getValue()));
        c.json(snap);
    }

    /** Shared synchronous DECISION core: snapshot -> rich features -> merge(inbound disposition + facts, event-time LWW)
     *  -> eligibility(mode) -> nba-model score -> ranked top-n. channel null/blank scores ALL eligible channels; dispKey
     *  != null merges an inbound disposition first. Returns {nbaId, channel, mode, scorer, eligibleCount, featureSource,
     *  nbas[], timings{}} — used by BOTH the POST /disposition fast path and the GET /next-action hot path (facts given). */
    static ObjectNode hotPathDecide(JedisPooled redis, String entity, String nbaId, String channel,
            String dispKey, String dispStatus, JsonNode bodyFacts, long now, String mode, String scorer, int n) throws Exception {
        long t0 = System.nanoTime();
        // 1) snapshot read (loop state: dispositions/completions/milestones)
        long s0 = System.nanoTime();
        Map<String, String> h = snapshotHash(nbaId, redis);
        double snapshotMs = msSince(s0);
        // 1b) rich model features. Lakebase (the fast online store) needs a CONTINUOUS synced table, blocked by this
        //     account's rootless metastore (no storage root). Until that's configured, query the SOURCE directly — gold,
        //     via the SQL warehouse. Slower than a point-read (the hot path wears it), but NO Redis cache: source, live.
        long fc0 = System.nanoTime();
        Map<String, String> feats = goldFeatures(entity);
        String featSrc = (feats != null && !feats.isEmpty()) ? "gold" : "none";
        boolean cacheHit = feats != null && !feats.isEmpty();
        double featuresMs = msSince(fc0);
        // 2) merge: rich features + loop snapshot + inbound disposition + inbound facts, event-time LWW
        long m0 = System.nanoTime();
        ObjectNode struct = M.createObjectNode();
        Map<String, Object> flat = new java.util.HashMap<>();
        if (feats != null) for (Map.Entry<String, String> e : feats.entrySet()) {
            try { mergeFact(struct, flat, e.getKey(), (ObjectNode) M.readTree(e.getValue())); } catch (Exception ignore) {}
        }
        if (h != null) for (Map.Entry<String, String> e : h.entrySet()) {
            if (!e.getKey().startsWith("fact:")) continue;
            JsonNode fv = M.readTree(e.getValue());
            if (fv.isObject()) mergeFact(struct, flat, e.getKey().substring(5), (ObjectNode) fv);
        }
        if (dispKey != null) {
            ObjectNode dv = M.createObjectNode(); dv.put("value", dispStatus); dv.put("valueType", "STRING"); dv.put("eventTs", now);
            mergeFact(struct, flat, dispKey, dv);
        }
        if (bodyFacts != null && bodyFacts.isObject()) {
            var it = bodyFacts.fields();
            while (it.hasNext()) {
                var en = it.next(); JsonNode v = en.getValue(); ObjectNode fv;
                if (v.isObject() && v.has("value")) { fv = ((ObjectNode) v).deepCopy(); if (!fv.has("eventTs")) fv.put("eventTs", now); }
                else { fv = M.createObjectNode(); fv.set("value", v); fv.put("eventTs", now); }
                mergeFact(struct, flat, en.getKey(), fv);
            }
        }
        double mergeMs = msSince(m0);
        // 3) eligibility (mode-switched); channel null/blank keeps all eligible channels. Then strip operator-suppressed.
        long e0 = System.nanoTime();
        boolean allCh = channel == null || channel.isBlank();
        List<String[]> elig = new ArrayList<>();
        if ("inproc".equalsIgnoreCase(mode)) {
            if (allCh) for (String ch : CHANNEL_FUNNEL.keySet()) elig.addAll(inprocEligible(nbaId, ch, flat, redis));
            else elig = inprocEligible(nbaId, channel, flat, redis);
        } else {
            List<String> hits = kieEval(nbaId, flat);
            if (hits != null) for (String slug : hits) {
                int i = slug.indexOf("::"); if (i < 0) continue;
                String aid = slug.substring(0, i), ch = slug.substring(i + 2);
                if (allCh || ch.equals(channel)) elig.add(new String[]{aid, ch});
            }
        }
        elig.removeIf(ac -> isSuppressed(ac[0], ac[1]));
        double eligMs = msSince(e0);
        // 4) score: local nba-model (default) OR the Databricks Model Serving endpoint (scorer=dbx)
        long sc0 = System.nanoTime();
        List<ObjectNode> scored = "dbx".equalsIgnoreCase(scorer) ? modelScoreDbx(struct, elig) : modelScore(nbaId, struct, elig);
        double scoreMs = msSince(sc0);
        // 5) rank + build response
        scored.sort((x, y) -> Double.compare(y.path("score").asDouble(-1e9), x.path("score").asDouble(-1e9)));
        ObjectNode resp = M.createObjectNode();
        resp.put("nbaId", nbaId == null ? "" : nbaId); resp.put("channel", allCh ? "" : channel); resp.put("mode", mode); resp.put("scorer", scorer);
        resp.put("eligibleCount", elig.size()); resp.put("featuresCached", cacheHit); resp.put("featureSource", featSrc);
        ArrayNode nbas = resp.putArray("nbas");
        for (int i = 0; i < Math.min(n, scored.size()); i++) nbas.add(scored.get(i));
        double totalMs = msSince(t0);
        ObjectNode tm = resp.putObject("timings");
        tm.put("snapshot_ms", round3(snapshotMs)); tm.put("features_ms", round3(featuresMs)); tm.put("merge_ms", round3(mergeMs));
        tm.put("elig_ms", round3(eligMs)); tm.put("score_ms", round3(scoreMs)); tm.put("total_ms", round3(totalMs));

        // ── OBSERVABILITY ── per-stage + total hot-path latency + the decision outcome, scrapable at GET /metrics.
        //    This is the quantified "tens-of-ms synchronous decision vs the ~30s async medallion loop" story.
        recStage("snapshot", snapshotMs); recStage("features", featuresMs); recStage("merge", mergeMs);
        recStage("elig", eligMs); recStage("score", scoreMs);
        Metrics.timer("nba_hotpath_decision_seconds", "mode", mode, "scorer", scorer)
                .record((long) (totalMs * 1_000_000d), java.util.concurrent.TimeUnit.NANOSECONDS);
        Metrics.counter("nba_hotpath_decisions_total", "mode", mode, "scorer", scorer, "feature_source", featSrc).increment();

        // ── OPTIMISTIC WRITE-THROUGH ── only on a real hot path (presented facts / inbound disposition). The API is JUST
        //    the hot path: it warms Redis so the very next read — even a no-facts serve — is fresh, and emits the facts to
        //    Kafka as the DURABLE path. The snapshot-builder re-applies them (event-time LWW) as the self-heal — if this
        //    optimistic write loses a race or fails, the bus reconciles it. Best-effort: never fail the decision on it.
        boolean hasFacts = bodyFacts != null && bodyFacts.isObject() && bodyFacts.size() > 0;
        if (nbaId != null && (hasFacts || dispKey != null)) {
            try {
                Map<String, String> snapWrites = new java.util.LinkedHashMap<>();   // fact fields -> snapshot (eventTs-stamped)
                if (dispKey != null) {                                              // the inbound disposition itself
                    ObjectNode dv = M.createObjectNode(); dv.put("value", dispStatus); dv.put("valueType", "STRING"); dv.put("eventTs", now);
                    snapWrites.put("fact:" + dispKey, M.writeValueAsString(dv));    // (its Kafka durable copy is postDisposition's outbox — no double-emit)
                }
                if (hasFacts) {
                    var it = bodyFacts.fields();
                    while (it.hasNext()) {
                        var en = it.next(); JsonNode v = en.getValue(); ObjectNode fv;
                        if (v.isObject() && v.has("value")) { fv = ((ObjectNode) v).deepCopy(); if (!fv.has("eventTs")) fv.put("eventTs", now); }
                        else { fv = M.createObjectNode(); fv.set("value", v); fv.put("valueType", "STRING"); fv.put("eventTs", now); }
                        snapWrites.put("fact:" + en.getKey(), M.writeValueAsString(fv));
                        emitMemberFact(entity, nbaId, en.getKey(), fv);             // durable: -> nba.member.facts (datalake + builder backstop)
                    }
                }
                // refresh eligibility SCORES on the cached eval so a subsequent NO-FACTS serve isn't stale (read-modify-
                // write; the router's next eval heals the full structure). Best-effort, eventually-consistent.
                // source=kstreams: eligibility is materialized by the engine from nba.evaluations (no Redis eligibility to
                // refresh, and the read would be stale) — skip the optimistic refresh; the async loop heals it.
                String eligOut = null;
                String eligStr = "kstreams".equalsIgnoreCase(ELIG_SOURCE) ? null : redis.get("nba:eligibility:" + nbaId);
                if (eligStr != null && !scored.isEmpty()) {
                    ObjectNode eo = (ObjectNode) M.readTree(eligStr); JsonNode ca = eo.get("channelActions");
                    if (ca != null && ca.isArray()) {
                        Map<String, JsonNode> byKey = new java.util.HashMap<>();
                        for (ObjectNode s : scored) byKey.put(s.path("actionId").asText() + "::" + s.path("channel").asText(), s);
                        for (JsonNode e : ca) {
                            JsonNode s = byKey.get(e.path("actionId").asText() + "::" + e.path("channel").asText());
                            if (s != null && s.hasNonNull("score")) { ((ObjectNode) e).set("score", s.get("score")); ((ObjectNode) e).put("eligible", true); }
                        }
                        eligOut = M.writeValueAsString(eo);
                    }
                }
                // write-through: snapshot facts + refreshed eligibility, back-to-back (the two keys are read separately
                // downstream, so a strict txn buys nothing — and keeps us off Lua/MULTI).
                // source=kstreams: snapshot + eligibility both live in the engine's state (fed by the Kafka facts above /
                // by nba.evaluations), so there's nothing to optimistically write — both write-throughs are gated off
                // (eligOut stays null above). Under redis they run as before. The async loop heals either way.
                if (!snapWrites.isEmpty() && !"kstreams".equalsIgnoreCase(SNAPSHOT_SOURCE)) redis.hset("nba:snapshot:" + nbaId, snapWrites);
                if (eligOut != null) redis.set("nba:eligibility:" + nbaId, eligOut);
            } catch (Exception ex) { log.warn("hot-path write-through failed", ex); }
        }
        return resp;
    }

    /** Record a hot-path stage duration (ms) as the timer nba_hotpath_stage_seconds{stage=..}. */
    private static void recStage(String stage, double ms) {
        Metrics.timer("nba_hotpath_stage_seconds", "stage", stage)
                .record((long) (ms * 1_000_000d), java.util.concurrent.TimeUnit.NANOSECONDS);
    }

    /** POST /disposition — THE FAST PATH. {entityId, actionId, channel, status?, facts?, mode?=kie|inproc, n?=1, writeback?=async|sync|none}.
     *  Merges the inbound disposition + facts, runs eligibility, scores via nba-model, returns the top-n NBAs + timings, then
     *  durably writes the disposition back via the outbox (off the latency path) so the medallion loop stays intact. */
    static void postDisposition(Context c, JedisPooled redis) throws Exception {
        JsonNode b = M.readTree(c.body());
        String entity = b.path("entityId").asText("");
        String actionId = b.path("actionId").asText("");
        String channel = b.path("channel").asText("");
        String mode = b.path("mode").asText("kie");
        String scorer = b.path("scorer").asText(DEFAULT_SCORER);
        int n = Math.max(1, b.path("n").asInt(1));
        String writeback = b.path("writeback").asText("async");
        if (entity.isEmpty() || channel.isEmpty()) { c.status(400).result("entityId, channel required"); return; }
        String status = b.path("status").asText(funnelFor(channel).get(0));
        String inCorr = b.path("correlationId").asText("");          // the prior serve's id the member is acting on
        String nbaId = entity.startsWith("nba_") ? entity : redis.get("nba:idmap:OPERATOR:" + entity);
        long now = System.currentTimeMillis();
        String dispKey = actionId.isEmpty() ? null : "nba.disposition." + actionId + "." + channel;

        ObjectNode resp = hotPathDecide(redis, entity, nbaId, channel, dispKey, status, b.get("facts"), now, mode, scorer, n);
        // INBOUND TRACKING: the member ACTED -> emit a disposition event linked to the serve's correlationId; then stamp a
        // NEW correlationId on the next-served set + emit its serve event, so the journey keeps linking serve->disposition.
        if (!actionId.isEmpty() && !inCorr.isEmpty())
            emitInbound("INBOUND_DISPOSITION", entity, nbaId, channel, actionId, inCorr, status, null);
        String outCorr = UUID.randomUUID().toString();
        resp.put("correlationId", outCorr);
        for (JsonNode an : resp.path("nbas")) ((ObjectNode) an).put("correlationId", outCorr);
        c.json(resp);
        if (resp.path("nbas").size() > 0) { JsonNode top = resp.path("nbas").get(0);
            emitInbound("INBOUND_SERVE", entity, nbaId, top.path("channel").asText(channel), top.path("actionId").asText(), outCorr, null, top.get("score")); }

        // write-back (AFTER responding): durable disposition via the existing outbox, OFF the latency path. correlationId
        // rides the fact so the disposition links to the serve in the lake too.
        if (!"none".equalsIgnoreCase(writeback) && !actionId.isEmpty()) {
            final String fEntity = entity, fNbaId = nbaId, fAction = actionId, fChannel = channel, fStatus = status, fCorr = inCorr; final long fNow = now;
            Runnable wb = () -> {
                try (Connection conn = ds.getConnection()) {
                    conn.setAutoCommit(false);
                    ObjectNode fact = M.createObjectNode(); fact.put("entityType", "OPERATOR"); fact.put("entityId", fEntity);
                    if (fNbaId != null) fact.put("nbaId", fNbaId);
                    fact.put("key", "nba.disposition." + fAction + "." + fChannel); fact.put("value", fStatus);
                    fact.put("valueType", "STRING"); fact.put("eventTs", fNow); fact.put("source", "fastpath");
                    if (!fCorr.isEmpty()) fact.put("correlationId", fCorr);
                    outbox(conn, MEMBER_FACTS, "OPERATOR:" + fEntity, "disposition", M.writeValueAsString(fact)); conn.commit();
                } catch (Exception ex) { log.warn("fastpath writeback failed", ex); }
            };
            if ("sync".equalsIgnoreCase(writeback)) wb.run(); else WB_POOL.submit(wb);
        }
    }

    // Built-in opt-out: a channel is dead if ANY action's latest disposition on it hit the channel's opt-out raw
    // status (email Unsubscribe / sms STOP). Derived straight from the durable disposition facts on the snapshot —
    // mirrors the rules engine; the nba:optout side-latch is gone (one eligibility object is the only Redis state).
    static final Map<String, String> OPTOUT_RAW = Map.of("email", "Unsubscribe", "sms", "STOP");
    static boolean channelOptedOut(Map<String, Object> flat, String channel) {
        String raw = OPTOUT_RAW.get(channel);
        if (raw == null) return false;
        for (Map.Entry<String, Object> e : flat.entrySet()) {
            String k = e.getKey();
            if (k.startsWith("nba.disposition.") && k.endsWith("." + channel)
                    && e.getValue() != null && raw.equals(e.getValue().toString())) return true;
        }
        return false;
    }

    /** In-process eligibility FLOOR: candidate actions on the channel (cached catalog) minus the 4 gates — no HTTP hop. */
    static List<String[]> inprocEligible(String nbaId, String channel, Map<String, Object> flat, JedisPooled redis) {
        List<String[]> out = new ArrayList<>();
        if (truthy(flat.get("isDNC"))) return out;                                 // DNC -> nothing
        if ("sms".equals(channel) && !truthy(flat.get("smsConsent"))) return out;  // sms needs consent
        if (channelOptedOut(flat, channel)) return out;                            // channel opted out (off the snapshot)
        for (Map.Entry<String, JsonNode> e : catalog().entrySet()) {
            JsonNode chs = e.getValue().get("channels");
            if (chs == null) continue;
            for (JsonNode ch : chs) if (channel.equals(ch.path("channel").asText())) { out.add(new String[]{e.getKey(), channel}); break; }
        }
        return out;
    }

    /** POST {nbaId, facts} to nba-kie-server /evaluate; return its "actionId::channel" hits (null on failure). */
    static List<String> kieEval(String nbaId, Map<String, Object> f) {
        try {
            ObjectNode req = M.createObjectNode(); req.put("nbaId", nbaId == null ? "" : nbaId);
            ObjectNode facts = req.putObject("facts");
            for (Map.Entry<String, Object> e : f.entrySet()) {
                Object v = e.getValue();
                if (v == null) facts.putNull(e.getKey());
                else if (v instanceof Boolean bv) facts.put(e.getKey(), bv);
                else if (v instanceof Long lv) facts.put(e.getKey(), lv);
                else if (v instanceof Integer iv) facts.put(e.getKey(), iv.longValue());
                else if (v instanceof Double dv) facts.put(e.getKey(), dv);
                else if (v instanceof Number nv) facts.put(e.getKey(), nv.doubleValue());
                else facts.put(e.getKey(), v.toString());
            }
            java.net.http.HttpRequest hr = java.net.http.HttpRequest.newBuilder(java.net.URI.create(KIE_URL + "/evaluate"))
                    .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(M.writeValueAsString(req))).build();
            java.net.http.HttpResponse<String> resp = HTTP.send(hr, java.net.http.HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) { log.warn("kie HTTP " + resp.statusCode()); return null; }
            JsonNode hitsNode = M.readTree(resp.body()).get("hits");
            List<String> hits = new ArrayList<>();
            if (hitsNode != null && hitsNode.isArray()) for (JsonNode h : hitsNode) hits.add(h.asText());
            return hits;
        } catch (Exception e) { log.warn("kie eval failed", e); return null; }
    }

    /** POST {facts, candidates} to nba-model /score; return NBA nodes {actionId,channel,name,contentKey,score}. */
    static List<ObjectNode> modelScore(String nbaId, ObjectNode structFacts, List<String[]> candidates) {
        List<ObjectNode> out = new ArrayList<>();
        if (candidates.isEmpty()) return out;
        try {
            ObjectNode req = M.createObjectNode(); if (nbaId != null) req.put("nbaId", nbaId);
            req.set("facts", structFacts);
            ArrayNode cand = req.putArray("candidates");
            for (String[] ac : candidates) { ObjectNode o = cand.addObject(); o.put("actionId", ac[0]); o.put("channel", ac[1]); }
            java.net.http.HttpRequest hr = java.net.http.HttpRequest.newBuilder(java.net.URI.create(MODEL_URL + "/score"))
                    .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(M.writeValueAsString(req))).build();
            java.net.http.HttpResponse<String> resp = HTTP.send(hr, java.net.http.HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) { log.warn("model HTTP " + resp.statusCode()); return out; }
            JsonNode scores = M.readTree(resp.body()).get("scores");
            Map<String, JsonNode> cat = catalog();
            if (scores != null) for (JsonNode s : scores) {
                if (s.get("score") == null || s.get("score").isNull()) continue;
                String aid = s.path("actionId").asText(), ch = s.path("channel").asText();
                ObjectNode o = M.createObjectNode(); o.put("actionId", aid); o.put("channel", ch); o.set("score", s.get("score"));
                JsonNode doc = cat.get(aid);
                if (doc != null) {
                    o.put("name", doc.path("name").asText(""));
                    JsonNode chs = doc.get("channels"); String ckey = "";
                    if (chs != null) for (JsonNode cc : chs) if (ch.equals(cc.path("channel").asText())) { ckey = cc.path("contentKey").asText(""); break; }
                    o.put("contentKey", ckey);
                }
                out.add(o);
            }
        } catch (Exception e) { log.warn("model score failed", e); }
        return out;
    }

    // ── FEATURE STORE: read a member's rich features STRAIGHT from gold via the SQL warehouse (goldFeatures) — no cache ──
    static synchronized String dbxToken() {                          // OAuth M2M (client-credentials), cached ~1h
        if (DBX_TOKEN != null && System.currentTimeMillis() < DBX_TOKEN_EXP) return DBX_TOKEN;
        if (DBX_HOST.isEmpty() || DBX_CLIENT_ID.isEmpty()) return null;
        try {
            String basic = java.util.Base64.getEncoder().encodeToString((DBX_CLIENT_ID + ":" + DBX_CLIENT_SECRET).getBytes());
            java.net.http.HttpRequest hr = java.net.http.HttpRequest.newBuilder(java.net.URI.create(DBX_HOST + "/oidc/v1/token"))
                    .timeout(Duration.ofSeconds(10)).header("Authorization", "Basic " + basic)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString("grant_type=client_credentials&scope=all-apis")).build();
            java.net.http.HttpResponse<String> resp = HTTP.send(hr, java.net.http.HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) { log.warn("dbx token HTTP " + resp.statusCode()); return null; }
            JsonNode t = M.readTree(resp.body());
            DBX_TOKEN = t.path("access_token").asText();
            DBX_TOKEN_EXP = System.currentTimeMillis() + (t.path("expires_in").asLong(3600) - 120) * 1000;
            return DBX_TOKEN;
        } catch (Exception e) { log.warn("dbx token failed", e); return null; }
    }

    /** Read the member's rich features STRAIGHT from gold_member_snapshot via the SQL warehouse — NO Redis cache. Slower
     *  than a Lakebase point-read, but Lakebase's CONTINUOUS sync is blocked by this account's rootless metastore (no
     *  storage root), so gold is the live source until that's configured. Returns the merge-shaped fact map. */
    static Map<String, String> goldFeatures(String entityId) {
        Map<String, String> out = new java.util.HashMap<>();
        if (entityId == null || entityId.isEmpty() || DBX_HOST.isEmpty()) return out;
        String tok = dbxToken();
        if (tok == null) return out;
        try {
            String sql = "SELECT key, value, valueType FROM " + LAKE_NS + ".gold_member_snapshot WHERE entityId = '"
                    + entityId.replace("'", "''") + "' AND key LIKE 'operator.%'";
            ObjectNode req = M.createObjectNode();
            req.put("warehouse_id", DBX_WAREHOUSE); req.put("statement", sql); req.put("wait_timeout", "30s");
            java.net.http.HttpRequest hr = java.net.http.HttpRequest.newBuilder(java.net.URI.create(DBX_HOST + "/api/2.0/sql/statements"))
                    .timeout(Duration.ofSeconds(30)).header("Authorization", "Bearer " + tok).header("Content-Type", "application/json")
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(M.writeValueAsString(req))).build();
            java.net.http.HttpResponse<String> resp = HTTP.send(hr, java.net.http.HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) { log.warn("gold sql HTTP " + resp.statusCode()); return out; }
            JsonNode data = M.readTree(resp.body()).path("result").path("data_array");
            if (!data.isArray() || data.isEmpty()) return out;
            long now = System.currentTimeMillis();
            for (JsonNode row : data) {
                String k = row.get(0).asText(null); if (k == null) continue;
                String v = row.get(1).isNull() ? null : row.get(1).asText();
                String vt = row.size() > 2 ? row.get(2).asText("STRING") : "STRING";
                ObjectNode fv = M.createObjectNode();
                if (v == null) fv.putNull("value");
                else if ("LONG".equals(vt)) { try { fv.put("value", Long.parseLong(v)); } catch (Exception e) { fv.put("value", v); } }
                else if ("DOUBLE".equals(vt)) { try { fv.put("value", Double.parseDouble(v)); } catch (Exception e) { fv.put("value", v); } }
                else if (vt.startsWith("BOOL")) fv.put("value", "true".equalsIgnoreCase(v) || "1".equals(v));
                else fv.put("value", v);
                fv.put("valueType", vt); fv.put("eventTs", now);
                out.put(k, M.writeValueAsString(fv));
            }
        } catch (Exception e) { log.warn("goldFeatures failed", e); }
        return out;
    }

    // ── LAKEBASE online feature store: OAuth-mint a DB credential, pooled JDBC, read gold features straight from PG ──
    static synchronized String lakebaseToken() {                     // generate-database-credential, cached ~50m
        if (LB_TOKEN != null && System.currentTimeMillis() < LB_TOKEN_EXP) return LB_TOKEN;
        String bearer = dbxToken();                                  // the LAKE-workspace SP token (has the database API)
        if (bearer == null || DBX_HOST.isEmpty()) return null;
        try {
            ObjectNode req = M.createObjectNode(); req.put("request_id", java.util.UUID.randomUUID().toString());
            req.putArray("instance_names").add(LAKEBASE_INSTANCE);   // request_id is REQUIRED (else HTTP 400)
            java.net.http.HttpRequest hr = java.net.http.HttpRequest.newBuilder(java.net.URI.create(DBX_HOST + "/api/2.0/database/credentials"))
                    .timeout(Duration.ofSeconds(10)).header("Authorization", "Bearer " + bearer).header("Content-Type", "application/json")
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(M.writeValueAsString(req))).build();
            java.net.http.HttpResponse<String> resp = HTTP.send(hr, java.net.http.HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) { log.warn("lakebase cred HTTP " + resp.statusCode()); return null; }
            LB_TOKEN = M.readTree(resp.body()).path("token").asText("");
            LB_TOKEN_EXP = System.currentTimeMillis() + 50 * 60 * 1000;   // tokens last ~1h; rotate early
            return LB_TOKEN.isEmpty() ? null : LB_TOKEN;
        } catch (Exception e) { log.warn("lakebase token failed", e); return null; }
    }

    static synchronized javax.sql.DataSource lakebaseDS() {          // pooled; rebuilt hourly with a fresh credential
        if (LB_DS != null && System.currentTimeMillis() < LB_DS_EXP) return LB_DS;
        String tok = lakebaseToken();
        if (tok == null || LAKEBASE_HOST.isEmpty()) return null;
        com.zaxxer.hikari.HikariDataSource old = LB_DS;
        com.zaxxer.hikari.HikariConfig cfg = new com.zaxxer.hikari.HikariConfig();
        cfg.setJdbcUrl("jdbc:postgresql://" + LAKEBASE_HOST + ":5432/nba?sslmode=require");
        cfg.setUsername(LAKEBASE_USER); cfg.setPassword(tok);
        cfg.setMaximumPoolSize(4); cfg.setConnectionTimeout(8000); cfg.setMaxLifetime(50 * 60 * 1000); cfg.setPoolName("lakebase");
        LB_DS = new com.zaxxer.hikari.HikariDataSource(cfg);
        LB_DS_EXP = System.currentTimeMillis() + 50 * 60 * 1000;
        if (old != null) try { old.close(); } catch (Exception ignore) {}
        return LB_DS;
    }

    /** DORMANT — the fast online store. The hot path queries gold directly (goldFeatures) because Lakebase's CONTINUOUS
     *  synced table is blocked by this account's rootless metastore (no storage root). Once that's configured, create the
     *  synced table (keyed by nbaId) and switch the hot path back to this point-read. */
    static Map<String, String> lakebaseFeatures(String entityId) {
        Map<String, String> out = new java.util.HashMap<>();
        javax.sql.DataSource ds = lakebaseDS();
        if (ds == null || entityId == null || entityId.isEmpty()) return out;
        String sql = "SELECT key, value, \"valueType\" FROM public.gold_member_snapshot WHERE \"entityId\" = ? AND key LIKE 'operator.%'";
        try (java.sql.Connection conn = ds.getConnection(); java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, entityId);
            long now = System.currentTimeMillis();
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String k = rs.getString(1); if (k == null) continue;
                    String v = rs.getString(2); String vt = rs.getString(3); if (vt == null) vt = "STRING";
                    ObjectNode fv = M.createObjectNode();
                    if (v == null) fv.putNull("value");
                    else if ("LONG".equals(vt)) { try { fv.put("value", Long.parseLong(v)); } catch (Exception e) { fv.put("value", v); } }
                    else if ("DOUBLE".equals(vt)) { try { fv.put("value", Double.parseDouble(v)); } catch (Exception e) { fv.put("value", v); } }
                    else if (vt.startsWith("BOOL")) fv.put("value", "true".equalsIgnoreCase(v) || "1".equals(v));
                    else fv.put("value", v);
                    fv.put("valueType", vt); fv.put("eventTs", now);
                    out.put(k, M.writeValueAsString(fv));
                }
            }
        } catch (Exception e) { log.warn("lakebaseFeatures failed", e); }
        return out;
    }

    static synchronized String mlToken() {                           // OAuth M2M for the ML workspace (serving endpoint)
        if (ML_TOKEN != null && System.currentTimeMillis() < ML_TOKEN_EXP) return ML_TOKEN;
        if (ML_HOST.isEmpty() || ML_CLIENT_ID.isEmpty()) return null;
        try {
            String basic = java.util.Base64.getEncoder().encodeToString((ML_CLIENT_ID + ":" + ML_CLIENT_SECRET).getBytes());
            java.net.http.HttpRequest hr = java.net.http.HttpRequest.newBuilder(java.net.URI.create(ML_HOST + "/oidc/v1/token"))
                    .timeout(Duration.ofSeconds(10)).header("Authorization", "Basic " + basic)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString("grant_type=client_credentials&scope=all-apis")).build();
            java.net.http.HttpResponse<String> resp = HTTP.send(hr, java.net.http.HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) { log.warn("ml token HTTP " + resp.statusCode()); return null; }
            JsonNode t = M.readTree(resp.body());
            ML_TOKEN = t.path("access_token").asText();
            ML_TOKEN_EXP = System.currentTimeMillis() + (t.path("expires_in").asLong(3600) - 120) * 1000;
            return ML_TOKEN;
        } catch (Exception e) { log.warn("ml token failed", e); return null; }
    }

    /** scorer=dbx: POST the member's facts to the Databricks Model Serving endpoint (nba-cql) -> per-arm Q-values. */
    static List<ObjectNode> modelScoreDbx(ObjectNode structFacts, List<String[]> candidates) {
        List<ObjectNode> out = new ArrayList<>();
        if (candidates.isEmpty() || SERVING_URL.isEmpty()) return out;
        String tok = mlToken();
        if (tok == null) return out;
        try {
            ObjectNode rec = M.createObjectNode(); rec.put("facts", M.writeValueAsString(structFacts));
            ObjectNode req = M.createObjectNode(); req.putArray("dataframe_records").add(rec);
            java.net.http.HttpRequest hr = java.net.http.HttpRequest.newBuilder(java.net.URI.create(SERVING_URL))
                    .timeout(Duration.ofSeconds(30)).header("Authorization", "Bearer " + tok).header("Content-Type", "application/json")
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(M.writeValueAsString(req))).build();
            java.net.http.HttpResponse<String> resp = HTTP.send(hr, java.net.http.HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) { log.warn("serving HTTP " + resp.statusCode()); return out; }
            JsonNode preds = M.readTree(resp.body()).get("predictions");
            if (preds == null || !preds.isArray() || preds.isEmpty()) return out;
            JsonNode scores = M.readTree(preds.get(0).asText()).get("scores");           // pyfunc returns a JSON string per row
            Set<String> want = new HashSet<>(); for (String[] ac : candidates) want.add(ac[0] + "|" + ac[1]);
            Map<String, JsonNode> cat = catalog();
            if (scores != null) for (JsonNode s : scores) {
                String aid = s.path("actionId").asText(), ch = s.path("channel").asText();
                if (!want.contains(aid + "|" + ch)) continue;                            // only the eligible candidates
                ObjectNode o = M.createObjectNode(); o.put("actionId", aid); o.put("channel", ch); o.set("score", s.get("score"));
                JsonNode doc = cat.get(aid);
                if (doc != null) {
                    o.put("name", doc.path("name").asText(""));
                    JsonNode chs = doc.get("channels"); String ckey = "";
                    if (chs != null) for (JsonNode cc : chs) if (ch.equals(cc.path("channel").asText())) { ckey = cc.path("contentKey").asText(""); break; }
                    o.put("contentKey", ckey);
                }
                out.add(o);
            }
        } catch (Exception e) { log.warn("serving score failed", e); }
        return out;
    }

    static void upsert(Context c, String table, String aggType) throws Exception {
        ObjectNode doc = (ObjectNode) M.readTree(c.body());
        String id = c.pathParamMap().getOrDefault("id",
                doc.hasNonNull("id") ? doc.get("id").asText() : null);
        if (id == null || id.isBlank()) {
            id = aggType.toLowerCase() + "_" + UUID.randomUUID().toString().substring(0, 8);
        }
        doc.put("id", id);

        // TYPE-CORRECTNESS GATE: reject any rule operator that doesn't fit the fact's declared type (nba:facttype).
        // Unknown facts pass (a new fact may be authored before it's catalogued); 'exists' fits any type.
        try {
            validateTree(doc.get("inclusion")); validateTree(doc.get("exclusion"));
            validateTree(doc.get("logic")); validateTree(doc.get("completion"));
            JsonNode chs = doc.get("channels");
            if (chs != null && chs.isArray()) for (JsonNode ch : chs) {
                JsonNode vs = ch.get("variants");
                if (vs != null && vs.isArray()) for (JsonNode v : vs) validateTree(v.get("conditions"));
            }
        } catch (IllegalArgumentException ve) {
            c.status(400).json(M.createObjectNode().put("error", ve.getMessage())); return;
        }

        Set<String> facts = new TreeSet<>();
        collectFacts(doc.get("inclusion"), facts);
        collectFacts(doc.get("exclusion"), facts);
        collectFacts(doc.get("logic"), facts);
        JsonNode channels = doc.get("channels");
        if (channels != null && channels.isArray()) {
            for (JsonNode chn : channels) {
                JsonNode variants = chn.get("variants");
                if (variants != null && variants.isArray())
                    for (JsonNode v : variants) collectFacts(v.get("conditions"), facts);
            }
        }
        // HARD COMPLETION (actions only): the `completion` tree is the goal criterion over member facts —
        // its facts must ride the lean snapshot so the rules engine can evaluate + latch it. (The explicit
        // nba.completion.{id} signal rides via the snapshot-builder's always-attach rule, not rulefacts.)
        if ("ACTION".equals(aggType)) collectFacts(doc.get("completion"), facts);
        ArrayNode fu = doc.putArray("factsUsed");
        facts.forEach(fu::add);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(
                    "insert into " + table + "(id, doc, updated_at) values (?, ?::jsonb, now()) " +
                    "on conflict (id) do update set doc = excluded.doc, updated_at = now()")) {
                ps.setString(1, id);
                ps.setString(2, M.writeValueAsString(doc));
                ps.executeUpdate();
            }
            // SAME transaction: Debezium routes ACTION:{id} (payload=doc) to the definitions topic.
            outbox(conn, DEFS_TOPIC, aggType + ":" + id, null, M.writeValueAsString(doc));
            conn.commit();
        }
        log.info("upsert " + table + " id=" + id + " factsUsed=" + facts);
        c.json(doc);
    }

    static void list(Context c, String table) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select doc from " + table + " order by updated_at desc")) {
            ArrayNode arr = M.createArrayNode();
            while (rs.next()) arr.add(M.readTree(rs.getString(1)));
            c.json(arr);
        }
    }

    static void get(Context c, String table) throws Exception {
        String id = c.pathParam("id");
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement("select doc from " + table + " where id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) c.json(M.readTree(rs.getString(1)));
                else c.status(404).json(M.createObjectNode().put("error", "not_found"));
            }
        }
    }

    static void delete(Context c, String table, String aggType) throws Exception {
        String id = c.pathParam("id");
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement("delete from " + table + " where id = ?")) {
                ps.setString(1, id);
                ps.executeUpdate();
            }
            // SAME transaction: null payload -> Debezium emits a TOMBSTONE for ACTION:{id} on the compacted topic.
            outbox(conn, DEFS_TOPIC, aggType + ":" + id, null, null);
            conn.commit();
        }
        c.json(M.createObjectNode().put("deleted", id));
    }

    /** Insert an outbox row in the caller's transaction (transactional outbox; Debezium relays it).
     *  aggregatetype = target topic, aggregateid = kafka key, kind -> 'kind' header, null payload = tombstone. */
    static void outbox(Connection conn, String aggregatetype, String aggregateid, String kind, String payload) throws Exception {
        try (PreparedStatement ob = conn.prepareStatement(
                "insert into outbox_defs(aggregatetype, aggregateid, kind, payload) values (?, ?, ?, ?)")) {
            ob.setString(1, aggregatetype);
            ob.setString(2, aggregateid);
            if (kind == null) ob.setNull(3, java.sql.Types.VARCHAR); else ob.setString(3, kind);
            if (payload == null) ob.setNull(4, java.sql.Types.VARCHAR); else ob.setString(4, payload);
            ob.executeUpdate();
        }
    }

    /** Recursively collect every "fact" key referenced in a structured condition tree. */
    static void collectFacts(JsonNode node, Set<String> out) {
        if (node == null || node.isNull()) return;
        if (node.hasNonNull("fact")) out.add(node.get("fact").asText());
        JsonNode conds = node.get("conditions");
        if (conds != null && conds.isArray()) {
            for (JsonNode child : conds) collectFacts(child, out);
        }
    }

    /** Validate a condition tree against the fact-type catalog (nba:facttype). Mirrors collectFacts' walk.
     *  Throws IllegalArgumentException on a type/operator mismatch; unknown facts + 'exists' are allowed. */
    static void validateTree(JsonNode node) {
        if (node == null || node.isNull()) return;
        if (node.hasNonNull("fact")) {
            String fact = node.get("fact").asText("");
            String cmp = node.path("cmp").asText("eq");
            if (!fact.isEmpty() && !"exists".equals(cmp)) {
                String type = redis.hget("nba:facttype", fact);
                if (type != null) {
                    String t = type.toUpperCase();
                    Set<String> ok;
                    if (t.equals("BOOL") || t.equals("BOOLEAN")) ok = Set.of("eq", "ne", "exists");
                    else if (t.equals("STRING")) ok = Set.of("eq", "ne", "in", "exists");
                    else ok = Set.of("eq", "ne", "gt", "gte", "lt", "lte", "exists");   // LONG / DOUBLE / numeric
                    if (!ok.contains(cmp))
                        throw new IllegalArgumentException("fact '" + fact + "' is " + type +
                            "; operator '" + cmp + "' is invalid (allowed: " + ok + ")");
                }
            }
        }
        JsonNode conds = node.get("conditions");
        if (conds != null && conds.isArray()) for (JsonNode child : conds) validateTree(child);
    }

    static HikariDataSource connectWithRetry(HikariConfig hc) throws InterruptedException {
        for (int i = 0; i < 30; i++) {
            try {
                HikariDataSource d = new HikariDataSource(hc);
                try (Connection cn = d.getConnection()) { /* probe */ }
                return d;
            } catch (Exception e) {
                log.info("waiting for db... " + e.getMessage());
                Thread.sleep(2000);
            }
        }
        throw new RuntimeException("db unreachable");
    }

    static void initSchema() throws Exception {
        String[] ddl = {
            "create table if not exists action       (id text primary key, doc jsonb not null, updated_at timestamptz not null default now())",
            "create table if not exists global_rule  (id text primary key, doc jsonb not null, updated_at timestamptz not null default now())",
            "create table if not exists channel_rule (id text primary key, doc jsonb not null, updated_at timestamptz not null default now())",
            // transactional outbox (Debezium Outbox Event Router convention; shared nba-outbox connector tails it).
            // payload NULL = tombstone for the compacted definitions topic (def delete).
            "create table if not exists outbox_defs (id uuid primary key default gen_random_uuid(), aggregatetype text not null, aggregateid text not null, kind text, payload text, created_at timestamptz not null default now())",
            // action GROUPS: a taxonomy TREE over actions (parent_id = adjacency list; NULL = root). An action's
            // group is the `groupId` field on its doc. Pure command-center taxonomy — not on the Kafka pipeline.
            "create table if not exists action_group (id text primary key, name text not null, parent_id text references action_group(id), updated_at timestamptz not null default now())",
            // EXPERIENCES: a SECOND, business-facing taxonomy (enrollment, onboarding, …) that groups actions
            // into member journeys. Flat. An action's experience is the `experienceId` field on its doc.
            "create table if not exists experience (id text primary key, name text not null, description text, updated_at timestamptz not null default now())",
            // MILESTONES: a def (name + structured logic, like a rule). The rules engine evaluates it per member
            // and LATCHES completion permanently. Rides the def pipeline (-> nba.definitions MILESTONE:{id}).
            "create table if not exists milestone (id text primary key, doc jsonb not null, updated_at timestamptz not null default now())"
        };
        try (Connection conn = ds.getConnection(); Statement st = conn.createStatement()) {
            for (String d : ddl) st.execute(d);
        }
        log.info("schema ready");
    }

    static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? d : v;
    }
}
