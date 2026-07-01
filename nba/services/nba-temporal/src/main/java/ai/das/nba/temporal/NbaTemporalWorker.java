package ai.das.nba.temporal;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.client.WorkflowOptions;
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy;
import io.temporal.api.enums.v1.WorkflowIdReusePolicy;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import redis.clients.jedis.JedisPooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * NBA Temporal worker.
 *
 * Registers the ChannelAction workflow + activities on a task queue, then runs a bridge that
 * consumes nba.activations and starts (or attaches to) one workflow per (member, action,
 * channel). The workflow is the durable state machine for that activation — it debounces,
 * dispatches, and (next) honors TTL and CANCEL.
 *
 * Workflow id = nba-ca:{nbaId}:{actionId}:{channel}. With CONFLICT=USE_EXISTING a repeated
 * CREATE for an already-running ChannelAction attaches to the live workflow instead of
 * starting a duplicate; REUSE=ALLOW_DUPLICATE lets a fresh one start once the prior closed.
 */
public class NbaTemporalWorker {
    private static final Logger log = LoggerFactory.getLogger(NbaTemporalWorker.class);
    static final ObjectMapper M = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static final String TASK_QUEUE = env("NBA_TASK_QUEUE", "nba-channel-actions");

    // actionId -> its channels, cached off the nba.definitions stream so a per-action hard-completion fact
    // (nba.completion.{actionId}, channel-agnostic) can fan out hardComplete to each channel-workflow.
    static final java.util.Map<String, java.util.Set<String>> ACTION_CHANNELS = new java.util.concurrent.ConcurrentHashMap<>();

    public static void main(String[] args) {
        String target    = env("NBA_TEMPORAL", "nba-temporal:7233");
        String namespace = env("NBA_TEMPORAL_NS", "default");
        String bootstrap = env("NBA_BOOTSTRAP", "nba-redpanda:9092");
        String actTopic    = env("NBA_ACT_TOPIC", "nba.activations");
        String memberFacts = env("NBA_MEMBER_FACTS", "nba.member.facts");
        String factsTopic  = env("NBA_FACTS_TOPIC", "nba.facts");
        String defsTopic   = env("NBA_DEFINITIONS_TOPIC", "nba.definitions");
        String dlq         = env("NBA_DLQ", "nba.activations.dlq");
        String dispDlq     = env("NBA_DISP_DLQ", "nba.dlq.temporal-disposition");
        String bridgeDlq   = env("NBA_BRIDGE_DLQ", "nba.dlq.temporal-bridge");
        long   debounce    = Long.parseLong(env("NBA_DEBOUNCE_SECONDS", "60"));

        // Channel throttle gate — exact cap enforcement. Fed from nba.definitions (the same broadcast
        // the rules engine uses): THROTTLE:{channel} = the lake's level, CHANNEL_RULE = the cap.
        ThrottleGate throttle = new ThrottleGate();

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps(bootstrap));

        // The state machine writes its two sinks (member.facts, activations) to a Postgres
        // outbox; Debezium publishes them to Kafka. No direct Kafka produce from the workflow.
        com.zaxxer.hikari.HikariConfig hc = new com.zaxxer.hikari.HikariConfig();
        hc.setJdbcUrl("jdbc:postgresql://" + env("NBA_PG_HOST", "ais-nba-postgres") + ":5432/" + env("NBA_PG_DB", "actionlib"));
        hc.setUsername(env("NBA_PG_USER", "nba"));
        hc.setPassword(env("NBA_PG_PASSWORD", "nba"));
        hc.setMaximumPoolSize(5);
        javax.sql.DataSource ds = new com.zaxxer.hikari.HikariDataSource(hc);

        // channel_touch: a MONOTONIC per-(member, channel) send counter. The send activity bumps it atomically at the
        // single DISPATCH point (after debounce/suppress/throttle settle) and picks the channel's first/second/third-touch
        // template by it. Per-channel (regardless of action) — which is why it can't live in a per-action workflow's state.
        try (java.sql.Connection c = ds.getConnection(); java.sql.Statement st = c.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS channel_touch (nba_id text NOT NULL, channel text NOT NULL, "
                     + "n bigint NOT NULL DEFAULT 0, PRIMARY KEY (nba_id, channel))");
            log.info("channel_touch table ready");
        } catch (Exception e) { log.warn("channel_touch schema failed", e); }

        WorkflowServiceStubs service = WorkflowServiceStubs.newServiceStubs(
                WorkflowServiceStubsOptions.newBuilder().setTarget(target).build());
        WorkflowClient client = WorkflowClient.newInstance(service,
                WorkflowClientOptions.newBuilder().setNamespace(namespace).build());

        WorkerFactory factory = WorkerFactory.newInstance(client);
        Worker worker = factory.newWorker(TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(ChannelActionWorkflowImpl.class, BatchOrchestratorWorkflowImpl.class, SuppressionWorkflowImpl.class);
        worker.registerActivitiesImplementations(new ActionActivitiesImpl(ds, memberFacts, actTopic, throttle, client));
        factory.start();
        log.info("worker up: target=" + target + " ns=" + namespace
                + " tq=" + TASK_QUEUE + " debounce=" + debounce + "s");

        // Prometheus /metrics on a side port (the worker has no HTTP surface otherwise).
        Metrics.serve(Integer.parseInt(env("NBA_METRICS_PORT", "9409")));

        // disposition consumer: the action layer's verdict is a member fact (nba.disposition.*);
        // route it back to the waiting workflow.
        Thread disp = new Thread(() -> runDispositionConsumer(bootstrap, memberFacts, dispDlq, client), "dispositions");
        disp.setDaemon(true);
        disp.start();

        // throttle feed: tail nba.definitions (own group = broadcast, replay on init) to keep the
        // gate's per-channel level + cap current — the same stream the rules engine reads.
        Thread thr = new Thread(() -> runThrottleFeed(bootstrap, defsTopic, throttle), "throttle-feed");
        thr.setDaemon(true);
        thr.start();

        // Operator ACTION_SUPPRESS is an ACTION-LEVEL priority signal: ride the low-volume nba.definitions broadcast
        // (own group) so a suppress fires immediately, NOT stuck behind the member.facts CREATE backlog the bridge
        // drains. Idempotent SuppressionWorkflow start dedups the per-instance broadcast copies.
        Thread sup = new Thread(() -> runSuppressFeed(bootstrap, defsTopic, client), "suppress-feed");
        sup.setDaemon(true);
        sup.start();

        // The bridge now consumes ROUTER DECISIONS off nba.member.facts (kind=router), not nba.activations.
        // nba.activations carries only the state machine's own DISPATCH/CANCEL (emitted via the outbox, read
        // by the action layer). Router CREATE/SUPPRESS are member-level facts, so they ride member.facts.
        runBridge(bootstrap, memberFacts, bridgeDlq, client, debounce);
    }

    // member.facts (key nba.disposition.{actionId}.{channel}) -> signal the matching workflow
    static void runDispositionConsumer(String bootstrap, String memberFacts, String dlq, WorkflowClient client) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(bootstrap, "nba-temporal-disp"));
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps(bootstrap));
        consumer.subscribe(List.of(memberFacts));
        log.info("disposition consumer up: in=" + memberFacts + " (nba.disposition.*)");
        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : recs) {
                // Header-based filter: only disposition (state walk) + completion (hard goal) facts.
                org.apache.kafka.common.header.Header kind = r.headers().lastHeader("kind");
                String kindStr = kind == null ? "" : new String(kind.value(), java.nio.charset.StandardCharsets.UTF_8);
                if (!"disposition".equals(kindStr) && !"completion".equals(kindStr)) continue;
                try {
                    maybeInjectFault(r.value());                 // test hook: force a DLQ on marked messages
                    com.fasterxml.jackson.databind.JsonNode d = M.readTree(r.value());
                    if ("completion".equals(kindStr)) {          // nba.completion.{actionId} -> hardComplete EACH channel-workflow
                        String nbaId = d.path("nbaId").asText(""), actionId = d.path("actionId").asText("");
                        String ccorr = d.path("correlationId").asText("");
                        if (nbaId.isEmpty() || actionId.isEmpty()) continue;
                        for (String ch : ACTION_CHANNELS.getOrDefault(actionId, java.util.Set.of())) {
                            String wfId = "nba-ca:" + nbaId + ":" + actionId + ":" + ch;
                            try { client.newWorkflowStub(ChannelActionWorkflow.class, wfId).hardComplete(ccorr.isEmpty() ? null : ccorr); }
                            catch (Exception ignore) { /* not running */ }
                        }
                        log.info("hardComplete " + nbaId + ":" + actionId + " -> " + ACTION_CHANNELS.getOrDefault(actionId, java.util.Set.of()));
                        continue;
                    }
                    String status = d.path("state").asText(d.path("value").asText(""));   // canonical DELIVERY state
                    // trackingId = workflowId + "|" + correlationId. Deconstruct it to route the
                    // disposition straight to the workflow — no nbaId/key derivation, no lookup.
                    String trackingId = d.path("trackingId").asText("");
                    String[] wc = splitTrackingId(trackingId);
                    if (status.isEmpty() || wc == null) continue;
                    String wfId = wc[0];
                    String corr = wc[1];
                    try {
                        client.newWorkflowStub(ChannelActionWorkflow.class, wfId).disposition(status, corr);
                        log.info("disposition -> " + wfId + " = " + status);
                    } catch (Exception ignore) {
                        log.info("disposition no-op (not running) " + wfId);
                    }
                } catch (Exception e) {
                    log.warn("disp DLQ <- " + e + " :: " + r.value());
                    producer.send(dlqEnvelope(dlq, "temporal-disposition", r, String.valueOf(e)));
                }
            }
            if (!recs.isEmpty()) consumer.commitSync();
        }
    }

    // nba.definitions -> keep the throttle gate's level + cap current. Own consumer group so EVERY
    // worker instance gets every record (broadcast) and replays the compacted topic on init.
    static void runThrottleFeed(String bootstrap, String defsTopic, ThrottleGate throttle) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                consumerProps(bootstrap, "nba-temporal-throttle-" + java.util.UUID.randomUUID()));
        consumer.subscribe(List.of(defsTopic));
        log.info("throttle feed up: " + defsTopic);
        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : recs) {
                try {
                    String key = r.key() == null ? "" : r.key();
                    if (key.startsWith("THROTTLE:")) {                 // THROTTLE:{ch}.{metric} = lake level (daily/rate)
                        if (r.value() == null) continue;
                        throttle.onFact(M.readTree(r.value()));        // channel + metric parsed from the fact key
                    } else if (key.startsWith("CHANNEL_RULE:")) {      // the caps (if it references nba.throttle.*)
                        String ruleId = key.substring("CHANNEL_RULE:".length());
                        boolean removed = r.value() == null;
                        throttle.onChannelRule(ruleId, removed ? null : M.readTree(r.value()), removed);
                    } else if (key.startsWith("ACTION:")) {            // cache action -> channels (for hard-completion fan-out)
                        String actionId = key.substring("ACTION:".length());
                        if (r.value() == null) { ACTION_CHANNELS.remove(actionId); continue; }
                        java.util.Set<String> chs = new java.util.HashSet<>();
                        com.fasterxml.jackson.databind.JsonNode chans = M.readTree(r.value()).get("channels");
                        if (chans != null && chans.isArray()) for (com.fasterxml.jackson.databind.JsonNode c : chans) {
                            String ch = c.path("channel").asText(""); if (!ch.isEmpty()) chs.add(ch);
                        }
                        ACTION_CHANNELS.put(actionId, chs);
                    }
                } catch (Exception e) {
                    log.warn("throttle feed skip: " + e);
                }
            }
        }
    }

    // nba.definitions -> operator ACTION_SUPPRESS flags are ACTION-LEVEL PRIORITY signals. They ride the low-volume
    // definitions broadcast (own consumer group -> EVERY worker instance sees every flag), so an operator suppress
    // fires immediately instead of queuing behind the member.facts CREATE backlog the member-keyed bridge drains.
    // On a fresh suppress (value=true, eventTs after this worker started -> skips the compacted-topic replay), fan the
    // cancel out to the in-flight workflows via the SuppressionWorkflow. The idempotent start (target+eventTs,
    // USE_EXISTING) collapses the N broadcast copies across worker instances into ONE batch.
    static void runSuppressFeed(String bootstrap, String defsTopic, WorkflowClient client) {
        final long startTime = System.currentTimeMillis();
        final java.util.Map<String, Long> fired = new java.util.concurrent.ConcurrentHashMap<>();  // target -> last eventTs fired
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                consumerProps(bootstrap, "nba-temporal-suppress-" + java.util.UUID.randomUUID()));
        consumer.subscribe(List.of(defsTopic));
        log.info("suppress feed up: " + defsTopic + " (ACTION_SUPPRESS -> SuppressionWorkflow, priority)");
        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : recs) {
                try {
                    String key = r.key() == null ? "" : r.key();
                    if (!key.startsWith("ACTION_SUPPRESS:") || r.value() == null) continue;
                    var v = M.readTree(r.value());
                    String target = key.substring("ACTION_SUPPRESS:".length());
                    Long last = fired.get(target);
                    long eventTs = v.path("eventTs").asLong(0);
                    if (!shouldFanSuppress(v.path("value").asBoolean(false), eventTs, startTime, last == null ? 0L : last))
                        continue;
                    fired.put(target, eventTs);
                    startSuppression(client, v.path("actionId").asText(""), v.path("channel").asText(""), eventTs, "defs");
                } catch (Exception e) {
                    log.warn("suppress feed skip: " + e);
                }
            }
        }
    }

    /** Should an ACTION_SUPPRESS definitions flag trigger a fresh in-flight fan-out? Only a suppress (value=true)
     *  whose eventTs post-dates this worker's start (so the compacted-topic replay of pre-existing flags is skipped)
     *  and that hasn't already fired for this target (redelivery dedup). Pure -> unit-testable. */
    static boolean shouldFanSuppress(boolean suppressed, long eventTs, long startTime, long lastFiredEventTs) {
        return suppressed && eventTs > startTime && eventTs != lastFiredEventTs;
    }

    // Fan an operator suppression out to every in-flight workflow for the action via the SuppressionWorkflow (a
    // Temporal batch op). Idempotent workflowId (target+eventTs, USE_EXISTING) -> concurrent/duplicate triggers
    // (e.g. the defs broadcast reaching N worker instances) collapse to one batch.
    static void startSuppression(WorkflowClient client, String actionId, String channel, long eventTs, String via) {
        if (actionId == null || actionId.isEmpty()) return;
        String target = (channel == null || channel.isEmpty()) ? actionId : actionId + "." + channel;
        String swfId = "nba-suppress:" + target + ":" + eventTs;
        WorkflowOptions sopts = WorkflowOptions.newBuilder()
                .setTaskQueue(TASK_QUEUE).setWorkflowId(swfId)
                .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)
                .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
                .build();
        SuppressionWorkflow swf = client.newWorkflowStub(SuppressionWorkflow.class, sopts);
        WorkflowClient.start(swf::suppress, actionId, channel == null ? "" : channel);
        log.info("suppress-action (" + via + ") -> " + swfId);
    }

    // nba.member.facts (kind=router) -> start/attach a ChannelAction workflow per (member, action, channel).
    // Router decisions ride member.facts now; we filter to kind=router by HEADER (no body deserialize) and
    // ignore every other member fact (dispositions, states, scores) — those are for the snapshot/ML paths.
    static void runBridge(String bootstrap, String inTopic, String dlq,
                          WorkflowClient client, long debounce) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(bootstrap, "nba-temporal"));
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps(bootstrap));
        consumer.subscribe(List.of(inTopic));
        // Workflow STARTS are blocking gRPC round-trips (~tens of ms each). Issuing them serially caps the bridge
        // at ~12/s regardless of Temporal's real capacity — so fan the per-record starts across a pool. Starts are
        // idempotent (deterministic workflowId + USE_EXISTING conflict policy), so concurrency is safe; we still
        // await the whole batch before commitSync so at-least-once delivery is preserved. Default 1 = legacy serial.
        int concurrency = Integer.parseInt(System.getenv().getOrDefault("NBA_BRIDGE_CONCURRENCY", "1"));
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(Math.max(1, concurrency));
        log.info("bridge up: in=" + inTopic + " (kind=router) concurrency=" + concurrency);

        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
            java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
            for (ConsumerRecord<String, String> r : recs) {
                // Header filter: only router DECISIONS drive the bridge; skip all other member facts cheaply.
                org.apache.kafka.common.header.Header kind = r.headers().lastHeader("kind");
                if (kind == null || !"router".equals(new String(kind.value(), java.nio.charset.StandardCharsets.UTF_8)))
                    continue;
                futures.add(pool.submit(() -> handleBridgeRecord(r, client, producer, dlq, debounce)));
            }
            for (java.util.concurrent.Future<?> f : futures) {
                try { f.get(); } catch (Exception ignore) { /* per-record failures already DLQ'd inside */ }
            }
            if (!recs.isEmpty()) consumer.commitSync();
        }
    }

    // One router-decision record -> start/signal the matching workflow. Extracted from runBridge so the blocking
    // WorkflowClient.start gRPC (the real throughput serializer) can be fanned across a pool. `continue` -> `return`.
    static void handleBridgeRecord(ConsumerRecord<String, String> r, WorkflowClient client,
                                   KafkaProducer<String, String> producer, String dlq, long debounce) {
                try {
                    maybeInjectFault(r.value());                 // test hook: force a DLQ on marked messages
                    Activation act = M.readValue(r.value(), Activation.class);

                    // Operator suppression now rides the ACTION_SUPPRESS flag on the nba.definitions broadcast
                    // (runSuppressFeed) — an ACTION-LEVEL signal on a low-volume, un-backlogged channel, so it fires
                    // immediately instead of queuing behind this member.facts CREATE backlog. This member.facts path
                    // is retained only as a defensive fallback (idempotent start dedups if both ever fire).
                    if ("SUPPRESS_ACTION".equals(act.op)) {
                        startSuppression(client, act.actionId, act.channel, act.eventTs, "router");
                        return;
                    }

                    if (act.nbaId == null || act.channel == null) return;

                    // BATCH create (actions[]) -> start/signal the BatchOrchestrator for this (member, channel).
                    // signalWithStart: starts orchestrate() if new, and always delivers the latest batch.
                    if ("CREATE_BATCH".equals(act.op)) {              // the router's explicit batch flow (actions[])
                        String batchId = "nba-batch:" + act.batchSlug();
                        WorkflowOptions bopts = WorkflowOptions.newBuilder()
                                .setTaskQueue(TASK_QUEUE).setWorkflowId(batchId)
                                .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)
                                .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
                                .build();
                        BatchOrchestratorWorkflow bwf = client.newWorkflowStub(BatchOrchestratorWorkflow.class, bopts);
                        io.temporal.client.BatchRequest req = client.newSignalWithStartRequest();
                        req.add(bwf::orchestrate, debounce);
                        req.add(bwf::updateBatch, act);
                        client.signalWithStart(req);
                        log.info("batch CREATE " + batchId + " x" + act.actions.size());
                        return;
                    }
                    if (act.actionId == null) return;   // single-action ops below need an actionId
                    String wfId = "nba-ca:" + act.slug();

                    if ("SUPPRESS".equals(act.op)) {
                        // Signal the running workflow to cancel before dispatch. If it's already
                        // closed or never ran, the signal target is gone -> no-op.
                        try {
                            client.newWorkflowStub(ChannelActionWorkflow.class, wfId).suppress();
                            log.info("suppress " + wfId);
                        } catch (Exception ignore) {
                            log.info("suppress no-op (not running) " + wfId);
                        }
                        return;
                    }
                    // Router-bridged completion evaluations -> drive SOFT_COMPLETED / HARD_COMPLETED (idempotent).
                    // The router carries the real channel on each (it bridges off the ChannelAction in eligible[]
                    // or inFlight[]), so this targets the one channel-workflow directly.
                    if ("SOFT_COMPLETE".equals(act.op) || "HARD_COMPLETE".equals(act.op)) {
                        try {
                            ChannelActionWorkflow wf = client.newWorkflowStub(ChannelActionWorkflow.class, wfId);
                            if ("SOFT_COMPLETE".equals(act.op)) wf.softComplete(act.correlationId);
                            else wf.hardComplete(act.correlationId);
                            log.info("" + act.op + " -> " + wfId);
                        } catch (Exception ignore) {
                            log.info("" + act.op + " no-op (not running) " + wfId);
                        }
                        return;
                    }
                    if (!"CREATE".equals(act.op)) return;

                    WorkflowOptions opts = WorkflowOptions.newBuilder()
                            .setTaskQueue(TASK_QUEUE)
                            .setWorkflowId(wfId)
                            .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)
                            .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
                            .setTypedSearchAttributes(saFor(act.actionId, act.channel))   // findable by the suppression batch op
                            .build();
                    ChannelActionWorkflow wf = client.newWorkflowStub(ChannelActionWorkflow.class, opts);
                    WorkflowExecution exec = WorkflowClient.start(wf::activate, act, debounce);
                    log.info("activate " + wfId + " runId=" + exec.getRunId());
                } catch (Exception e) {
                    log.warn("DLQ <- " + e + " :: " + r.value());
                    producer.send(dlqEnvelope(dlq, "temporal-bridge", r, String.valueOf(e)));
                }
    }

    static Properties consumerProps(String bootstrap, String group) {
        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // member.facts is a busy channel — pull whole batches per poll (we header-filter cheaply and commit
        // once per batch), never one message at a time.
        cp.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        return cp;
    }

    static Properties producerProps(String bootstrap) {
        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pp.put(ProducerConfig.ACKS_CONFIG, "all");
        return pp;
    }

    /** Typed search attributes set at workflow START (NOT in the body — that would be a non-deterministic command),
     *  so the SuppressionWorkflow's batch op can find every running workflow for an action via Visibility. */
    static io.temporal.common.SearchAttributes saFor(String actionId, String channel) {
        return io.temporal.common.SearchAttributes.newBuilder()
                .set(io.temporal.common.SearchAttributeKey.forKeyword("NbaActionId"), actionId == null ? "" : actionId)
                .set(io.temporal.common.SearchAttributeKey.forKeyword("NbaChannel"), channel == null ? "" : channel)
                .build();
    }

    /**
     * Deconstruct a disposition trackingId ("workflowId|correlationId") into {wfId, corr} on the LAST '|'
     * (correlationId is bar-free; a workflowId could in theory contain one). Returns null when there is no
     * usable split — no '|' at all, or a leading '|' (empty workflowId). An empty trailing correlationId is
     * permitted (the workflow ignores it) so a stray trailing-'|' still routes to the workflow.
     */
    static String[] splitTrackingId(String trackingId) {
        if (trackingId == null) return null;
        int bar = trackingId.lastIndexOf('|');
        if (bar <= 0) return null;
        return new String[]{ trackingId.substring(0, bar), trackingId.substring(bar + 1) };
    }

    /** Standard DLQ envelope: source coordinates + raw value + error (replayable to the source topic). */
    static ProducerRecord<String, String> dlqEnvelope(String dlqTopic, String consumer, ConsumerRecord<String, String> r, String error) {
        try {
            ObjectNode env = M.createObjectNode();
            env.put("consumer", consumer); env.put("topic", r.topic());
            env.put("partition", r.partition()); env.put("offset", r.offset());
            if (r.key() != null) env.put("key", r.key()); else env.putNull("key");
            env.put("value", r.value());
            ObjectNode h = env.putObject("headers");
            for (org.apache.kafka.common.header.Header hd : r.headers())
                if (hd.value() != null) h.put(hd.key(), new String(hd.value(), java.nio.charset.StandardCharsets.UTF_8));
            env.put("error", error); env.put("dlqTs", System.currentTimeMillis());
            return new ProducerRecord<>(dlqTopic, r.key(), M.writeValueAsString(env));
        } catch (Exception e) { return new ProducerRecord<>(dlqTopic, r.key(), r.value()); }
    }

    // Test hook: when NBA_FAULT_INJECT is set, any record whose raw value CONTAINS that substring throws —
    // exercises this consumer's DLQ + replay path on demand. Empty (default) = no-op, no prod impact.
    static final String FAULT_INJECT = env("NBA_FAULT_INJECT", "");
    static void maybeInjectFault(String raw) {
        if (!FAULT_INJECT.isEmpty() && raw != null && raw.contains(FAULT_INJECT))
            throw new RuntimeException("injected fault (NBA_FAULT_INJECT=" + FAULT_INJECT + ")");
    }

    static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? d : v;
    }
}
