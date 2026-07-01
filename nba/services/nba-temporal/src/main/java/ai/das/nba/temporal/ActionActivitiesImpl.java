package ai.das.nba.temporal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.temporal.api.batch.v1.BatchOperationSignal;
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy;
import io.temporal.api.enums.v1.WorkflowIdReusePolicy;
import io.temporal.api.workflowservice.v1.StartBatchOperationRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Activities run OUTSIDE the workflow sandbox — real I/O is allowed here.
 *
 * The state machine NEVER produces to Kafka directly. Both sinks go through a Postgres
 * transactional outbox (one table per topic); Debezium + Kafka Connect CDC-tail the tables
 * and publish to Kafka via the Outbox Event Router SMT (aggregateid -> key, payload -> value,
 * kind -> "kind" header, aggregatetype -> topic).
 *
 *  emitState()      -> outbox_member_facts -> nba.member.facts  (kind=state; the snapshot
 *                      builder re-routes it onto the firehose for the ML feature store)
 *  emitActivation() -> outbox_activations  -> nba.activations   (DISPATCH/CANCEL for the layer)
 */
public class ActionActivitiesImpl implements ActionActivities {
    private static final Logger log = LoggerFactory.getLogger(ActionActivitiesImpl.class);
    static final ObjectMapper M = new ObjectMapper();

    private final DataSource ds;
    private final String memberFactsTopic;
    private final String activationsTopic;
    private final ThrottleGate throttle;
    private final WorkflowClient client;   // to start per-action tracker workflows for a batch

    public ActionActivitiesImpl(DataSource ds, String memberFactsTopic, String activationsTopic, ThrottleGate throttle, WorkflowClient client) {
        this.ds = ds;
        this.memberFactsTopic = memberFactsTopic;
        this.activationsTopic = activationsTopic;
        this.throttle = throttle;
        this.client = client;
    }

    /** Operator suppression fan-out — a server-side Temporal Batch Operation that signals operatorSuppress() to every
     *  RUNNING ChannelActionWorkflow matching the action (+channel) via the NbaActionId/NbaChannel search attributes.
     *  Temporal does the fan-out across the namespace, so this scales to millions without us listing/signalling each. */
    /** The Temporal Visibility query that selects the RUNNING ChannelActionWorkflows to fan the operator suppress to:
     *  every RUNNING workflow for the action, narrowed to one channel when given. Extracted as a pure static so the
     *  scoping (action-wide vs action-channel) + the ExecutionStatus='Running' guard are unit-testable. */
    static String suppressQuery(String actionId, String channel) {
        return "NbaActionId='" + actionId + "' AND ExecutionStatus='Running'"
                + ((channel == null || channel.isEmpty()) ? "" : " AND NbaChannel='" + channel + "'");
    }

    @Override
    public String suppressMatching(String actionId, String channel) {
        String jobId = "nba-suppress-" + java.util.UUID.randomUUID();
        String query = suppressQuery(actionId, channel);
        StartBatchOperationRequest req = StartBatchOperationRequest.newBuilder()
                .setNamespace(client.getOptions().getNamespace())
                .setJobId(jobId)
                .setReason("operator suppress " + actionId + ((channel == null || channel.isEmpty()) ? "" : "." + channel))
                .setVisibilityQuery(query)
                .setSignalOperation(BatchOperationSignal.newBuilder()
                        .setSignal("operatorSuppress").setIdentity("nba-suppression").build())
                .build();
        client.getWorkflowServiceStubs().blockingStub().startBatchOperation(req);
        log.info("suppress batch " + jobId + " query=[" + query + "]");
        Metrics.counter("nba_suppression_fanout_total").increment();
        return jobId;
    }

    // ── PER-TOUCH TEMPLATE: the action catalog (same actionlib DB) holds per-channel touchKeys = [first, second, third]
    //    template ids. Cached 60s. At the DISPATCH send point we bump the MONOTONIC channel_touch counter atomically and
    //    pick touchKeys[min(n,len)-1], overriding the variant-selected contentKey. Per-channel, regardless of action —
    //    which is why it can't live in the per-(member,action,channel) workflow's own state.
    private final java.util.Map<String, JsonNode> CATALOG = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.Set<String> touchChannels = java.util.concurrent.ConcurrentHashMap.newKeySet();  // channels with touchKeys on ANY action
    private volatile long catalogAt = 0;

    private synchronized void refreshCatalog() {
        if (System.currentTimeMillis() - catalogAt < 60_000) return;
        try (Connection c = ds.getConnection(); java.sql.Statement st = c.createStatement();
             java.sql.ResultSet rs = st.executeQuery("SELECT id, doc FROM action")) {
            java.util.Map<String, JsonNode> m = new java.util.concurrent.ConcurrentHashMap<>();
            java.util.Set<String> chans = java.util.concurrent.ConcurrentHashMap.newKeySet();
            while (rs.next()) {
                JsonNode doc = M.readTree(rs.getString(2)); m.put(rs.getString(1), doc);
                for (JsonNode ch : doc.path("channels")) {
                    JsonNode tk = ch.get("touchKeys");
                    if (tk != null && tk.isArray() && tk.size() > 0) chans.add(ch.path("channel").asText());
                }
            }
            CATALOG.clear(); CATALOG.putAll(m); touchChannels.clear(); touchChannels.addAll(chans);
            catalogAt = System.currentTimeMillis();
        } catch (Exception e) { log.warn("catalog refresh failed", e); }
    }

    /** The channel's per-touch template ids (channels[].touchKeys) from the catalog, or null when not configured. */
    private java.util.List<String> touchKeys(String actionId, String channel) {
        refreshCatalog();
        JsonNode doc = CATALOG.get(actionId);
        if (doc == null) return null;
        for (JsonNode ch : doc.path("channels")) {
            if (channel.equals(ch.path("channel").asText())) {
                JsonNode tk = ch.get("touchKeys");
                if (tk != null && tk.isArray() && tk.size() > 0) {
                    java.util.List<String> out = new java.util.ArrayList<>();
                    for (JsonNode k : tk) out.add(k.asText());
                    return out;
                }
                return null;                                       // channel found, no touchKeys -> use the variant contentKey
            }
        }
        return null;
    }

    /** At the SEND point: if the channel has per-touch templates, atomically bump the MONOTONIC (member, channel) counter
     *  and return touchKeys[min(n,len)-1] (caps at the last configured, never resets). Else the passed contentKey. The
     *  atomic UPSERT ... RETURNING serializes even concurrent batch sends, so each gets a distinct, sequential touch. */
    private String touchTemplate(String nbaId, String channel, String actionId, String fallback) {
        refreshCatalog();
        if (channel == null || !touchChannels.contains(channel)) return fallback;   // no touch templates on this channel -> no counting
        long n = 0;                                                                  // count EVERY send on the channel (any action)
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "INSERT INTO channel_touch(nba_id, channel, n) VALUES (?,?,1) "
               + "ON CONFLICT (nba_id, channel) DO UPDATE SET n = channel_touch.n + 1 RETURNING n")) {
            ps.setString(1, nbaId == null ? "" : nbaId); ps.setString(2, channel == null ? "" : channel);
            try (java.sql.ResultSet rs = ps.executeQuery()) { if (rs.next()) n = rs.getLong(1); }
        } catch (Exception e) { log.warn("channel touch counter failed (" + channel + "): " + e); return fallback; }
        java.util.List<String> tk = touchKeys(actionId, channel);                   // THIS action's templates (may be absent even when
        if (tk == null || tk.isEmpty()) return fallback;                            // the channel has them for OTHER actions — still counted)
        return tk.get((int) Math.min(n, tk.size()) - 1);
    }

    @Override
    public void emitBatchDispatch(Activation batch) {
        ObjectNode a = M.createObjectNode();
        a.put("op", "DISPATCH");
        a.put("entityType", batch.entityType);
        a.put("entityId", batch.entityId);
        a.put("nbaId", batch.nbaId);
        a.put("memberId", batch.member());      // the action layer sends to the MEMBER
        a.put("channel", batch.channel);
        a.put("correlationId", batch.correlationId);
        a.put("source", "batch-orchestrator");
        a.put("eventTs", System.currentTimeMillis());
        ArrayNode actions = a.putArray("actions");
        for (Activation.BatchAction ba : batch.actions) {
            ObjectNode o = actions.addObject();
            o.put("actionId", ba.actionId);
            // per-touch template per batched action (each bumps the channel counter -> escalating touches within a batch)
            o.put("contentKey", touchTemplate(batch.nbaId, batch.channel, ba.actionId, ba.contentKey));
            o.put("name", ba.name);
            o.put("ttlSeconds", ba.ttlSeconds);
            o.put("score", ba.score);
            // per-action trackingId so the action-layer's per-action disposition routes to the child wf
            o.put("trackingId", "nba-ca:" + batch.nbaId + ":" + ba.actionId + ":" + batch.channel + "|" + batch.correlationId);
        }
        String key = batch.nbaId + ":" + batch.channel + ":batch:sm";   // distinct from router CREATE keys
        outbox("outbox_activations", activationsTopic, key, null, write(a));
        // NOTE: IN_PROCESS is emitted by each tracking CHILD (ChannelActionWorkflowImpl, preDispatched branch),
        // not here — the child owns its own state lifecycle (IN_PROCESS -> PRESENTED -> ...).
        log.info("batch DISPATCH " + batch.batchSlug() + " x" + batch.actions.size());
    }

    @Override
    public void startTracker(Activation childAct) {
        String wfId = "nba-ca:" + childAct.slug();
        WorkflowOptions opts = WorkflowOptions.newBuilder()
                .setTaskQueue(NbaTemporalWorker.TASK_QUEUE)
                .setWorkflowId(wfId)
                .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)
                .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)
                .setTypedSearchAttributes(NbaTemporalWorker.saFor(childAct.actionId, childAct.channel))   // findable by the suppression batch op
                .build();
        ChannelActionWorkflow wf = client.newWorkflowStub(ChannelActionWorkflow.class, opts);
        try { WorkflowClient.start(wf::activate, childAct, 0L); log.info("tracker " + wfId); }
        catch (Exception e) { log.info("tracker exists " + wfId); }
    }

    // Sibling states where the outcome is still UNCERTAIN — hold and recheck (it may bounce/free or progress).
    static final java.util.Set<String> WAIT_ON = java.util.Set.of("IN_PROCESS", "SUPPRESSING");

    /** Debounce dedup verdict: discover the OTHER in-flight workflows for this member via Temporal's own
     *  visibility (WorkflowId STARTS_WITH nba-ca:{nbaId}:, Running) and query each LIVE for its state+score.
     *  Returns "LOSE" (self-DEBOUNCE), "WAIT" (an uncertain sibling — hold another window + recheck), or
     *  "PROCEED" (I'm the winner — send). Discovery is eventually-consistent (~1-2s), immaterial at the prod
     *  60s window; the per-sibling query is live/fresh. The system also self-corrects via re-evaluation. */
    @Override
    public String debounceLost(Activation act) {
        String myId = "nba-ca:" + act.slug();
        double mine = act.score;
        boolean wait = false;
        String query = "WorkflowId STARTS_WITH 'nba-ca:" + act.nbaId + ":' AND ExecutionStatus='Running'";
        try {
            java.util.List<String> sibs = client.listExecutions(query)
                    .map(m -> m.getExecution().getWorkflowId())
                    .filter(id -> !id.equals(myId))
                    .collect(java.util.stream.Collectors.toList());
            for (String sibId : sibs) {
                String info;
                try { info = client.newUntypedWorkflowStub(sibId).query("debounceInfo", String.class); }
                catch (Exception e) { continue; }   // raced to terminal / not queryable -> gone (non-blocking)
                int bar = info.lastIndexOf('|');
                String st = bar < 0 ? info : info.substring(0, bar);
                double sc; try { sc = Double.parseDouble(info.substring(bar + 1)); } catch (Exception e) { sc = 0; }
                // a touch already REACHED the member (delivered / declined) -> stand down. SOFT_COMPLETED is
                // engagement (green light, non-blocking); FAILED/terminals aren't Running so they never appear.
                if ("PRESENTED".equals(st) || "DECLINED".equals(st)) {
                    log.info("debounce LOSE " + act.slug() + " <- sibling " + st + " (" + sibId + ")");
                    return "LOSE";
                }
                // both still racing in the debounce window -> highest score sends; tiebreak on smaller id.
                if ("CREATED".equals(st) && (sc > mine || (sc == mine && sibId.compareTo(myId) < 0))) {
                    log.info("debounce LOSE " + act.slug() + " (" + mine + ") <- higher CREATED sibling " + sibId + " (" + sc + ")");
                    return "LOSE";
                }
                // dispatched-but-unconfirmed (might bounce -> free) or mid-cancel -> uncertain, recheck later.
                if (WAIT_ON.contains(st)) wait = true;
            }
        } catch (Exception e) {
            log.warn("debounceLost query failed (fail-open=PROCEED) " + act.slug() + ": " + e);
            return "PROCEED";
        }
        return wait ? "WAIT" : "PROCEED";
    }

    @Override
    public String throttleGate(Activation act) {
        String decision = throttle.admit(act.channel, System.currentTimeMillis());
        if (!ThrottleGate.SEND.equals(decision)) log.info("throttle " + decision + " " + act.slug() + " (channel " + act.channel + ")");
        return decision;
    }

    @Override
    public boolean operatorSuppressed(Activation act) {
        return NbaTemporalWorker.isOperatorSuppressed(act.actionId, act.channel);
    }

    @Override
    public void throttleEnterBacklog(Activation act) { throttle.enterBacklog(act.channel); }

    @Override
    public void throttleExitBacklog(Activation act) { throttle.exitBacklog(act.channel); }

    @Override
    public void emitState(Activation act, String state) { emitStateReason(act, state, null); }

    @Override
    public void emitStateReason(Activation act, String state, String reason) {
        String key = "nba.actionstate." + act.actionId + "." + act.channel;
        ObjectNode fact = M.createObjectNode();
        fact.put("entityType", act.entityType);
        fact.put("entityId", act.entityId);
        fact.put("nbaId", act.nbaId);
        fact.put("key", key);
        fact.put("value", state);
        fact.put("valueType", "STRING");
        fact.put("eventTs", System.currentTimeMillis());
        fact.put("source", "temporal");
        if (reason != null) fact.put("reason", reason);
        // keyed by memberId (entityType:entityId) so a member's facts co-locate on one partition; the fact
        // key (with the channel) rides the body, where the snapshot-builder parses it.
        String kafkaKey = act.entityType + ":" + act.entityId;
        // kind=throttle-suppress lets the snapshot-builder route this to the definitions topic by HEADER
        // (channel parsed from the fact key in the body) with no extra lookup; otherwise a normal state fact.
        String kind = "throttle".equals(reason) ? "throttle-suppress" : "state";
        outbox("outbox_member_facts", memberFactsTopic, kafkaKey, kind, write(fact));
        log.info("state " + act.slug() + " = " + state + (reason != null ? " (" + reason + ")" : ""));
        Metrics.counter("nba_state_transitions_total", "state", state).increment();
    }

    @Override
    public void emitActivation(Activation act, String op) {
        ObjectNode a = M.createObjectNode();
        a.put("op", op);                          // DISPATCH | CANCEL (action layer filters these)
        a.put("entityType", act.entityType);
        // entityId + nbaId are the MEMBER KEY the medallion needs to attribute this activation to a member (silver_
        // activations -> journey reconstruction -> RL training). The action layer ignores them (it sends to memberId),
        // but the batch path (emitBatchDispatch) already carries them — without them here, every single-action DISPATCH
        // lands unattributed in silver and is lost to the RL journey set.
        a.put("entityId", act.entityId);
        a.put("nbaId", act.nbaId);
        a.put("memberId", act.member());          // the action layer sends to the MEMBER — never nbaId
        a.put("actionId", act.actionId);
        a.put("channel", act.channel);
        a.put("name", act.name);
        // per-touch template: only on a real DISPATCH (the settled send point), so debounced/suppressed never escalate.
        a.put("contentKey", "DISPATCH".equals(op) ? touchTemplate(act.nbaId, act.channel, act.actionId, act.contentKey) : act.contentKey);
        a.put("ttlSeconds", act.ttlSeconds);
        a.put("score", act.score);
        a.put("correlationId", act.correlationId);
        a.put("trackingId", "nba-ca:" + act.slug() + "|" + act.correlationId);
        a.put("source", "state-machine");
        a.put("eventTs", System.currentTimeMillis());
        // keyed per (member,action,channel)+":sm" so it doesn't compact against the router's CREATE/SUPPRESS
        String key = act.nbaId + ":" + act.actionId + ":" + act.channel + ":sm";
        outbox("outbox_activations", activationsTopic, key, null, write(a));   // no kind header for activations
        log.info("activation " + op + " " + act.slug());
        Metrics.counter("nba_activations_emitted_total", "op", op).increment();
    }

    // Insert into the transactional outbox. Debezium publishes it to Kafka.
    private void outbox(String table, String aggregatetype, String aggregateid, String kind, String payload) {
        String sql = "INSERT INTO " + table + " (aggregatetype, aggregateid, kind, payload) VALUES (?,?,?,?)";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, aggregatetype);
            ps.setString(2, aggregateid);
            ps.setString(3, kind);
            ps.setString(4, payload);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("outbox insert failed (" + table + "): " + e, e);  // Temporal retries the activity
        }
    }

    static String write(ObjectNode n) {
        try { return M.writeValueAsString(n); } catch (Exception e) { throw new RuntimeException(e); }
    }
}
