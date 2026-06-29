package ai.das.nba.temporal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** A router action fact from nba.activations — the input to a ChannelAction workflow. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Activation {
    public String nbaId;
    public String entityType;
    public String entityId;
    public String memberId;      // the ORIGINAL member id (not nbaId) — rides the whole path
    public String op;            // CREATE | SUPPRESS | DISPATCH | CANCEL
    public String actionId;
    public String channel;
    public String name;
    public String contentKey;
    public long   ttlSeconds;
    public double score;
    public String correlationId;
    public String trackingId;    // workflowId + correlationId — correlates a send to its disposition
    public String source;
    public long   eventTs;

    // ---- batching ----
    // A batch CREATE carries the N top actions to send together on `channel` (actionId/contentKey above
    // are unset for a batch). preDispatched marks a CHILD workflow the BatchOrchestrator spawned — it
    // starts post-dispatch (the batch already went out) and only tracks this action's disposition.
    public java.util.List<BatchAction> actions;
    public boolean preDispatched;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BatchAction {
        public String actionId;
        public String contentKey;
        public String name;
        public long   ttlSeconds;
        public double score;
        public BatchAction() {}
    }

    public boolean isBatch() { return actions != null && !actions.isEmpty(); }

    /** memberId, falling back to entityId (the source member identifier). */
    public String member() { return (memberId != null && !memberId.isEmpty()) ? memberId : entityId; }

    public Activation() {}

    public String slug() { return nbaId + ":" + actionId + ":" + channel; }
    /** Stable id for the batch orchestrator on this (member, channel). */
    public String batchSlug() { return nbaId + ":" + channel; }
}
