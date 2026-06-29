package ai.das.nba.flink;

/** The per-(member,action,channel) lifecycle state held in Flink keyed state — the Flink equivalent of a
 *  ChannelActionWorkflow instance. phase = DEBOUNCE (in the debounce window) | TRACKING (dispatched, watching
 *  dispositions until TTL) | DONE (terminal). The rest mirrors ChannelActionWorkflowImpl's fields. */
public class SmState {
    public String phase;          // DEBOUNCE | TRACKING | DONE
    public String currentState;   // last emitted lifecycle state
    public boolean suppressRequested;
    public boolean operatorSuppress;
    public boolean hardCompleted;
    public boolean emittedSoft;
    public boolean cancelSent;
    public long debounceAt;        // processing-time the debounce timer fires (0 = none)
    public long throttleAt;        // processing-time the throttle WAIT-recheck timer fires (0 = none)
    public long ttlAt;             // processing-time the TTL timer fires (0 = none)
    public boolean inBacklog;      // currently counted in the channel's throttle backlog (WAITing)
    public String nbaId, entityType, entityId, actionId, channel, name, contentKey, correlationId;
    public long ttlSeconds;
    public double score;

    public SmState() {}

    String memberKey() { return entityType + ":" + entityId; }
    String stateKey() { return "nba.actionstate." + actionId + "." + channel; }
    String trackingId() { return "nba-ca:" + nbaId + ":" + actionId + ":" + channel + "|" + (correlationId == null ? "" : correlationId); }
}
