package ai.das.nba.flink;

/** A snapshot-bound fact after classify + NBAID-resolve, ready to be keyed by nbaId and folded by event-time
 *  LWW. fvJson is the flattened fact-value object ({"value","valueType","eventTs","source"}) as JSON. */
public class SnapMsg {
    public String nbaId;
    public String entityType;
    public String entityId;
    public String factKey;
    public long eventTs;
    public String fvJson;

    public SnapMsg() {}

    public SnapMsg(String nbaId, String entityType, String entityId, String factKey, long eventTs, String fvJson) {
        this.nbaId = nbaId; this.entityType = entityType; this.entityId = entityId;
        this.factKey = factKey; this.eventTs = eventTs; this.fvJson = fvJson;
    }
}
