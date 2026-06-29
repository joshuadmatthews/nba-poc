package ai.das.nba.flink;

/**
 * A record read off any NBA Kafka topic: the kafka key, the raw JSON value, the "kind" header (router /
 * disposition / score / state / completion / milestone / throttle-suppress — null if absent), and the kafka
 * timestamp (used as event time). Flink POJO (public fields + no-arg ctor) for efficient serialization.
 */
public class FactRecord {
    public String key;
    public String value;
    public String kind;
    public long ts;

    public FactRecord() {}

    public FactRecord(String key, String value, String kind, long ts) {
        this.key = key; this.value = value; this.kind = kind; this.ts = ts;
    }
}
