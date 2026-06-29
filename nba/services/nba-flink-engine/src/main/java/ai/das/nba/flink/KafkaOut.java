package ai.das.nba.flink;

/** An outbound produce: kafka key, raw JSON value, and an optional "kind" header. Topic is fixed per sink. */
public class KafkaOut {
    public String key;
    public String value;
    public String kind;   // null = no header

    public KafkaOut() {}

    public KafkaOut(String key, String value, String kind) {
        this.key = key; this.value = value; this.kind = kind;
    }

    public static KafkaOut of(String key, String value) { return new KafkaOut(key, value, null); }
    public static KafkaOut of(String key, String value, String kind) { return new KafkaOut(key, value, kind); }
}
