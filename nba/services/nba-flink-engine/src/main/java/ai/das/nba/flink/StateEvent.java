package ai.das.nba.flink;

/** A signal into the lifecycle state machine, keyed by "{nbaId}:{actionId}:{channel}". type =
 *  CREATE | SUPPRESS | OPERATOR_SUPPRESS | SOFT_COMPLETE | HARD_COMPLETE | DISPOSITION. value = the raw
 *  router-activation or disposition JSON. */
public class StateEvent {
    public String key;
    public String type;
    public String value;

    public StateEvent() {}

    public StateEvent(String key, String type, String value) { this.key = key; this.type = type; this.value = value; }
}
