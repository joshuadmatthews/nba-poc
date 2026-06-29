package ai.das.nba.rules;

import java.util.Map;

/**
 * The Drools working-memory fact: one per snapshot. The generated rules match against
 * its facts via get("<dotted key>"), e.g. Snap(get("operator.activity.usedChat") == false).
 */
public class Snap {
    public String nbaId;
    public Map<String, Object> f;   // factKey -> typed value (Boolean/Number/String)

    public Snap(String nbaId, Map<String, Object> f) {
        this.nbaId = nbaId;
        this.f = f;
    }

    public Object get(String key) {
        return f == null ? null : f.get(key);
    }

    /** Value for key, or `def` when the fact is absent — lets a missing fact behave as
     *  the type's default (0 / false / ""), so new members aren't wrongly suppressed. */
    public Object getOr(String key, Object def) {
        Object v = (f == null) ? null : f.get(key);
        if (v == null) return def;
        // A BOOLEAN driven-fact compared NUMERICALLY (a journey prereq is `gte respondedToOutreach 1`) must
        // coerce true/false -> 1/0 — the same coercion the Java completion-check (condPass) already does. Without
        // it, MVEL compares Boolean vs number and the prereq silently fails, so journeys never advance past the
        // no-prereq REACH actions. `def` carries the expected type from defaultFor(value): Number => numeric compare.
        if (def instanceof Number && v instanceof Boolean b) return b ? 1 : 0;
        return v;
    }

    public String getNbaId() { return nbaId; }
}
