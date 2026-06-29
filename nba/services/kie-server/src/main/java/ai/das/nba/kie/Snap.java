package ai.das.nba.kie;

import java.util.Map;

/**
 * The Drools working-memory fact: one per snapshot. The generated rules match against its facts via
 * get("<dotted key>") / getOr("<key>", default). Identical to the rules engine's Snap so the same DRL fires.
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

    public Object getOr(String key, Object def) {
        Object v = (f == null) ? null : f.get(key);
        return v != null ? v : def;
    }

    public String getNbaId() { return nbaId; }
}
