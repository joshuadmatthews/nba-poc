package ai.das.nba.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Eligibility evaluation — a faithful Java port of rules-engine.evaluate(), computing the per-action-channel
 * hit list with the SAME condition-tree semantics ({@link #treePass}/{@link #condPass}) the classic engine
 * uses for milestones/completion/variants. The classic engine compiles those same trees to MVEL/Drools
 * (inclusion && global && channel && !exclusion); evaluating them in Java yields the identical hits without
 * embedding Drools in the Flink operator. Everything downstream of the hit list (enrichment, completion/
 * milestone detection, opt-out, soft-completion, change-detect signatures) is ported verbatim.
 */
final class RulesLogic {
    private RulesLogic() {}

    static final Map<String, List<String>> CHANNEL_FUNNEL = Map.of(
            "email", List.of("Delivered", "Opened", "LinkClicked"),
            "sms", List.of("Delivered", "LinkClicked"),
            "push", List.of("Delivered", "Opened"),
            "voice", List.of("Answered", "Completed"),
            "mail", List.of("Delivered"));
    static final List<String> INBOUND_FUNNEL = List.of("Presented", "Accepted", "Completed");
    static final Map<String, String> OPTOUT_RAW = Map.of("email", "Unsubscribe", "sms", "STOP");
    static final Set<String> ACTIVE_STATES = Set.of(
            "CREATED", "IN_PROCESS", "SUPPRESSING", "PRESENTED", "SOFT_COMPLETED", "DECLINED");

    static List<String> funnelFor(String ch) { return CHANNEL_FUNNEL.getOrDefault(ch, INBOUND_FUNNEL); }

    /** Bucketed definition state passed into the eval (built from the broadcast map at call time). */
    static final class Defs {
        Map<String, JsonNode> actions = new HashMap<>();
        Map<String, JsonNode> globalRules = new HashMap<>();
        Map<String, JsonNode> channelRules = new HashMap<>();
        Map<String, JsonNode> milestones = new HashMap<>();
        Map<String, Long> globalThrottle = new HashMap<>();   // fact-key -> level
        Map<String, Long> channelHotUntil = new HashMap<>();  // channel -> hotUntil ms
        Set<String> suppressed = new java.util.HashSet<>();   // actionId | actionId.channel
    }

    static boolean isOperatorSuppressed(Defs d, String actionId, String channel) {
        return d.suppressed.contains(actionId) || d.suppressed.contains(actionId + "." + channel);
    }

    /** The eval result: the JSON to emit (null = no change, skip), the type header, and the new signatures. */
    static final class Result {
        final String evalJson; final boolean eligibilityChanged; final String fullHash; final String eligHash;
        Result(String e, boolean ec, String fh, String eh) { evalJson = e; eligibilityChanged = ec; fullHash = fh; eligHash = eh; }
    }

    /** Evaluate one snapshot against the defs. priorFull/priorElig = the member's last-emitted signatures (memory
     *  change-detect). Returns Result with evalJson==null when nothing moved (skip emit). */
    static Result evaluate(String snapJson, Defs d, String priorFull, String priorElig) throws Exception {
        JsonNode snap = SnapshotLogic.M.readTree(snapJson);
        String nbaId = snap.path("nbaId").asText();
        Map<String, Object> f = new HashMap<>();
        JsonNode facts = snap.get("facts");
        if (facts != null)
            for (Iterator<Map.Entry<String, JsonNode>> it = facts.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> e = it.next();
                f.put(e.getKey(), typedValue(e.getValue()));
            }
        for (Map.Entry<String, Long> e : d.globalThrottle.entrySet()) f.put(e.getKey(), e.getValue());

        long now = System.currentTimeMillis();
        // milestones
        Map<String, String> doneMs = new HashMap<>();
        java.util.Set<String> newMs = new java.util.LinkedHashSet<>();
        for (String fk : f.keySet()) if (fk.startsWith("nba.milestone."))
            doneMs.put(fk.substring("nba.milestone.".length()), String.valueOf(f.get(fk)));
        for (Map.Entry<String, JsonNode> me : d.milestones.entrySet()) {
            JsonNode logic = me.getValue().get("logic");
            if (logic != null && !logic.isNull() && treePass(logic, f)) {
                if (!doneMs.containsKey(me.getKey())) newMs.add(me.getKey());
                doneMs.putIfAbsent(me.getKey(), String.valueOf(now));
            }
        }
        // hard completion (per action)
        Map<String, String> doneCompleted = new HashMap<>();
        java.util.Set<String> newCompleted = new java.util.LinkedHashSet<>();
        for (Map.Entry<String, JsonNode> ae : d.actions.entrySet()) {
            JsonNode cTree = ae.getValue().get("completion");
            boolean byCriterion = cTree != null && !cTree.isNull() && treePass(cTree, f);
            boolean signalled = isTruthy(f.get("nba.completion." + ae.getKey()));
            if (byCriterion || signalled) doneCompleted.put(ae.getKey(), String.valueOf(now));
            if (byCriterion && !signalled) newCompleted.add(ae.getKey());
        }
        // opt-out (built-in compliance channel rule)
        Map<String, String> optedOut = new HashMap<>();
        for (String fk : f.keySet()) {
            if (!fk.startsWith("nba.disposition.")) continue;
            int dot = fk.lastIndexOf('.'); if (dot < 0) continue;
            String ch = fk.substring(dot + 1); String raw = OPTOUT_RAW.get(ch); Object dv = f.get(fk);
            if (raw != null && dv != null && raw.equals(dv.toString())) optedOut.put(ch, "1");
        }

        // hit list (Java tree-eval, == the DRL): inclusion && global && channel(non-rate) && !exclusion
        Set<String> hitSlugs = eligibleHits(d, f);

        ObjectNode eval = SnapshotLogic.M.createObjectNode();
        eval.put("nbaId", nbaId);
        eval.put("entityType", snap.path("entityType").asText(""));
        eval.put("entityId", snap.path("entityId").asText(""));
        eval.put("correlationId", snap.path("correlationId").asText(""));
        eval.put("evaluatedAt", now);
        ArrayNode chans = eval.putArray("channelActions");
        List<String> eligSig = new ArrayList<>(), fullSig = new ArrayList<>();

        // candidate slugs = hits + any live workflow
        LinkedHashMap<String, String[]> slugs = new LinkedHashMap<>();
        for (String r : hitSlugs) { String[] p = r.split("::", 2); if (p.length == 2 && d.actions.containsKey(p[0])) slugs.put(r, p); }
        for (String fk : f.keySet()) {
            if (!fk.startsWith("nba.actionstate.")) continue;
            Object v = f.get(fk); String st = v == null ? null : v.toString();
            if (st == null || !ACTIVE_STATES.contains(st)) continue;
            String rem = fk.substring("nba.actionstate.".length()); int dot = rem.lastIndexOf('.'); if (dot < 0) continue;
            slugs.putIfAbsent(rem.substring(0, dot) + "::" + rem.substring(dot + 1), new String[]{rem.substring(0, dot), rem.substring(dot + 1)});
        }

        for (Map.Entry<String, String[]> se : slugs.entrySet()) {
            String slug = se.getKey(), aid = se.getValue()[0], ch = se.getValue()[1];
            JsonNode a = d.actions.get(aid);
            Object stt = f.get("nba.actionstate." + aid + "." + ch);
            String state = stt == null ? null : stt.toString();
            boolean activeWf = state != null && ACTIVE_STATES.contains(state);
            boolean completed = doneCompleted.containsKey(aid);
            Long hotUntil = d.channelHotUntil.get(ch);
            boolean throttleHot = hotUntil != null && now < hotUntil;
            boolean autoExcluded = completed && a != null && a.path("autoExcludeOnCompletion").asBoolean(true) && !activeWf;
            boolean channelOptedOut = optedOut.containsKey(ch);
            boolean eligible = hitSlugs.contains(slug) && !throttleHot && !autoExcluded && !channelOptedOut
                    && !isOperatorSuppressed(d, aid, ch);
            if (!eligible && !activeWf) continue;

            long ttl = a == null ? 0 : a.path("ttlSeconds").asLong(0);
            String contentKey = a == null ? "" : contentKeyFor(a, ch, f, nbaId);
            ObjectNode ca = chans.addObject();
            ca.put("actionId", aid); ca.put("channel", ch);
            ca.put("name", a == null ? "" : a.path("name").asText());
            ca.put("ttlSeconds", ttl); ca.put("contentKey", contentKey);
            ca.put("eligible", eligible);
            Object sc = f.get("nba.score." + aid + "." + ch);
            if (sc instanceof Number num) ca.put("score", num.doubleValue()); else ca.putNull("score");
            if (state != null) ca.put("workflowState", state); else ca.putNull("workflowState");
            ca.put("active", activeWf);
            ca.put("cancellable", "CREATED".equals(state));
            ca.put("hardCompleted", completed);
            long hardTtl = a == null ? 0 : a.path("hardTtlSeconds").asLong(0);
            if (hardTtl > 0) ca.put("hardTtlSeconds", hardTtl);
            Object disp = f.get("nba.disposition." + aid + "." + ch);
            boolean soft = a != null && softCompleted(a, ch, disp == null ? null : disp.toString());
            ca.put("softCompleted", soft);
            if (channelOptedOut) ca.put("optedOut", true);

            String idSig = aid + "::" + ch + "::" + contentKey + "::" + ttl;
            if (eligible) eligSig.add(idSig);
            fullSig.add(idSig + "=" + (sc instanceof Number num ? num.doubleValue() : "null")
                    + "@" + state + "#e" + eligible + "#s" + soft + "#h" + completed);
        }
        if (!doneMs.isEmpty()) {
            ArrayNode ms = eval.putArray("milestones");
            for (String mid : new java.util.TreeSet<>(doneMs.keySet())) {
                JsonNode mdef = d.milestones.get(mid);
                ObjectNode mo = ms.addObject(); mo.put("id", mid);
                mo.put("name", mdef != null ? mdef.path("name").asText(mid) : mid);
                long cat = 0; try { cat = Long.parseLong(doneMs.get(mid)); } catch (Exception ignore) {}
                mo.put("completedAt", cat); fullSig.add("milestone:" + mid);
            }
        }
        if (!newMs.isEmpty()) {
            ArrayNode nm = eval.putArray("newMilestones");
            for (String mid : newMs) { ObjectNode mo = nm.addObject(); mo.put("id", mid); mo.put("completedAt", now); }
        }
        if (!doneCompleted.isEmpty()) {
            ArrayNode comp = eval.putArray("completed");
            for (String aid : new java.util.TreeSet<>(doneCompleted.keySet())) { comp.add(aid); fullSig.add("completed:" + aid); }
        }
        if (!newCompleted.isEmpty()) {
            ArrayNode nc = eval.putArray("newCompleted");
            for (String aid : newCompleted) nc.add(aid);
        }

        java.util.Collections.sort(eligSig); java.util.Collections.sort(fullSig);
        String eligHash = String.join("|", eligSig), fullHash = String.join("|", fullSig);
        if (fullHash.equals(priorFull == null ? "" : priorFull)) return new Result(null, false, fullHash, eligHash);
        boolean eligibilityChanged = !eligHash.equals(priorElig == null ? "" : priorElig);
        eval.put("eligibilityChanged", eligibilityChanged);
        return new Result(SnapshotLogic.M.writeValueAsString(eval), eligibilityChanged, fullHash, eligHash);
    }

    /** Hit list = action-channels passing inclusion && all global && all channel(non-rate) && NOT exclusion. */
    static Set<String> eligibleHits(Defs d, Map<String, Object> f) {
        Set<String> hits = new java.util.HashSet<>();
        for (Map.Entry<String, JsonNode> ae : d.actions.entrySet()) {
            String aid = ae.getKey(); JsonNode a = ae.getValue();
            if (aid.isEmpty()) continue;
            boolean incl = treePass(a.get("inclusion"), f);
            boolean excl = exclTreePresentAndPasses(a.get("exclusion"), f);
            boolean global = allTreesPass(d.globalRules.values(), f);
            JsonNode channels = a.get("channels");
            if (channels == null || !channels.isArray()) continue;
            for (JsonNode ch : channels) {
                String chName = ch.path("channel").asText(""); if (chName.isEmpty()) continue;
                boolean channelPass = allTreesPass(channelRulesFor(d, chName), f);
                if (incl && global && channelPass && !excl) hits.add(aid + "::" + chName);
            }
        }
        return hits;
    }

    /** Exclusion: empty tree => not excluded; otherwise excluded iff the tree passes (mirrors `not Snap(excl)`). */
    static boolean exclTreePresentAndPasses(JsonNode tree, Map<String, Object> f) {
        if (tree == null || tree.isNull()) return false;
        JsonNode conds = tree.get("conditions");
        if (conds == null || !conds.isArray() || conds.size() == 0) return false;
        return treePass(tree, f);
    }

    static boolean allTreesPass(Collection<JsonNode> rules, Map<String, Object> f) {
        for (JsonNode r : rules) {
            JsonNode logic = r.get("logic");
            if (logic != null && !logic.isNull() && !treePass(logic, f)) return false;
        }
        return true;
    }

    static Collection<JsonNode> channelRulesFor(Defs d, String ch) {
        List<JsonNode> out = new ArrayList<>();
        for (JsonNode r : d.channelRules.values())
            if (ch.equals(r.path("channel").asText()) && !referencesRate(r)) out.add(r);
        return out;
    }

    static boolean referencesRate(JsonNode rule) {
        JsonNode conds = rule.path("logic").path("conditions");
        if (conds.isArray()) for (JsonNode c : conds) if (c.path("fact").asText("").endsWith(".rate")) return true;
        return false;
    }

    // ── condition-tree evaluation (verbatim from rules-engine) ──
    static boolean treePass(JsonNode tree, Map<String, Object> facts) {
        if (tree == null || tree.isNull()) return true;
        JsonNode conds = tree.get("conditions");
        if (conds == null || !conds.isArray() || conds.size() == 0) return true;
        boolean any = "any".equals(tree.path("op").asText("all"));
        for (JsonNode c : conds) {
            boolean ok = (c.has("conditions") && c.get("conditions").isArray()) ? treePass(c, facts) : condPass(c, facts);
            if (any && ok) return true;
            if (!any && !ok) return false;
        }
        return !any;
    }

    static boolean condPass(JsonNode c, Map<String, Object> facts) {
        String fact = c.path("fact").asText("");
        if (fact.isEmpty()) return true;
        String cmp = c.path("cmp").asText("eq");
        Object actual = facts.get(fact);
        if ("exists".equals(cmp)) return actual != null;
        JsonNode val = c.get("value");
        if (val != null && val.isNumber() && (actual == null || actual instanceof Number || actual instanceof Boolean)) {
            double x = actual == null ? 0.0 : actual instanceof Boolean ? (((Boolean) actual) ? 1.0 : 0.0) : ((Number) actual).doubleValue();
            double y = val.asDouble();
            return switch (cmp) {
                case "ne" -> x != y; case "gt" -> x > y; case "gte" -> x >= y;
                case "lt" -> x < y; case "lte" -> x <= y; default -> x == y;
            };
        }
        if (val != null && val.isBoolean()) {
            boolean x = actual instanceof Boolean b ? b : Boolean.parseBoolean(String.valueOf(actual)), y = val.asBoolean();
            return "ne".equals(cmp) ? x != y : x == y;
        }
        String x = actual == null ? "" : String.valueOf(actual), y = val == null ? "" : val.asText();
        if ("in".equals(cmp)) { for (String part : y.split(",")) if (part.trim().equals(x)) return true; return false; }
        return "ne".equals(cmp) ? !x.equals(y) : x.equals(y);
    }

    static boolean isTruthy(Object v) {
        if (v == null) return false;
        if (v instanceof Boolean b) return b;
        String s = String.valueOf(v);
        return s.equalsIgnoreCase("completed") || s.equalsIgnoreCase("true") || s.equals("1");
    }

    static Object typedValue(JsonNode fv) {
        JsonNode v = fv.get("value");
        if (v == null || v.isNull()) return null;
        if (v.isBoolean()) return v.asBoolean();
        if (v.isIntegralNumber()) return v.asLong();
        if (v.isNumber()) return v.asDouble();
        return v.asText();
    }

    static String contentKeyFor(JsonNode a, String ch, Map<String, Object> facts, String memberKey) {
        JsonNode channels = a.get("channels");
        if (channels == null) return "";
        for (JsonNode c : channels) {
            if (!ch.equals(c.path("channel").asText())) continue;
            String base = c.path("contentKey").asText("");
            JsonNode variants = c.get("variants");
            if (variants != null && variants.isArray()) {
                int idx = 0;
                for (JsonNode v : variants) {
                    idx++;
                    String vk = v.path("contentKey").asText(""); if (vk.isEmpty()) continue;
                    JsonNode conds = v.get("conditions");
                    if (conds != null && !conds.isNull() && !treePass(conds, facts)) continue;
                    JsonNode pct = v.get("percent");
                    if (pct != null && pct.isNumber() && pct.asInt() < 100) {
                        int bucket = Math.floorMod((memberKey + ":" + ch + ":" + idx).hashCode(), 100);
                        if (bucket >= pct.asInt()) continue;
                    }
                    return vk;
                }
            }
            return base;
        }
        return "";
    }

    static boolean softCompleted(JsonNode a, String channel, String status) {
        if (status == null || status.isEmpty()) return false;
        List<String> funnel = funnelFor(channel);
        String bar = funnel.get(funnel.size() - 1);
        JsonNode chans = a.get("channels");
        if (chans != null) for (JsonNode c : chans)
            if (channel.equals(c.path("channel").asText()) && c.hasNonNull("softCompletion")) { bar = c.get("softCompletion").asText(bar); break; }
        int si = funnel.indexOf(status), bi = funnel.indexOf(bar);
        if (bi < 0) bi = funnel.size() - 1;
        return si >= 0 && si >= bi;
    }
}
