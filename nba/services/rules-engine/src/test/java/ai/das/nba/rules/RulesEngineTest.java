package ai.das.nba.rules;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Component-level unit tests for the PURE static core of the NBA rules engine — the parts that
 * need no Kafka / Redis / KieSession: single-condition eval ({@link RulesEngine#condPass}), the
 * AND/OR/nested condition-tree walker ({@link RulesEngine#treePass}), deterministic A/B variant
 * selection ({@link RulesEngine#contentKeyFor}), and the DRL/MVEL text the engine generates from
 * the structured definition logic ({@link RulesEngine#buildDrl} and friends).
 *
 * All functions under test are package-private statics on {@link RulesEngine}; no production
 * visibility was changed. The facts map is {@code Map<String,Object>} (matching the runtime: the
 * eval builds it from the snapshot's typed fact values via {@code typedValue}).
 *
 * Tests assert ACTUAL observed behavior. Where a behavior is subtle or arguably surprising (e.g.
 * the numeric comparator only engaging when the actual fact is null-or-Number, so a numeric
 * `value` against a String fact silently falls through to string compare) it is called out in the
 * test name / comments rather than "fixed".
 */
class RulesEngineTest {

    // ── tiny JSON builders (dependency-free; mirror what the Command Center authors) ───────────
    static JsonNode cond(String fact, String cmp, Object value) {
        ObjectNode c = RulesEngine.M.createObjectNode();
        c.put("fact", fact);
        c.put("cmp", cmp);
        if (value == null) c.putNull("value");
        else if (value instanceof Boolean b) c.put("value", b);
        else if (value instanceof Integer i) c.put("value", i);
        else if (value instanceof Long l) c.put("value", l);
        else if (value instanceof Double d) c.put("value", d);
        else c.put("value", value.toString());
        return c;
    }

    static ObjectNode tree(String op, JsonNode... conds) {
        ObjectNode t = RulesEngine.M.createObjectNode();
        t.put("op", op);
        var arr = t.putArray("conditions");
        for (JsonNode c : conds) arr.add(c);
        return t;
    }

    static Map<String, Object> facts(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    // The static definition maps are shared/global; reset them around every test that touches DRL.
    @BeforeEach
    @AfterEach
    void clearDefs() {
        RulesEngine.actions.clear();
        RulesEngine.globalRules.clear();
        RulesEngine.channelRules.clear();
        RulesEngine.milestones.clear();
        RulesEngine.GLOBAL_THROTTLE.clear();
        RulesEngine.CHANNEL_HOT_UNTIL.clear();
        RulesEngine.SUPPRESSED.clear();
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // 1. condPass — single-condition evaluation, EVERY comparator
    // ══════════════════════════════════════════════════════════════════════════════════════════

    @Test
    void condPass_emptyFact_isVacuouslyTrue() {
        assertTrue(RulesEngine.condPass(cond("", "eq", 1), facts()));
    }

    @Test
    void condPass_exists_presentVsAbsent() {
        assertTrue(RulesEngine.condPass(cond("score", "exists", null), facts("score", 5L)));
        assertFalse(RulesEngine.condPass(cond("score", "exists", null), facts()));
        // exists is presence-only: a present-but-zero / present-but-false value still EXISTS.
        assertTrue(RulesEngine.condPass(cond("score", "exists", null), facts("score", 0L)));
        assertTrue(RulesEngine.condPass(cond("flag", "exists", null), facts("flag", false)));
    }

    @Test
    void condPass_numericComparators_allOps() {
        Map<String, Object> f = facts("n", 5L);
        assertTrue(RulesEngine.condPass(cond("n", "eq", 5), f));
        assertFalse(RulesEngine.condPass(cond("n", "eq", 6), f));
        assertTrue(RulesEngine.condPass(cond("n", "ne", 6), f));
        assertFalse(RulesEngine.condPass(cond("n", "ne", 5), f));
        assertTrue(RulesEngine.condPass(cond("n", "gt", 4), f));
        assertFalse(RulesEngine.condPass(cond("n", "gt", 5), f));
        assertTrue(RulesEngine.condPass(cond("n", "gte", 5), f));
        assertFalse(RulesEngine.condPass(cond("n", "gte", 6), f));
        assertTrue(RulesEngine.condPass(cond("n", "lt", 6), f));
        assertFalse(RulesEngine.condPass(cond("n", "lt", 5), f));
        assertTrue(RulesEngine.condPass(cond("n", "lte", 5), f));
        assertFalse(RulesEngine.condPass(cond("n", "lte", 4), f));
    }

    @Test
    void condPass_numericComparator_castsAcrossNumberTypes() {
        // facts may be Long (integral) or Double (fractional) — both cast to double for compare.
        assertTrue(RulesEngine.condPass(cond("n", "gt", 1), facts("n", 2.5d)));
        assertTrue(RulesEngine.condPass(cond("n", "eq", 3), facts("n", 3.0d)));
        assertTrue(RulesEngine.condPass(cond("n", "lt", 3.5d), facts("n", 3L)));
    }

    @Test
    void condPass_missingNumericFact_defaultsToZero() {
        // A member with no count reads as 0 against a numeric comparison.
        Map<String, Object> f = facts();
        assertTrue(RulesEngine.condPass(cond("commsThisWeek", "eq", 0), f), "absent == 0");
        assertTrue(RulesEngine.condPass(cond("commsThisWeek", "lt", 3), f), "absent < 3");
        assertFalse(RulesEngine.condPass(cond("commsThisWeek", "gt", 0), f), "absent !> 0");
        assertTrue(RulesEngine.condPass(cond("commsThisWeek", "lte", 0), f));
        assertTrue(RulesEngine.condPass(cond("commsThisWeek", "ne", 5), f), "0 != 5");
    }

    @Test
    void condPass_booleanEq_trueAndFalse() {
        assertTrue(RulesEngine.condPass(cond("optedIn", "eq", true), facts("optedIn", true)));
        assertFalse(RulesEngine.condPass(cond("optedIn", "eq", true), facts("optedIn", false)));
        assertTrue(RulesEngine.condPass(cond("optedIn", "eq", false), facts("optedIn", false)));
        assertTrue(RulesEngine.condPass(cond("optedIn", "ne", false), facts("optedIn", true)));
        assertFalse(RulesEngine.condPass(cond("optedIn", "ne", true), facts("optedIn", true)));
    }

    @Test
    void condPass_booleanEq_missingFactDefaultsFalse() {
        // actual==null -> Boolean.parseBoolean("null") == false
        assertTrue(RulesEngine.condPass(cond("optedIn", "eq", false), facts()));
        assertFalse(RulesEngine.condPass(cond("optedIn", "eq", true), facts()));
    }

    @Test
    void condPass_booleanEq_coercesStringFact() {
        // a stringy "true" fact still compares as boolean true.
        assertTrue(RulesEngine.condPass(cond("optedIn", "eq", true), facts("optedIn", "true")));
        assertFalse(RulesEngine.condPass(cond("optedIn", "eq", true), facts("optedIn", "nope")));
    }

    @Test
    void condPass_stringEq_exactAndNonMatch() {
        assertTrue(RulesEngine.condPass(cond("tier", "eq", "gold"), facts("tier", "gold")));
        assertFalse(RulesEngine.condPass(cond("tier", "eq", "gold"), facts("tier", "silver")));
    }

    @Test
    void condPass_stringNe_exactAndNonMatch() {
        assertTrue(RulesEngine.condPass(cond("tier", "ne", "gold"), facts("tier", "silver")));
        assertFalse(RulesEngine.condPass(cond("tier", "ne", "gold"), facts("tier", "gold")));
    }

    @Test
    void condPass_stringEq_missingFactIsEmptyString() {
        assertTrue(RulesEngine.condPass(cond("tier", "eq", ""), facts()), "absent string == \"\"");
        assertTrue(RulesEngine.condPass(cond("tier", "ne", "gold"), facts()), "absent != \"gold\"");
    }

    @Test
    void condPass_in_membershipAndNonMembership() {
        JsonNode c = cond("tier", "in", "gold,silver,bronze");
        assertTrue(RulesEngine.condPass(c, facts("tier", "silver")), "member of list");
        assertFalse(RulesEngine.condPass(c, facts("tier", "platinum")), "not in list");
        // whitespace in the list is trimmed before compare.
        assertTrue(RulesEngine.condPass(cond("tier", "in", "gold, silver , bronze"), facts("tier", "silver")));
    }

    @Test
    void condPass_in_coercesNumericFactToString() {
        // `in` lives in the string branch; a Long fact stringifies to "5" and matches "5".
        assertTrue(RulesEngine.condPass(cond("seg", "in", "3,5,7"), facts("seg", 5L)));
        assertFalse(RulesEngine.condPass(cond("seg", "in", "3,5,7"), facts("seg", 4L)));
    }

    @Test
    void condPass_numericValueAgainstStringFact_fallsThroughToStringCompare() {
        // DOCUMENTED behavior: the numeric branch only engages when actual is null or a Number.
        // A numeric `value` (5) against a String fact "5" therefore string-compares "5"=="5" -> true,
        // and "5" vs "x" eq -> false. (Not a bug to fix here; asserting the real branch taken.)
        assertTrue(RulesEngine.condPass(cond("n", "eq", 5), facts("n", "5")));
        assertFalse(RulesEngine.condPass(cond("n", "eq", 5), facts("n", "x")));
        // ...and since it is the STRING branch, "ne" works but gt/gte/lt/lte are NOT understood:
        // they collapse to the eq default (x.equals(y)). gt here behaves like eq.
        assertTrue(RulesEngine.condPass(cond("n", "gt", 5), facts("n", "5")),
                "string-branch: unknown cmp 'gt' falls to eq, '5'=='5' -> true");
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // 2. treePass — AND / OR / empty / nested composition
    // ══════════════════════════════════════════════════════════════════════════════════════════

    @Test
    void treePass_nullOrEmptyTree_matchesAll() {
        assertTrue(RulesEngine.treePass(null, facts()));
        assertTrue(RulesEngine.treePass(RulesEngine.M.nullNode(), facts()));
        assertTrue(RulesEngine.treePass(tree("all"), facts()), "no conditions -> true");
        // an object with no `conditions` array at all -> true.
        assertTrue(RulesEngine.treePass(RulesEngine.M.createObjectNode(), facts()));
    }

    @Test
    void treePass_all_requiresEveryCondition() {
        JsonNode t = tree("all", cond("a", "eq", 1), cond("b", "eq", 2));
        assertTrue(RulesEngine.treePass(t, facts("a", 1L, "b", 2L)));
        assertFalse(RulesEngine.treePass(t, facts("a", 1L, "b", 9L)), "one failing -> all fails");
        assertFalse(RulesEngine.treePass(t, facts("a", 9L, "b", 2L)));
    }

    @Test
    void treePass_defaultOpIsAll() {
        // op absent -> defaults to "all".
        ObjectNode t = RulesEngine.M.createObjectNode();
        var arr = t.putArray("conditions");
        arr.add(cond("a", "eq", 1));
        arr.add(cond("b", "eq", 2));
        assertTrue(RulesEngine.treePass(t, facts("a", 1L, "b", 2L)));
        assertFalse(RulesEngine.treePass(t, facts("a", 1L, "b", 3L)));
    }

    @Test
    void treePass_any_requiresAtLeastOne() {
        JsonNode t = tree("any", cond("a", "eq", 1), cond("b", "eq", 2));
        assertTrue(RulesEngine.treePass(t, facts("a", 1L, "b", 9L)), "first matches");
        assertTrue(RulesEngine.treePass(t, facts("a", 9L, "b", 2L)), "second matches");
        assertFalse(RulesEngine.treePass(t, facts("a", 9L, "b", 9L)), "none match -> false");
    }

    @Test
    void treePass_nested_allContainingAny() {
        // all( a==1 , any( b==2 , c==3 ) )
        JsonNode t = tree("all",
                cond("a", "eq", 1),
                tree("any", cond("b", "eq", 2), cond("c", "eq", 3)));
        assertTrue(RulesEngine.treePass(t, facts("a", 1L, "b", 2L)), "a + b-branch");
        assertTrue(RulesEngine.treePass(t, facts("a", 1L, "c", 3L)), "a + c-branch");
        assertFalse(RulesEngine.treePass(t, facts("a", 1L)), "a but neither inner -> false");
        assertFalse(RulesEngine.treePass(t, facts("b", 2L)), "inner ok but outer a fails");
    }

    @Test
    void treePass_nested_anyContainingAll_deep() {
        // any( all( a==1, b==2 ) , all( c==3, d==4 ) )  — deep, mixed
        JsonNode t = tree("any",
                tree("all", cond("a", "eq", 1), cond("b", "eq", 2)),
                tree("all", cond("c", "eq", 3), cond("d", "eq", 4)));
        assertTrue(RulesEngine.treePass(t, facts("a", 1L, "b", 2L)), "left all satisfied");
        assertTrue(RulesEngine.treePass(t, facts("c", 3L, "d", 4L)), "right all satisfied");
        assertFalse(RulesEngine.treePass(t, facts("a", 1L, "c", 3L)), "neither all fully satisfied");
        assertTrue(RulesEngine.treePass(t, facts("a", 1L, "b", 2L, "c", 3L, "d", 4L)), "both satisfied");
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // 3. contentKeyFor — deterministic A/B variant selection
    // ══════════════════════════════════════════════════════════════════════════════════════════

    /** Build an action with one channel: base contentKey + an ordered variant list. */
    static JsonNode actionWithVariants(String channel, String base, JsonNode... variants) {
        ObjectNode a = RulesEngine.M.createObjectNode();
        a.put("id", "act1");
        var chans = a.putArray("channels");
        ObjectNode c = chans.addObject();
        c.put("channel", channel);
        c.put("contentKey", base);
        var vs = c.putArray("variants");
        for (JsonNode v : variants) vs.add(v);
        return a;
    }

    static ObjectNode variant(String contentKey, Integer percent, JsonNode conditions) {
        ObjectNode v = RulesEngine.M.createObjectNode();
        v.put("contentKey", contentKey);
        if (percent != null) v.put("percent", percent);
        if (conditions != null) v.set("conditions", conditions);
        return v;
    }

    @Test
    void contentKeyFor_noChannelMatch_returnsEmpty() {
        JsonNode a = actionWithVariants("email", "base.email");
        assertEquals("", RulesEngine.contentKeyFor(a, "sms", facts(), "m1"));
    }

    @Test
    void contentKeyFor_noVariants_returnsBase() {
        JsonNode a = actionWithVariants("email", "base.email");
        assertEquals("base.email", RulesEngine.contentKeyFor(a, "email", facts(), "m1"));
    }

    @Test
    void contentKeyFor_nullChannels_returnsEmpty() {
        ObjectNode a = RulesEngine.M.createObjectNode();
        a.put("id", "act1");           // no "channels" node at all
        assertEquals("", RulesEngine.contentKeyFor(a, "email", facts(), "m1"));
    }

    @Test
    void contentKeyFor_variantConditionGate_failingFallsThrough() {
        // variant gated on tier==gold; a silver member fails it -> base.
        JsonNode a = actionWithVariants("email", "base.email",
                variant("v.gold", 100, tree("all", cond("tier", "eq", "gold"))));
        assertEquals("v.gold", RulesEngine.contentKeyFor(a, "email", facts("tier", "gold"), "m1"));
        assertEquals("base.email", RulesEngine.contentKeyFor(a, "email", facts("tier", "silver"), "m1"));
    }

    @Test
    void contentKeyFor_firstMatchWins() {
        // two ungated 100% variants — the FIRST in order wins.
        JsonNode a = actionWithVariants("email", "base.email",
                variant("v.first", 100, null),
                variant("v.second", 100, null));
        assertEquals("v.first", RulesEngine.contentKeyFor(a, "email", facts(), "m1"));
    }

    @Test
    void contentKeyFor_firstGatedFails_secondWins() {
        JsonNode a = actionWithVariants("email", "base.email",
                variant("v.gold", 100, tree("all", cond("tier", "eq", "gold"))),
                variant("v.silver", 100, tree("all", cond("tier", "eq", "silver"))));
        assertEquals("v.silver", RulesEngine.contentKeyFor(a, "email", facts("tier", "silver"), "m1"));
    }

    @Test
    void contentKeyFor_deterministicPerMember_sameMemberSameVariant() {
        // a 50% split: whatever bucket a member lands in, it is STABLE across calls.
        JsonNode a = actionWithVariants("email", "base.email", variant("v.50", 50, null));
        String first = RulesEngine.contentKeyFor(a, "email", facts(), "memberX");
        String second = RulesEngine.contentKeyFor(a, "email", facts(), "memberX");
        assertEquals(first, second, "same member -> same variant on repeated calls");
        // and it is one of the two legal outcomes.
        assertTrue(first.equals("v.50") || first.equals("base.email"), "got: " + first);
    }

    @Test
    void contentKeyFor_percentSplit_honoredAcrossPopulation() {
        // For a 50% split over many members, BOTH outcomes occur and the split is roughly honored.
        JsonNode a = actionWithVariants("email", "base.email", variant("v.50", 50, null));
        int inVariant = 0, total = 2000;
        for (int i = 0; i < total; i++) {
            if ("v.50".equals(RulesEngine.contentKeyFor(a, "email", facts(), "member-" + i))) inVariant++;
        }
        assertTrue(inVariant > 0 && inVariant < total, "both buckets populated: " + inVariant);
        double frac = inVariant / (double) total;
        assertTrue(frac > 0.40 && frac < 0.60, "50% split roughly honored, got " + frac);
    }

    @Test
    void contentKeyFor_percentZero_neverSelectsVariant() {
        JsonNode a = actionWithVariants("email", "base.email", variant("v.never", 0, null));
        for (int i = 0; i < 200; i++)
            assertEquals("base.email", RulesEngine.contentKeyFor(a, "email", facts(), "m-" + i),
                    "percent=0 -> bucket(0..99) >= 0 always true -> skipped");
    }

    @Test
    void contentKeyFor_percent100_orAbsent_alwaysSelectsVariant() {
        // percent==100 (== not <100) and an absent percent both skip the random-split gate entirely.
        JsonNode hundred = actionWithVariants("email", "base.email", variant("v.always", 100, null));
        JsonNode none = actionWithVariants("email", "base.email", variant("v.always", null, null));
        assertEquals("v.always", RulesEngine.contentKeyFor(hundred, "email", facts(), "m1"));
        assertEquals("v.always", RulesEngine.contentKeyFor(none, "email", facts(), "m1"));
    }

    @Test
    void contentKeyFor_emptyVariantContentKey_skipped() {
        // a variant with an empty contentKey is skipped; falls to the next / base.
        JsonNode a = actionWithVariants("email", "base.email",
                variant("", 100, null),
                variant("v.real", 100, null));
        assertEquals("v.real", RulesEngine.contentKeyFor(a, "email", facts(), "m1"));
    }

    @Test
    void contentKeyFor_bucketVariesByChannelAndIndex() {
        // The bucket hashes memberKey + channel + variant-index, so different channels can land a
        // member in different buckets. Just assert the call is stable and legal per channel.
        JsonNode a = actionWithVariants("email", "base.email", variant("v.50", 50, null));
        JsonNode b = actionWithVariants("sms", "base.sms", variant("w.50", 50, null));
        String e1 = RulesEngine.contentKeyFor(a, "email", facts(), "mZ");
        String s1 = RulesEngine.contentKeyFor(b, "sms", facts(), "mZ");
        assertEquals(e1, RulesEngine.contentKeyFor(a, "email", facts(), "mZ"));
        assertEquals(s1, RulesEngine.contentKeyFor(b, "sms", facts(), "mZ"));
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // 4. DRL generation — buildDrl / exprForTree / exprForCond  (string-shape assertions)
    // ══════════════════════════════════════════════════════════════════════════════════════════

    @Test
    void exprForCond_everyComparator_rendersExpectedMvel() {
        assertEquals("getOr(\"n\", 0) == 5", RulesEngine.exprForCond(cond("n", "eq", 5)));
        assertEquals("getOr(\"n\", 0) != 5", RulesEngine.exprForCond(cond("n", "ne", 5)));
        assertEquals("getOr(\"n\", 0) > 5", RulesEngine.exprForCond(cond("n", "gt", 5)));
        assertEquals("getOr(\"n\", 0) >= 5", RulesEngine.exprForCond(cond("n", "gte", 5)));
        assertEquals("getOr(\"n\", 0) < 5", RulesEngine.exprForCond(cond("n", "lt", 5)));
        assertEquals("getOr(\"n\", 0) <= 5", RulesEngine.exprForCond(cond("n", "lte", 5)));
    }

    @Test
    void exprForCond_exists_usesGetNotNull_neverCoalesced() {
        assertEquals("get(\"flag\") != null", RulesEngine.exprForCond(cond("flag", "exists", null)));
    }

    @Test
    void exprForCond_typeDefaultsInferredFromValue() {
        // number -> default 0; boolean -> false; string -> "".
        assertTrue(RulesEngine.exprForCond(cond("n", "eq", 3)).startsWith("getOr(\"n\", 0)"));
        assertTrue(RulesEngine.exprForCond(cond("b", "eq", true)).startsWith("getOr(\"b\", false)"));
        assertTrue(RulesEngine.exprForCond(cond("s", "eq", "gold")).startsWith("getOr(\"s\", \"\")"));
    }

    @Test
    void exprForCond_stringAndBooleanValues_rendered() {
        assertEquals("getOr(\"tier\", \"\") == \"gold\"", RulesEngine.exprForCond(cond("tier", "eq", "gold")));
        assertEquals("getOr(\"optedIn\", false) == true", RulesEngine.exprForCond(cond("optedIn", "eq", true)));
    }

    @Test
    void exprForCond_emptyFact_rendersEmpty() {
        assertEquals("", RulesEngine.exprForCond(cond("", "eq", 1)));
    }

    @Test
    void exprForCond_stringValueWithQuotes_escaped() {
        JsonNode c = cond("name", "eq", "a\"b");
        assertEquals("getOr(\"name\", \"\") == \"a\\\"b\"", RulesEngine.exprForCond(c));
    }

    @Test
    void exprForTree_allJoinsWithAnd_anyJoinsWithOr_andParenthesizes() {
        JsonNode all = tree("all", cond("a", "eq", 1), cond("b", "gt", 2));
        assertEquals("(getOr(\"a\", 0) == 1 && getOr(\"b\", 0) > 2)", RulesEngine.exprForTree(all));
        JsonNode any = tree("any", cond("a", "eq", 1), cond("b", "gt", 2));
        assertEquals("(getOr(\"a\", 0) == 1 || getOr(\"b\", 0) > 2)", RulesEngine.exprForTree(any));
    }

    @Test
    void exprForTree_nullOrEmpty_rendersEmpty() {
        assertEquals("", RulesEngine.exprForTree(null));
        assertEquals("", RulesEngine.exprForTree(RulesEngine.M.nullNode()));
        assertEquals("", RulesEngine.exprForTree(tree("all")));
    }

    @Test
    void buildDrl_headerAndImports() {
        String drl = RulesEngine.buildDrl();
        assertTrue(drl.contains("import ai.das.nba.rules.Snap;"), drl);
        assertTrue(drl.contains("global java.util.List results;"), drl);
    }

    @Test
    void buildDrl_emitsOneRulePerActionChannel_withInclusionExpr() throws Exception {
        // an action with an inclusion tree + two channels -> two "elig::" rules, each carrying the
        // inclusion MVEL inside Snap(...), and each adding "actId::channel" to results.
        String actJson = """
            {"id":"act1","name":"Welcome","ttlSeconds":3600,
             "inclusion":{"op":"all","conditions":[{"fact":"score","cmp":"gte","value":50}]},
             "channels":[{"channel":"email"},{"channel":"sms"}]}
            """;
        RulesEngine.actions.put("act1", RulesEngine.M.readTree(actJson));
        String drl = RulesEngine.buildDrl();

        assertTrue(drl.contains("rule \"elig::act1::email\""), drl);
        assertTrue(drl.contains("rule \"elig::act1::sms\""), drl);
        assertTrue(drl.contains("dialect \"mvel\""), drl);
        assertTrue(drl.contains("Snap((getOr(\"score\", 0) >= 50))"), drl);
        assertTrue(drl.contains("results.add(\"act1::email\");"), drl);
        assertTrue(drl.contains("results.add(\"act1::sms\");"), drl);
    }

    @Test
    void buildDrl_exclusion_emitsNotSnapClause() throws Exception {
        String actJson = """
            {"id":"act1","channels":[{"channel":"email"}],
             "exclusion":{"op":"all","conditions":[{"fact":"unsub","cmp":"eq","value":true}]}}
            """;
        RulesEngine.actions.put("act1", RulesEngine.M.readTree(actJson));
        String drl = RulesEngine.buildDrl();
        assertTrue(drl.contains("not Snap((getOr(\"unsub\", false) == true))"), drl);
    }

    @Test
    void buildDrl_noInclusionNoRules_snapMatchesAny() throws Exception {
        // no inclusion/global/channel exprs -> Snap() with an empty body (matches any).
        RulesEngine.actions.put("act1",
                RulesEngine.M.readTree("{\"id\":\"act1\",\"channels\":[{\"channel\":\"email\"}]}"));
        String drl = RulesEngine.buildDrl();
        assertTrue(drl.contains("Snap()"), "empty body -> matches any:\n" + drl);
    }

    @Test
    void buildDrl_globalRules_andedIntoEveryChannelRule() throws Exception {
        RulesEngine.actions.put("act1",
                RulesEngine.M.readTree("{\"id\":\"act1\",\"channels\":[{\"channel\":\"email\"}]}"));
        RulesEngine.globalRules.put("g1", RulesEngine.M.readTree(
                "{\"logic\":{\"op\":\"all\",\"conditions\":[{\"fact\":\"optedIn\",\"cmp\":\"eq\",\"value\":true}]}}"));
        String drl = RulesEngine.buildDrl();
        assertTrue(drl.contains("getOr(\"optedIn\", false) == true"), drl);
    }

    @Test
    void buildDrl_channelRules_appliedOnlyToMatchingChannel() throws Exception {
        RulesEngine.actions.put("act1", RulesEngine.M.readTree(
                "{\"id\":\"act1\",\"channels\":[{\"channel\":\"email\"},{\"channel\":\"sms\"}]}"));
        // a channel rule for email only.
        RulesEngine.channelRules.put("c1", RulesEngine.M.readTree(
                "{\"channel\":\"email\",\"logic\":{\"op\":\"all\",\"conditions\":[{\"fact\":\"emailValid\",\"cmp\":\"eq\",\"value\":true}]}}"));
        String drl = RulesEngine.buildDrl();
        // The email rule body must carry emailValid; the sms rule must NOT.
        int emailRule = drl.indexOf("elig::act1::email");
        int smsRule = drl.indexOf("elig::act1::sms");
        assertTrue(emailRule >= 0 && smsRule >= 0, drl);
        String emailBlock = drl.substring(emailRule, smsRule > emailRule ? smsRule : drl.length());
        assertTrue(emailBlock.contains("getOr(\"emailValid\", false) == true"),
                "email channel rule present in email block:\n" + emailBlock);
    }

    @Test
    void buildDrl_skipsActionWithoutChannelsArray() throws Exception {
        RulesEngine.actions.put("act1", RulesEngine.M.readTree("{\"id\":\"act1\"}"));   // no channels
        String drl = RulesEngine.buildDrl();
        assertFalse(drl.contains("elig::act1"), "no channels -> no rule:\n" + drl);
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // 5. Other pure helpers — channelRulesFor / referencesRate / defaultFor / renderVal / typedValue
    // ══════════════════════════════════════════════════════════════════════════════════════════

    @Test
    void referencesRate_detectsDotRateFact() throws Exception {
        JsonNode rateRule = RulesEngine.M.readTree(
                "{\"channel\":\"email\",\"logic\":{\"conditions\":[{\"fact\":\"nba.throttle.email.rate\",\"cmp\":\"lt\",\"value\":1}]}}");
        JsonNode dailyRule = RulesEngine.M.readTree(
                "{\"channel\":\"email\",\"logic\":{\"conditions\":[{\"fact\":\"nba.throttle.email.daily\",\"cmp\":\"lt\",\"value\":1}]}}");
        assertTrue(RulesEngine.referencesRate(rateRule), ".rate fact -> true");
        assertFalse(RulesEngine.referencesRate(dailyRule), ".daily fact -> false");
    }

    @Test
    void channelRulesFor_excludesRateRules_andOtherChannels() throws Exception {
        RulesEngine.channelRules.put("daily", RulesEngine.M.readTree(
                "{\"channel\":\"email\",\"logic\":{\"conditions\":[{\"fact\":\"nba.throttle.email.daily\",\"cmp\":\"lt\",\"value\":1}]}}"));
        RulesEngine.channelRules.put("rate", RulesEngine.M.readTree(
                "{\"channel\":\"email\",\"logic\":{\"conditions\":[{\"fact\":\"nba.throttle.email.rate\",\"cmp\":\"lt\",\"value\":1}]}}"));
        RulesEngine.channelRules.put("smsd", RulesEngine.M.readTree(
                "{\"channel\":\"sms\",\"logic\":{\"conditions\":[{\"fact\":\"nba.throttle.sms.daily\",\"cmp\":\"lt\",\"value\":1}]}}"));
        var out = RulesEngine.channelRulesFor("email");
        assertEquals(1, out.size(), "only the email .daily rule (rate excluded, sms excluded)");
        assertEquals("email", out.iterator().next().path("channel").asText());
    }

    static final JsonNodeFactory JF = JsonNodeFactory.instance;

    @Test
    void defaultFor_byValueType() {
        assertEquals("0", RulesEngine.defaultFor(JF.numberNode(5)));
        assertEquals("false", RulesEngine.defaultFor(JF.booleanNode(true)));
        assertEquals("\"\"", RulesEngine.defaultFor(JF.textNode("x")));
        assertEquals("null", RulesEngine.defaultFor(null));
        assertEquals("null", RulesEngine.defaultFor(JF.nullNode()));
    }

    @Test
    void renderVal_byValueType() {
        assertEquals("5", RulesEngine.renderVal(JF.numberNode(5)));
        assertEquals("true", RulesEngine.renderVal(JF.booleanNode(true)));
        assertEquals("\"gold\"", RulesEngine.renderVal(JF.textNode("gold")));
        assertEquals("null", RulesEngine.renderVal(null));
        assertEquals("null", RulesEngine.renderVal(JF.nullNode()));
        assertEquals("\"a\\\"b\"", RulesEngine.renderVal(JF.textNode("a\"b")), "quotes escaped");
    }

    @Test
    void typedValue_mapsJsonToJavaTypes() throws Exception {
        // typedValue reads the {"value": ...} wrapper used in snapshot facts.
        assertEquals(7L, RulesEngine.typedValue(RulesEngine.M.readTree("{\"value\":7}")));
        assertEquals(2.5d, RulesEngine.typedValue(RulesEngine.M.readTree("{\"value\":2.5}")));
        assertEquals(Boolean.TRUE, RulesEngine.typedValue(RulesEngine.M.readTree("{\"value\":true}")));
        assertEquals("gold", RulesEngine.typedValue(RulesEngine.M.readTree("{\"value\":\"gold\"}")));
        assertNull(RulesEngine.typedValue(RulesEngine.M.readTree("{\"value\":null}")));
        assertNull(RulesEngine.typedValue(RulesEngine.M.readTree("{}")), "missing value -> null");
    }

    // ---- hard/soft completion ----
    @Test
    void isTruthy_completionSignalValues() {
        assertTrue(RulesEngine.isTruthy("completed"));
        assertTrue(RulesEngine.isTruthy("true"));
        assertTrue(RulesEngine.isTruthy(Boolean.TRUE));
        assertTrue(RulesEngine.isTruthy("1"));
        assertFalse(RulesEngine.isTruthy(null));
        assertFalse(RulesEngine.isTruthy("no"));
        assertFalse(RulesEngine.isTruthy(Boolean.FALSE));
    }

    @Test
    void softCompleted_defaultBarIsChannelFunnelTerminal() throws Exception {
        JsonNode a = RulesEngine.M.readTree("{\"channels\":[{\"channel\":\"email\"}]}");
        // email funnel = Delivered, Opened, LinkClicked -> default bar = LinkClicked (terminal)
        assertFalse(RulesEngine.softCompleted(a, "email", "Opened"), "engaged but below the terminal bar");
        assertTrue(RulesEngine.softCompleted(a, "email", "LinkClicked"), "reached terminal");
        assertFalse(RulesEngine.softCompleted(a, "email", null), "no disposition yet");
        assertFalse(RulesEngine.softCompleted(a, "email", ""));
        assertFalse(RulesEngine.softCompleted(a, "email", "Bogus"), "unknown status never counts");
    }

    @Test
    void softCompleted_perActionOverrideMovesTheBar() throws Exception {
        JsonNode a = RulesEngine.M.readTree("{\"channels\":[{\"channel\":\"email\",\"softCompletion\":\"Opened\"}]}");
        assertFalse(RulesEngine.softCompleted(a, "email", "Delivered"), "below the lowered bar");
        assertTrue(RulesEngine.softCompleted(a, "email", "Opened"), "exactly the override bar");
        assertTrue(RulesEngine.softCompleted(a, "email", "LinkClicked"), "past the override bar");
    }

    @Test
    void softCompleted_unknownChannelFallsBackToInboundFunnel() throws Exception {
        JsonNode a = RulesEngine.M.readTree("{\"channels\":[]}");
        // inbound funnel = Presented, Accepted, Completed -> terminal = Completed
        assertTrue(RulesEngine.softCompleted(a, "inbound", "Completed"));
        assertFalse(RulesEngine.softCompleted(a, "inbound", "Accepted"));
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // Opt-out as a BUILT-IN channel-eligibility rule (a Unsubscribe/STOP disposition -> channel ineligible)
    // ══════════════════════════════════════════════════════════════════════════════════════════

    @Test
    void optoutRaw_onlyHardChannelOptOutsGateEligibility() {
        // email Unsubscribe + sms STOP are PERMANENT channel opt-outs: the engine latches them durably and gates that
        // channel ineligible for the member (an always-on compliance channel rule).
        assertEquals("Unsubscribe", RulesEngine.OPTOUT_RAW.get("email"));
        assertEquals("STOP", RulesEngine.OPTOUT_RAW.get("sms"));
        // voice Declined / push Dismissed are negative DISPOSITIONS the model learns from, NOT permanent channel
        // removals (those re-approach; global opt-out is isDNC) -> deliberately NOT eligibility gates.
        assertFalse(RulesEngine.OPTOUT_RAW.containsKey("voice"), "voice Declined is not a permanent channel opt-out");
        assertFalse(RulesEngine.OPTOUT_RAW.containsKey("push"), "push Dismissed is not a permanent channel opt-out");
    }

    @Test
    void channelFunnel_mirrorsActivationLayerDispositionStages() {
        // soft-completion is decided against the SAME raw disposition stages the activation layer emits, so the
        // funnels MUST match (email click-to-soft, sms click, push open, voice complete).
        assertEquals(java.util.List.of("Delivered", "Opened", "LinkClicked"), RulesEngine.CHANNEL_FUNNEL.get("email"));
        assertEquals(java.util.List.of("Delivered", "LinkClicked"), RulesEngine.CHANNEL_FUNNEL.get("sms"));
        assertEquals(java.util.List.of("Delivered", "Opened"), RulesEngine.CHANNEL_FUNNEL.get("push"));
        assertEquals(java.util.List.of("Answered", "Completed"), RulesEngine.CHANNEL_FUNNEL.get("voice"));
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // Operator suppression — the SUPPRESSED set + isOperatorSuppressed gate AND-ed into eligibility.
    // (The engine ANDs `!isOperatorSuppressed(aid, ch)` into the per-action-channel `eligible` so a
    //  suppressed action goes ineligible on the next eval, which is what makes the action-router's
    //  in-flight SUPPRESS cancel it. Here we test the gate's pure membership semantics + the
    //  ACTION_SUPPRESS toggle that feeds the set from the definitions stream.)
    // ══════════════════════════════════════════════════════════════════════════════════════════

    @Test
    void isOperatorSuppressed_emptySet_neverSuppresses() {
        // the steady state (nothing suppressed) must NOT gate anything — the AND term is a no-op.
        assertFalse(RulesEngine.isOperatorSuppressed("act_x", "email"));
        assertFalse(RulesEngine.isOperatorSuppressed("act_x", "sms"));
    }

    @Test
    void isOperatorSuppressed_wholeAction_gatesEveryChannel() {
        // target = {actionId} suppresses the action on ALL channels.
        RulesEngine.SUPPRESSED.add("act_x");
        assertTrue(RulesEngine.isOperatorSuppressed("act_x", "email"));
        assertTrue(RulesEngine.isOperatorSuppressed("act_x", "sms"));
        assertTrue(RulesEngine.isOperatorSuppressed("act_x", "voice"));
        assertFalse(RulesEngine.isOperatorSuppressed("act_y", "email"), "a different action is untouched");
    }

    @Test
    void isOperatorSuppressed_actionChannel_gatesOnlyThatChannel() {
        // target = {actionId}.{channel} suppresses ONLY that action-channel; the action stays eligible elsewhere.
        RulesEngine.SUPPRESSED.add("act_x.email");
        assertTrue(RulesEngine.isOperatorSuppressed("act_x", "email"));
        assertFalse(RulesEngine.isOperatorSuppressed("act_x", "sms"), "sibling channel is NOT suppressed");
        assertFalse(RulesEngine.isOperatorSuppressed("act_x", "voice"));
    }

    @Test
    void applyActionSuppress_trueSuppresses_falseOrTombstoneUnsuppresses() throws Exception {
        // value={value:true} -> add; value={value:false} -> remove; null (tombstone) -> remove.
        RulesEngine.applyActionSuppress("act_x", "{\"value\":true}");
        assertTrue(RulesEngine.isOperatorSuppressed("act_x", "email"));

        RulesEngine.applyActionSuppress("act_x", "{\"value\":false}");
        assertFalse(RulesEngine.isOperatorSuppressed("act_x", "email"), "value=false un-suppresses");

        RulesEngine.applyActionSuppress("act_x", "{\"value\":true}");
        assertTrue(RulesEngine.isOperatorSuppressed("act_x", "email"));
        RulesEngine.applyActionSuppress("act_x", null);
        assertFalse(RulesEngine.isOperatorSuppressed("act_x", "email"), "null tombstone un-suppresses");
    }

    @Test
    void applyActionSuppress_missingValueField_defaultsToUnsuppressed() throws Exception {
        // a fact with no `value` field is treated as NOT suppressed (asBoolean(false)) — fail-open.
        RulesEngine.SUPPRESSED.add("act_x");
        RulesEngine.applyActionSuppress("act_x", "{\"actionId\":\"act_x\"}");
        assertFalse(RulesEngine.isOperatorSuppressed("act_x", "email"));
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // SCORE TTL (NBA_SCORE_TTL_SECONDS) — a stale nba.score.* fact is DROPPED from the evaluation so
    // the router never acts on it; the eligible-but-unscored eval then re-triggers the scorer. The TTL
    // gate is a pure helper (scoreExpired) so it is testable with a pinned "now" — production still
    // passes SCORE_TTL_MS (env-defaulted) + System.currentTimeMillis(). Contract under test:
    //   - past the TTL: an nba.score.{a}.{c} fact is dropped (so score key is absent / score=null);
    //   - within the TTL: a FRESH score is KEPT;
    //   - completion / actionstate / milestone / disposition facts NEVER expire (only nba.score.*);
    //   - TTL=0 (default) drops NOTHING (carry-forever, prior behavior).
    // ══════════════════════════════════════════════════════════════════════════════════════════

    /** A snapshot fact envelope {value,valueType,eventTs,source} — the shape the eval gates on (eventTs is what ages). */
    static JsonNode fact(Object value, long eventTs) {
        ObjectNode fv = RulesEngine.M.createObjectNode();
        if (value == null) fv.putNull("value");
        else if (value instanceof Boolean b) fv.put("value", b);
        else if (value instanceof Integer i) fv.put("value", i);
        else if (value instanceof Long l) fv.put("value", l);
        else if (value instanceof Double d) fv.put("value", d);
        else fv.put("value", value.toString());
        fv.put("eventTs", eventTs);
        return fv;
    }

    static final long TTL_60S = 60_000L;
    static final long NOW = 1_000_000_000_000L;   // a fixed wall clock so the test is deterministic

    @Test
    void scoreExpired_staleScore_isDropped() {
        // a score whose eventTs is OLDER than the 60s TTL -> EXPIRED (dropped from the eval -> score=null -> router skips it).
        long staleTs = NOW - (TTL_60S + 1_000L);   // 61s old
        assertTrue(RulesEngine.scoreExpired("nba.score.act1.email", fact(0.91d, staleTs), TTL_60S, NOW),
                "score 61s old with a 60s TTL is stale -> dropped");
    }

    @Test
    void scoreExpired_freshScore_isKept() {
        // a score within the TTL window -> NOT expired (kept; the router can act on it).
        long freshTs = NOW - (TTL_60S - 1_000L);   // 59s old
        assertFalse(RulesEngine.scoreExpired("nba.score.act1.email", fact(0.91d, freshTs), TTL_60S, NOW),
                "score 59s old with a 60s TTL is fresh -> kept");
        // exactly AT the TTL boundary is still kept (gate is strictly-greater-than: now-ets > ttl).
        assertFalse(RulesEngine.scoreExpired("nba.score.act1.email", fact(0.91d, NOW - TTL_60S), TTL_60S, NOW),
                "exactly at the TTL boundary is kept (strict >)");
    }

    @Test
    void scoreExpired_onlyScoreFactsExpire_durableFactsNeverDrop() {
        // Even with an ancient eventTs, NON-score facts are durable and NEVER expire — only nba.score.* ages out.
        long ancientTs = NOW - (TTL_60S * 10_000L);   // absurdly old
        assertFalse(RulesEngine.scoreExpired("nba.completion.act1", fact(true, ancientTs), TTL_60S, NOW),
                "completions are durable -> never dropped");
        assertFalse(RulesEngine.scoreExpired("nba.actionstate.act1.email", fact("PRESENTED", ancientTs), TTL_60S, NOW),
                "lifecycle states are durable -> never dropped");
        assertFalse(RulesEngine.scoreExpired("nba.milestone.m1", fact(true, ancientTs), TTL_60S, NOW),
                "milestones are durable -> never dropped");
        assertFalse(RulesEngine.scoreExpired("nba.disposition.act1.email", fact("Unsubscribe", ancientTs), TTL_60S, NOW),
                "dispositions are durable -> never dropped");
        // ...and a stale SCORE with the same ancient eventTs DOES drop — proving the prefix is what gates it.
        assertTrue(RulesEngine.scoreExpired("nba.score.act1.email", fact(0.5d, ancientTs), TTL_60S, NOW),
                "same ancient eventTs on a score -> dropped (only nba.score.* expires)");
    }

    @Test
    void scoreExpired_ttlZero_dropsNothing_carryForever() {
        // TTL=0 (the default / NBA_SCORE_TTL_SECONDS unset) DISABLES the gate — even an ancient score is carried forever.
        long ancientTs = NOW - (TTL_60S * 10_000L);
        assertFalse(RulesEngine.scoreExpired("nba.score.act1.email", fact(0.5d, ancientTs), 0L, NOW),
                "TTL=0 -> gate disabled -> nothing dropped (prior behavior)");
        // a negative TTL is also treated as disabled (defensive).
        assertFalse(RulesEngine.scoreExpired("nba.score.act1.email", fact(0.5d, ancientTs), -1L, NOW));
    }

    @Test
    void scoreExpired_scoreWithoutEventTs_isKept() {
        // a score whose envelope carries no usable eventTs (0 / absent) can't be aged -> KEPT (we never drop a score we can't time).
        assertFalse(RulesEngine.scoreExpired("nba.score.act1.email", fact(0.91d, 0L), TTL_60S, NOW),
                "eventTs=0 -> not ageable -> kept");
        ObjectNode noTs = RulesEngine.M.createObjectNode();
        noTs.put("value", 0.91d);   // no eventTs field at all
        assertFalse(RulesEngine.scoreExpired("nba.score.act1.email", noTs, TTL_60S, NOW),
                "missing eventTs -> not ageable -> kept");
    }
}
