package ai.das.nba.kie;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kie.api.KieBase;
import org.kie.api.runtime.KieSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Component-level unit tests for the NBA KIE decision service.
 *
 * Three layers are exercised:
 *   1. {@link Snap} — the working-memory fact's get/getOr accessors.
 *   2. DRL generation — {@code buildDrl}/{@code exprForTree}/{@code exprForCond}/{@code andRules}:
 *      string-shape assertions that the generated DRL carries the right rule names, conditions and
 *      fact references for a representative action+rule definition.
 *   3. END-TO-END decision — Drools IS on the classpath, so we drive the exact KieServer build path
 *      ({@code rebuild()} -> {@link KieServer#kieBase}), open a real KieSession, insert a {@link Snap},
 *      {@code fireAllRules}, and assert the {@code hits} are EXACTLY what the rule logic implies.
 *      This proves the eval path, not just string generation.
 *
 * No visibility changes were required: every method/field the tests touch is already package-private
 * (static) on {@code KieServer}, and {@code Snap}'s accessors are public.
 *
 * NOTE on shared static state: KieServer holds the definition maps and the compiled KieBase as static
 * fields. Each test clears them first (see {@link #reset()}) so tests don't contaminate each other.
 */
class KieServerTest {
    static final ObjectMapper M = new ObjectMapper();

    static JsonNode json(String s) {
        try { return M.readTree(s); } catch (Exception e) { throw new RuntimeException(e); }
    }

    /** Wipe the shared static definition stores + compiled KieBase before each test. */
    @BeforeEach
    void reset() {
        KieServer.actions.clear();
        KieServer.globalRules.clear();
        KieServer.channelRules.clear();
        KieServer.kieBase = null;
    }

    /** Run the SAME path /evaluate does: build a Snap from facts, fire the rebuilt KieBase, return hits. */
    static List<String> evalHits(String nbaId, Map<String, Object> facts) {
        KieServer.rebuild();
        KieBase kb = KieServer.kieBase;
        assertNotNull(kb, "rebuild() must have produced a compilable KieBase");
        List<String> hits = new ArrayList<>();
        KieSession session = kb.newKieSession();
        try {
            session.setGlobal("results", hits);
            session.insert(new Snap(nbaId, facts));
            session.fireAllRules();
        } finally {
            session.dispose();
        }
        return hits;
    }

    static Map<String, Object> facts(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    // ───────────────────────────── 1. Snap accessors ─────────────────────────────

    @Test
    void snapGetReturnsPresentValueAndNullForMissing() {
        Snap s = new Snap("nba_1", facts("score", 42L, "flag", Boolean.TRUE, "tier", "gold"));
        assertEquals(42L, s.get("score"));
        assertEquals(Boolean.TRUE, s.get("flag"));
        assertEquals("gold", s.get("tier"));
        assertNull(s.get("absent"), "a missing key returns null");
        assertEquals("nba_1", s.getNbaId());
    }

    @Test
    void snapGetOrReturnsDefaultOnlyWhenKeyAbsent() {
        Snap s = new Snap("nba_1", facts("score", 7L));
        assertEquals(7L, s.getOr("score", 0L), "present key returns the stored value, not the default");
        assertEquals(0L, s.getOr("missing", 0L), "absent key returns the supplied default");
        assertEquals("dflt", s.getOr("missing", "dflt"));
        assertNull(s.getOr("missing", null), "default may itself be null");
    }

    @Test
    void snapGetOrTreatsAStoredNullAsAbsentAndReturnsDefault() {
        // f.get(key) == null both for absent keys AND keys mapped to null -> getOr returns the default.
        Map<String, Object> f = new HashMap<>();
        f.put("nullable", null);
        Snap s = new Snap("nba_1", f);
        assertNull(s.get("nullable"), "explicit null value reads back as null");
        assertEquals("fallback", s.getOr("nullable", "fallback"),
                "a key mapped to null is indistinguishable from absent: getOr returns the default");
    }

    @Test
    void snapToleratesNullFactMap() {
        Snap s = new Snap("nba_1", null);
        assertNull(s.get("anything"), "null fact map -> get is null, not an NPE");
        assertEquals("d", s.getOr("anything", "d"), "null fact map -> getOr returns the default");
    }

    // ───────────────────────── 2. condition/tree -> MVEL expr ─────────────────────────

    @Test
    void exprForCondCoversEveryComparisonOperator() {
        // eq (default), ne, gt, gte, lt, lte over a numeric value
        assertEquals("getOr(\"s\", 0) == 5", KieServer.exprForCond(json("{\"fact\":\"s\",\"cmp\":\"eq\",\"value\":5}")));
        assertEquals("getOr(\"s\", 0) == 5", KieServer.exprForCond(json("{\"fact\":\"s\",\"value\":5}")),
                "missing cmp defaults to eq");
        assertEquals("getOr(\"s\", 0) != 5", KieServer.exprForCond(json("{\"fact\":\"s\",\"cmp\":\"ne\",\"value\":5}")));
        assertEquals("getOr(\"s\", 0) > 5", KieServer.exprForCond(json("{\"fact\":\"s\",\"cmp\":\"gt\",\"value\":5}")));
        assertEquals("getOr(\"s\", 0) >= 5", KieServer.exprForCond(json("{\"fact\":\"s\",\"cmp\":\"gte\",\"value\":5}")));
        assertEquals("getOr(\"s\", 0) < 5", KieServer.exprForCond(json("{\"fact\":\"s\",\"cmp\":\"lt\",\"value\":5}")));
        assertEquals("getOr(\"s\", 0) <= 5", KieServer.exprForCond(json("{\"fact\":\"s\",\"cmp\":\"lte\",\"value\":5}")));
    }

    @Test
    void exprForCondExistsBypassesValueAndDefault() {
        assertEquals("get(\"opt.in\") != null",
                KieServer.exprForCond(json("{\"fact\":\"opt.in\",\"cmp\":\"exists\"}")));
    }

    @Test
    void exprForCondRendersValueAndDefaultPerType() {
        // boolean: default false, value verbatim
        assertEquals("getOr(\"b\", false) == true",
                KieServer.exprForCond(json("{\"fact\":\"b\",\"value\":true}")));
        // string: default empty-string, value quoted (and inner quotes escaped)
        assertEquals("getOr(\"t\", \"\") == \"gold\"",
                KieServer.exprForCond(json("{\"fact\":\"t\",\"value\":\"gold\"}")));
        assertEquals("getOr(\"t\", \"\") == \"a\\\"b\"",
                KieServer.exprForCond(json("{\"fact\":\"t\",\"value\":\"a\\\"b\"}")),
                "embedded quotes in the value are escaped for the DRL");
        // null value: default null, value null
        assertEquals("getOr(\"x\", null) == null",
                KieServer.exprForCond(json("{\"fact\":\"x\",\"value\":null}")));
        // absent value node behaves like null
        assertEquals("getOr(\"x\", null) == null",
                KieServer.exprForCond(json("{\"fact\":\"x\"}")));
    }

    @Test
    void exprForCondEmptyFactYieldsEmptyString() {
        assertEquals("", KieServer.exprForCond(json("{\"cmp\":\"gt\",\"value\":5}")),
                "a condition with no fact contributes nothing");
    }

    @Test
    void exprForTreeJoinsWithAndForAllAndOrForAny() {
        String all = KieServer.exprForTree(json(
                "{\"op\":\"all\",\"conditions\":[{\"fact\":\"a\",\"cmp\":\"gt\",\"value\":1}," +
                        "{\"fact\":\"b\",\"cmp\":\"lt\",\"value\":9}]}"));
        assertEquals("(getOr(\"a\", 0) > 1 && getOr(\"b\", 0) < 9)", all, "op=all -> &&-joined, parenthesised");

        String any = KieServer.exprForTree(json(
                "{\"op\":\"any\",\"conditions\":[{\"fact\":\"a\",\"cmp\":\"gt\",\"value\":1}," +
                        "{\"fact\":\"b\",\"cmp\":\"lt\",\"value\":9}]}"));
        assertEquals("(getOr(\"a\", 0) > 1 || getOr(\"b\", 0) < 9)", any, "op=any -> ||-joined");

        assertEquals("(getOr(\"a\", 0) > 1 && getOr(\"b\", 0) < 9)",
                KieServer.exprForTree(json("{\"conditions\":[{\"fact\":\"a\",\"cmp\":\"gt\",\"value\":1}," +
                        "{\"fact\":\"b\",\"cmp\":\"lt\",\"value\":9}]}")),
                "missing op defaults to all (&&)");
    }

    @Test
    void exprForTreeEmptyForNullMissingOrEmptyConditions() {
        assertEquals("", KieServer.exprForTree(null));
        assertEquals("", KieServer.exprForTree(json("null")));
        assertEquals("", KieServer.exprForTree(json("{\"op\":\"all\"}")), "no conditions array -> empty");
        assertEquals("", KieServer.exprForTree(json("{\"op\":\"all\",\"conditions\":[]}")), "empty conditions -> empty");
        // a tree whose only condition is fact-less collapses to empty (no stray parens)
        assertEquals("", KieServer.exprForTree(json("{\"conditions\":[{\"cmp\":\"gt\",\"value\":5}]}")));
    }

    @Test
    void andRulesAndsTogetherEveryRulesLogicTree() {
        List<JsonNode> rules = List.of(
                json("{\"logic\":{\"conditions\":[{\"fact\":\"a\",\"cmp\":\"gt\",\"value\":1}]}}"),
                json("{\"logic\":{\"conditions\":[{\"fact\":\"b\",\"cmp\":\"eq\",\"value\":true}]}}"));
        // andRules wraps the &&-joined per-rule gates in an OUTER paren pair.
        assertEquals("((getOr(\"a\", 0) > 1) && (getOr(\"b\", false) == true))", KieServer.andRules(rules));
        assertEquals("", KieServer.andRules(List.of()), "no rules -> empty gate");
        assertEquals("", KieServer.andRules(List.of(json("{\"logic\":{\"conditions\":[]}}"))),
                "rules with empty logic contribute nothing");
    }

    // ───────────────────────── 2b. full DRL string-shape ─────────────────────────

    @Test
    void buildDrlEmitsRuleNameConditionsAndChannelFilteredGate() {
        // ACTION welcome with inclusion (score>=10), one email channel.
        KieServer.actions.put("welcome", json(
                "{\"id\":\"welcome\"," +
                        "\"inclusion\":{\"op\":\"all\",\"conditions\":[{\"fact\":\"score\",\"cmp\":\"gte\",\"value\":10}]}," +
                        "\"exclusion\":{\"op\":\"all\",\"conditions\":[{\"fact\":\"unsub\",\"cmp\":\"eq\",\"value\":true}]}," +
                        "\"channels\":[{\"channel\":\"email\"}]}"));
        // a GLOBAL rule that applies to every action
        KieServer.globalRules.put("consent", json(
                "{\"logic\":{\"conditions\":[{\"fact\":\"consent\",\"cmp\":\"eq\",\"value\":true}]}}"));
        // a CHANNEL rule for email (non-.rate so it is included as a gate)
        KieServer.channelRules.put("email_ok", json(
                "{\"channel\":\"email\",\"logic\":{\"conditions\":[{\"fact\":\"email.ok\",\"cmp\":\"eq\",\"value\":true}]}}"));

        String drl = KieServer.buildDrl();

        assertTrue(drl.contains("import ai.das.nba.kie.Snap;"), "imports Snap");
        assertTrue(drl.contains("global java.util.List results;"), "declares the results global");
        assertTrue(drl.contains("rule \"elig::welcome::email\""), "rule named elig::<action>::<channel>: \n" + drl);
        assertTrue(drl.contains("dialect \"mvel\""), "mvel dialect");
        // inclusion + global + channel gate all && together inside the positive Snap(...) pattern
        assertTrue(drl.contains("(getOr(\"score\", 0) >= 10)"), "inclusion present");
        assertTrue(drl.contains("(getOr(\"consent\", false) == true)"), "global rule gate present");
        assertTrue(drl.contains("(getOr(\"email.ok\", false) == true)"), "channel rule gate present");
        // exclusion becomes a `not Snap(...)` block
        assertTrue(drl.contains("not Snap((getOr(\"unsub\", false) == true))"),
                "exclusion negated via `not Snap(...)`: \n" + drl);
        // consequence emits the action::channel slug
        assertTrue(drl.contains("results.add(\"welcome::email\");"), "consequence adds the hit slug");
    }

    @Test
    void buildDrlSkipsActionsWithoutAChannelsArray() {
        KieServer.actions.put("noch", json("{\"id\":\"noch\"}"));
        KieServer.actions.put("nullch", json("{\"id\":\"nullch\",\"channels\":null}"));
        String drl = KieServer.buildDrl();
        assertFalse(drl.contains("rule \"elig::noch"), "an action with no channels emits no rule");
        assertFalse(drl.contains("rule \"elig::nullch"), "channels:null emits no rule");
        // header still present, just no rules
        assertTrue(drl.contains("global java.util.List results;"));
    }

    @Test
    void channelRulesForExcludesRateGateRulesAndOtherChannels() {
        KieServer.channelRules.put("email_attr", json(
                "{\"channel\":\"email\",\"logic\":{\"conditions\":[{\"fact\":\"email.ok\",\"cmp\":\"eq\",\"value\":true}]}}"));
        KieServer.channelRules.put("email_rate", json(
                "{\"channel\":\"email\",\"logic\":{\"conditions\":[{\"fact\":\"email.rate\",\"cmp\":\"lt\",\"value\":3}]}}"));
        KieServer.channelRules.put("sms_attr", json(
                "{\"channel\":\"sms\",\"logic\":{\"conditions\":[{\"fact\":\"sms.ok\",\"cmp\":\"eq\",\"value\":true}]}}"));

        var emailRules = KieServer.channelRulesFor("email");
        assertEquals(1, emailRules.size(), ".rate rules are gate-only and excluded; other channels excluded");
        assertTrue(KieServer.referencesRate(KieServer.channelRules.get("email_rate")));
        assertFalse(KieServer.referencesRate(KieServer.channelRules.get("email_attr")));
        // the surviving rule is the non-rate email attribute rule
        String expr = KieServer.andRules(emailRules);
        assertEquals("((getOr(\"email.ok\", false) == true))", expr);
    }

    // ───────────────────────── 3. END-TO-END decision (real KieSession) ─────────────────────────

    @Test
    void endToEndMemberMeetingInclusionIsEligible() {
        KieServer.actions.put("welcome", json(
                "{\"id\":\"welcome\"," +
                        "\"inclusion\":{\"op\":\"all\",\"conditions\":[{\"fact\":\"score\",\"cmp\":\"gte\",\"value\":10}]}," +
                        "\"channels\":[{\"channel\":\"email\"}]}"));
        List<String> hits = evalHits("nba_1", facts("score", 25L));
        assertEquals(List.of("welcome::email"), hits, "score 25 >= 10 -> eligible for welcome::email");
    }

    @Test
    void endToEndMemberFailingInclusionIsNotEligible() {
        KieServer.actions.put("welcome", json(
                "{\"id\":\"welcome\"," +
                        "\"inclusion\":{\"op\":\"all\",\"conditions\":[{\"fact\":\"score\",\"cmp\":\"gte\",\"value\":10}]}," +
                        "\"channels\":[{\"channel\":\"email\"}]}"));
        assertTrue(evalHits("nba_1", facts("score", 5L)).isEmpty(), "score 5 < 10 -> not eligible");
        assertTrue(evalHits("nba_1", facts()).isEmpty(),
                "no score fact -> getOr default 0 < 10 -> not eligible (default-deny)");
    }

    @Test
    void endToEndExclusionSuppressesAnOtherwiseEligibleMember() {
        KieServer.actions.put("promo", json(
                "{\"id\":\"promo\"," +
                        "\"inclusion\":{\"op\":\"all\",\"conditions\":[{\"fact\":\"score\",\"cmp\":\"gte\",\"value\":10}]}," +
                        "\"exclusion\":{\"op\":\"all\",\"conditions\":[{\"fact\":\"unsub\",\"cmp\":\"eq\",\"value\":true}]}," +
                        "\"channels\":[{\"channel\":\"email\"}]}"));
        assertEquals(List.of("promo::email"), evalHits("nba_1", facts("score", 50L, "unsub", Boolean.FALSE)),
                "meets inclusion, not excluded -> eligible");
        assertTrue(evalHits("nba_1", facts("score", 50L, "unsub", Boolean.TRUE)).isEmpty(),
                "the exclusion fires -> suppressed despite meeting inclusion");
    }

    @Test
    void endToEndGlobalRuleGatesEveryAction() {
        KieServer.actions.put("welcome", json(
                "{\"id\":\"welcome\"," +
                        "\"inclusion\":{\"op\":\"all\",\"conditions\":[{\"fact\":\"score\",\"cmp\":\"gte\",\"value\":10}]}," +
                        "\"channels\":[{\"channel\":\"email\"}]}"));
        KieServer.globalRules.put("consent", json(
                "{\"logic\":{\"conditions\":[{\"fact\":\"consent\",\"cmp\":\"eq\",\"value\":true}]}}"));
        assertEquals(List.of("welcome::email"), evalHits("nba_1", facts("score", 25L, "consent", Boolean.TRUE)),
                "inclusion met AND global consent true -> eligible");
        assertTrue(evalHits("nba_1", facts("score", 25L, "consent", Boolean.FALSE)).isEmpty(),
                "global consent gate false -> no action is eligible");
        assertTrue(evalHits("nba_1", facts("score", 25L)).isEmpty(),
                "global gate fails closed when consent absent (default false)");
    }

    @Test
    void endToEndChannelRuleFiltersPerChannelButRateRulesAreIgnored() {
        // welcome has two channels: email + sms
        KieServer.actions.put("welcome", json(
                "{\"id\":\"welcome\"," +
                        "\"inclusion\":{\"op\":\"all\",\"conditions\":[{\"fact\":\"score\",\"cmp\":\"gte\",\"value\":10}]}," +
                        "\"channels\":[{\"channel\":\"email\"},{\"channel\":\"sms\"}]}"));
        // email requires email.ok==true; sms has only a .rate rule (gate-only -> ignored here, so sms is unconditional)
        KieServer.channelRules.put("email_ok", json(
                "{\"channel\":\"email\",\"logic\":{\"conditions\":[{\"fact\":\"email.ok\",\"cmp\":\"eq\",\"value\":true}]}}"));
        KieServer.channelRules.put("sms_rate", json(
                "{\"channel\":\"sms\",\"logic\":{\"conditions\":[{\"fact\":\"sms.rate\",\"cmp\":\"lt\",\"value\":3}]}}"));

        // email.ok true -> both channels fire
        List<String> both = evalHits("nba_1", facts("score", 20L, "email.ok", Boolean.TRUE));
        assertEquals(2, both.size(), "both channels eligible: " + both);
        assertTrue(both.contains("welcome::email"));
        assertTrue(both.contains("welcome::sms"));

        // email.ok false -> email filtered out, sms still fires (its only rule was a .rate gate, ignored)
        List<String> smsOnly = evalHits("nba_1", facts("score", 20L, "email.ok", Boolean.FALSE));
        assertEquals(List.of("welcome::sms"), smsOnly,
                "email channel rule fails -> only sms; the sms .rate rule is gate-only and not enforced here");
    }

    @Test
    void endToEndMultipleActionsEvaluateIndependently() {
        KieServer.actions.put("low", json(
                "{\"id\":\"low\"," +
                        "\"inclusion\":{\"op\":\"all\",\"conditions\":[{\"fact\":\"score\",\"cmp\":\"gte\",\"value\":10}]}," +
                        "\"channels\":[{\"channel\":\"email\"}]}"));
        KieServer.actions.put("high", json(
                "{\"id\":\"high\"," +
                        "\"inclusion\":{\"op\":\"all\",\"conditions\":[{\"fact\":\"score\",\"cmp\":\"gte\",\"value\":100}]}," +
                        "\"channels\":[{\"channel\":\"email\"}]}"));
        assertEquals(List.of("low::email"), evalHits("nba_1", facts("score", 50L)),
                "score 50 clears low (>=10) but not high (>=100)");
        List<String> both = evalHits("nba_1", facts("score", 150L));
        assertEquals(2, both.size(), "score 150 clears both: " + both);
        assertTrue(both.contains("low::email"));
        assertTrue(both.contains("high::email"));
    }

    @Test
    void endToEndAnyOfInclusionIsAnOrGate() {
        KieServer.actions.put("reengage", json(
                "{\"id\":\"reengage\"," +
                        "\"inclusion\":{\"op\":\"any\",\"conditions\":[" +
                        "{\"fact\":\"opened\",\"cmp\":\"eq\",\"value\":true}," +
                        "{\"fact\":\"clicked\",\"cmp\":\"eq\",\"value\":true}]}," +
                        "\"channels\":[{\"channel\":\"email\"}]}"));
        assertEquals(List.of("reengage::email"), evalHits("nba_1", facts("opened", Boolean.TRUE, "clicked", Boolean.FALSE)),
                "any-of: opened alone is enough");
        assertEquals(List.of("reengage::email"), evalHits("nba_1", facts("opened", Boolean.FALSE, "clicked", Boolean.TRUE)),
                "any-of: clicked alone is enough");
        assertTrue(evalHits("nba_1", facts("opened", Boolean.FALSE, "clicked", Boolean.FALSE)).isEmpty(),
                "any-of: neither -> not eligible");
    }

    @Test
    void endToEndExistsConditionMatchesPresenceRegardlessOfValue() {
        KieServer.actions.put("hasemail", json(
                "{\"id\":\"hasemail\"," +
                        "\"inclusion\":{\"op\":\"all\",\"conditions\":[{\"fact\":\"email.addr\",\"cmp\":\"exists\"}]}," +
                        "\"channels\":[{\"channel\":\"email\"}]}"));
        assertEquals(List.of("hasemail::email"), evalHits("nba_1", facts("email.addr", "a@b.com")),
                "fact present -> exists matches");
        assertTrue(evalHits("nba_1", facts()).isEmpty(), "fact absent -> exists fails");
    }

    @Test
    void endToEndEmptyDefinitionsCompilesToZeroHits() {
        // no actions at all -> KieBase still builds, nothing fires.
        assertTrue(evalHits("nba_1", facts("score", 999L)).isEmpty(), "no definitions -> no hits");
    }
}
