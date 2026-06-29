package ai.das.nba.actionlib;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Component-level unit tests for the PURE static core of the NBA action-library hot path — the
 * decision logic the synchronous /next-action + /disposition fast path is built on, none of which
 * needs Redis / Postgres / the nba-kie-server / nba-model HTTP hops:
 *
 *   - {@link ActionLibrary#jsonScalar} / {@link ActionLibrary#truthy}: typed fact coercion + truthiness.
 *   - {@link ActionLibrary#mergeFact}: the event-time LWW merge the snapshot+inbound-facts blend relies on.
 *   - {@link ActionLibrary#channelOptedOut}: hard channel opt-out derived off the durable disposition facts.
 *   - {@link ActionLibrary#inprocEligible}: the in-process eligibility FLOOR (DNC / sms-consent / opt-out
 *     gates + the cached action catalog) used when mode=inproc skips the Drools hop.
 *   - {@link ActionLibrary#isSuppressed}: the operator-suppression strip applied to every eligible set.
 *   - {@link ActionLibrary#funnelFor}: the per-channel disposition funnel.
 *
 * Tests assert ACTUAL observed behavior; subtle/arguably-surprising behavior (truthy's Number>=1 rule,
 * mergeFact's equal-eventTs last-write-wins) is named in the test rather than "fixed".
 */
class ActionLibraryTest {

    // ── tiny builders ──────────────────────────────────────────────────────────────────────────
    /** A fact node {value, valueType?, eventTs} — the shape the snapshot/hot path carry per fact. */
    static ObjectNode fact(Object value, String valueType, long eventTs) {
        ObjectNode f = ActionLibrary.M.createObjectNode();
        if (value == null) f.putNull("value");
        else if (value instanceof Boolean b) f.put("value", b);
        else if (value instanceof Integer i) f.put("value", i);
        else if (value instanceof Long l) f.put("value", l);
        else if (value instanceof Double d) f.put("value", d);
        else f.put("value", value.toString());
        if (valueType != null) f.put("valueType", valueType);
        f.put("eventTs", eventTs);
        return f;
    }

    /** An action catalog doc with the given channels — what {@code catalog()} returns per actionId. */
    static JsonNode actionDoc(String... channels) {
        ObjectNode d = ActionLibrary.M.createObjectNode();
        ArrayNode chs = d.putArray("channels");
        for (String c : channels) chs.addObject().put("channel", c);
        return d;
    }

    static Map<String, Object> flat(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    /** action ids of an eligibility result (each entry is {actionId, channel}). */
    static Set<String> actionIds(List<String[]> elig) {
        return elig.stream().map(ac -> ac[0]).collect(Collectors.toSet());
    }

    // Shared static state the catalog + suppression read — reset around every test.
    @BeforeEach
    @AfterEach
    void resetSharedState() {
        ActionLibrary.SUPPRESSED.clear();
        ActionLibrary.ACTION_DOCS = java.util.Collections.emptyMap();
        ActionLibrary.CAT_AT = 0;
    }

    /** Seed the in-memory catalog and stamp CAT_AT recent so {@code catalog()} serves it without a DB hit. */
    static void seedCatalog(Map<String, JsonNode> docs) {
        ActionLibrary.ACTION_DOCS = docs;
        ActionLibrary.CAT_AT = System.currentTimeMillis();
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // jsonScalar — JsonNode -> Java scalar (drives the flat KIE fact map)
    // ══════════════════════════════════════════════════════════════════════════════════════════

    @Test
    void jsonScalar_coercesByJsonType() throws Exception {
        assertNull(ActionLibrary.jsonScalar((JsonNode) null));
        assertNull(ActionLibrary.jsonScalar(ActionLibrary.M.readTree("null")));
        assertEquals(Boolean.TRUE, ActionLibrary.jsonScalar(ActionLibrary.M.readTree("true")));
        assertEquals(5L, ActionLibrary.jsonScalar(ActionLibrary.M.readTree("5")), "integral -> Long");
        assertEquals(2.5d, ActionLibrary.jsonScalar(ActionLibrary.M.readTree("2.5")), "fractional -> Double");
        assertEquals("hi", ActionLibrary.jsonScalar(ActionLibrary.M.readTree("\"hi\"")));
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // truthy — DNC / smsConsent gate inputs
    // ══════════════════════════════════════════════════════════════════════════════════════════

    @Test
    void truthy_booleansAndNulls() {
        assertFalse(ActionLibrary.truthy(null));
        assertTrue(ActionLibrary.truthy(Boolean.TRUE));
        assertFalse(ActionLibrary.truthy(Boolean.FALSE));
    }

    @Test
    void truthy_numbers_useGreaterThanOrEqualOne() {
        // the rule is n >= 1 — NOT n != 0. A fractional 0.5 is falsey; exactly 1 is truthy.
        assertTrue(ActionLibrary.truthy(1));
        assertTrue(ActionLibrary.truthy(2L));
        assertFalse(ActionLibrary.truthy(0));
        assertFalse(ActionLibrary.truthy(0.5d), "0.5 is below the >=1 threshold");
    }

    @Test
    void truthy_strings_wordsAndNumericText() {
        assertTrue(ActionLibrary.truthy("true"));
        assertTrue(ActionLibrary.truthy("T"));
        assertTrue(ActionLibrary.truthy("yes"));
        assertTrue(ActionLibrary.truthy("1"));
        assertTrue(ActionLibrary.truthy("2"));
        assertFalse(ActionLibrary.truthy("0"));
        assertFalse(ActionLibrary.truthy("no"));
        assertFalse(ActionLibrary.truthy("nonsense"), "non-true non-numeric text is falsey");
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // mergeFact — event-time LWW into BOTH the structured node (model) and the flat map (KIE)
    // ══════════════════════════════════════════════════════════════════════════════════════════

    @Test
    void mergeFact_firstWrite_setsStructAndFlatScalar() {
        ObjectNode struct = ActionLibrary.M.createObjectNode();
        Map<String, Object> flat = new HashMap<>();
        ActionLibrary.mergeFact(struct, flat, "riskScore", fact(7L, "LONG", 100));
        assertEquals(7L, struct.get("riskScore").get("value").asLong());
        assertEquals(7L, flat.get("riskScore"), "flat carries the coerced scalar for KIE");
    }

    @Test
    void mergeFact_newerEventTsWins_olderIsIgnored() {
        ObjectNode struct = ActionLibrary.M.createObjectNode();
        Map<String, Object> flat = new HashMap<>();
        ActionLibrary.mergeFact(struct, flat, "k", fact("old", "STRING", 100));
        ActionLibrary.mergeFact(struct, flat, "k", fact("new", "STRING", 200));   // newer -> wins
        assertEquals("new", flat.get("k"));
        ActionLibrary.mergeFact(struct, flat, "k", fact("stale", "STRING", 150)); // older than 200 -> ignored
        assertEquals("new", flat.get("k"), "an out-of-order older event does NOT clobber the newer value");
        assertEquals("new", struct.get("k").get("value").asText());
    }

    @Test
    void mergeFact_equalEventTs_lastWriteWins() {
        // the guard is strictly `cur.eventTs > newTs`, so an equal-timestamp write proceeds (overwrites).
        ObjectNode struct = ActionLibrary.M.createObjectNode();
        Map<String, Object> flat = new HashMap<>();
        ActionLibrary.mergeFact(struct, flat, "k", fact("a", "STRING", 100));
        ActionLibrary.mergeFact(struct, flat, "k", fact("b", "STRING", 100));
        assertEquals("b", flat.get("k"), "same eventTs -> last write wins");
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // channelOptedOut — hard per-channel opt-out off the durable disposition facts
    // ══════════════════════════════════════════════════════════════════════════════════════════

    @Test
    void channelOptedOut_emailUnsubscribeAndSmsStop() {
        assertTrue(ActionLibrary.channelOptedOut(flat("nba.disposition.act_1.email", "Unsubscribe"), "email"));
        assertTrue(ActionLibrary.channelOptedOut(flat("nba.disposition.act_1.sms", "STOP"), "sms"));
    }

    @Test
    void channelOptedOut_isScopedToTheChannelAndExactStatus() {
        // an Unsubscribe on email does not opt the member out of sms.
        assertFalse(ActionLibrary.channelOptedOut(flat("nba.disposition.act_1.email", "Unsubscribe"), "sms"));
        // a non-opt-out disposition (a normal Delivered) is not an opt-out.
        assertFalse(ActionLibrary.channelOptedOut(flat("nba.disposition.act_1.email", "Delivered"), "email"));
    }

    @Test
    void channelOptedOut_channelsWithoutAHardOptOutAreNeverOptedOut() {
        // only email/sms have a hard opt-out raw status; push/voice negatives are dispositions, not opt-outs.
        assertFalse(ActionLibrary.channelOptedOut(flat("nba.disposition.act_1.push", "Dismissed"), "push"));
        assertFalse(ActionLibrary.channelOptedOut(flat("nba.disposition.act_1.voice", "Declined"), "voice"));
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // inprocEligible — the in-process eligibility floor (gates + catalog channel match), no Drools hop
    // ══════════════════════════════════════════════════════════════════════════════════════════

    @Test
    void inprocEligible_returnsCatalogActionsOfferingTheChannel() {
        seedCatalog(Map.of(
                "act_e", actionDoc("email"),
                "act_s", actionDoc("sms"),
                "act_both", actionDoc("email", "sms")));
        Set<String> email = actionIds(ActionLibrary.inprocEligible("nba_1", "email", flat(), null));
        assertEquals(Set.of("act_e", "act_both"), email, "only actions offering email");
        assertFalse(email.contains("act_s"));
    }

    @Test
    void inprocEligible_dncBlocksEverything() {
        seedCatalog(Map.of("act_e", actionDoc("email")));
        assertTrue(ActionLibrary.inprocEligible("nba_1", "email", flat("isDNC", true), null).isEmpty(),
                "a Do-Not-Contact member is eligible for nothing");
    }

    @Test
    void inprocEligible_smsRequiresConsent() {
        seedCatalog(Map.of("act_s", actionDoc("sms")));
        assertTrue(ActionLibrary.inprocEligible("nba_1", "sms", flat(), null).isEmpty(),
                "sms with no consent on record -> ineligible");
        assertEquals(Set.of("act_s"),
                actionIds(ActionLibrary.inprocEligible("nba_1", "sms", flat("smsConsent", true), null)),
                "sms with consent -> eligible");
    }

    @Test
    void inprocEligible_optedOutChannelYieldsNothing() {
        seedCatalog(Map.of("act_e", actionDoc("email")));
        Map<String, Object> f = flat("nba.disposition.act_e.email", "Unsubscribe");
        assertTrue(ActionLibrary.inprocEligible("nba_1", "email", f, null).isEmpty(),
                "a hard email opt-out gates the whole channel");
    }

    @Test
    void inprocEligible_channelWithNoCatalogActionIsEmpty() {
        seedCatalog(Map.of("act_e", actionDoc("email")));
        assertTrue(ActionLibrary.inprocEligible("nba_1", "voice", flat(), null).isEmpty());
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // isSuppressed — operator-suppression strip (whole action vs action-channel)
    // ══════════════════════════════════════════════════════════════════════════════════════════

    @Test
    void isSuppressed_emptySetNeverSuppresses() {
        assertFalse(ActionLibrary.isSuppressed("act_x", "email"));
    }

    @Test
    void isSuppressed_wholeActionGatesEveryChannel() {
        ActionLibrary.SUPPRESSED.add("act_x");
        assertTrue(ActionLibrary.isSuppressed("act_x", "email"));
        assertTrue(ActionLibrary.isSuppressed("act_x", "sms"));
        assertFalse(ActionLibrary.isSuppressed("act_y", "email"));
    }

    @Test
    void isSuppressed_actionChannelGatesOnlyThatChannel() {
        ActionLibrary.SUPPRESSED.add("act_x.email");
        assertTrue(ActionLibrary.isSuppressed("act_x", "email"));
        assertFalse(ActionLibrary.isSuppressed("act_x", "sms"), "sibling channel stays eligible");
    }

    @Test
    void inprocEligible_doesNotItselfStripSuppressed_thatIsAppliedAfter() {
        // inprocEligible is the channel-candidate floor; the SUPPRESSED strip is applied by hotPathDecide AFTER
        // (over both inproc + kie results). Documenting that boundary so the suppression contract stays clear.
        seedCatalog(Map.of("act_e", actionDoc("email")));
        ActionLibrary.SUPPRESSED.add("act_e");
        assertEquals(Set.of("act_e"), actionIds(ActionLibrary.inprocEligible("nba_1", "email", flat(), null)),
                "the floor still lists it; isSuppressed(\"act_e\",\"email\") is what removes it downstream");
        assertTrue(ActionLibrary.isSuppressed("act_e", "email"));
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // funnelFor — per-channel disposition funnel (default = inbound)
    // ══════════════════════════════════════════════════════════════════════════════════════════

    @Test
    void funnelFor_knownChannels_andInboundDefault() {
        assertEquals("Delivered", ActionLibrary.funnelFor("email").get(0), "email funnel starts at Delivered");
        // an unknown channel falls back to the INBOUND funnel (Presented/Accepted/Completed).
        assertEquals(ActionLibrary.INBOUND_FUNNEL, ActionLibrary.funnelFor("carrier-pigeon"));
        assertEquals("Presented", ActionLibrary.funnelFor("carrier-pigeon").get(0));
    }
}
