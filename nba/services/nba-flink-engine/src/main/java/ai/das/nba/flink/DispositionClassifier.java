package ai.das.nba.flink;

import java.util.Map;

/**
 * Raw provider disposition status -> canonical lifecycle state — OWNED BY THE STATE MACHINE (StateMachineFn),
 * not the sender (ActionLayerFn). The action-layer reports only the raw provider status on the
 * {@code nba.disposition.*} fact ({@code value} = raw); the state machine classifies it here to decide its
 * transition. Kept verbatim with the Temporal engine's copy ({@code nba-temporal/DispositionClassifier}) so the
 * two flavors stay decision-equivalent.
 */
public final class DispositionClassifier {
    public static final String IN_PROCESS = "IN_PROCESS", PRESENTED = "PRESENTED",
            DECLINED = "DECLINED", FAILED = "FAILED", SUPPRESSED = "SUPPRESSED", SUPPRESS_FAILED = "SUPPRESS_FAILED";

    private static final Map<String, String> CANONICAL = Map.ofEntries(
            Map.entry("Bounced", FAILED), Map.entry("Undelivered", FAILED),
            Map.entry("Failed", FAILED), Map.entry("NoAnswer", FAILED),
            Map.entry("Unsubscribe", DECLINED), Map.entry("STOP", DECLINED),
            Map.entry("Dismissed", DECLINED), Map.entry("Declined", DECLINED),
            Map.entry("Cancelled", SUPPRESSED), Map.entry("AlreadySent", SUPPRESS_FAILED));

    /** Raw provider status -> canonical state; already-canonical values pass through (safe during migration).
     *  Unknown delivery statuses (Delivered/Opened/LinkClicked/Answered/Completed/Presented/Accepted/…) -> PRESENTED. */
    public static String classify(String raw) {
        if (raw == null || raw.isEmpty()) return PRESENTED;
        if (SUPPRESSED.equals(raw) || SUPPRESS_FAILED.equals(raw)
                || DECLINED.equals(raw) || FAILED.equals(raw) || PRESENTED.equals(raw) || IN_PROCESS.equals(raw)) return raw;
        return CANONICAL.getOrDefault(raw, PRESENTED);
    }

    private DispositionClassifier() {}
}
