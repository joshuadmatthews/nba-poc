package ai.das.nba.temporal;

import java.util.Map;

/**
 * Raw provider disposition status -> canonical DELIVERY state. This mapping is OWNED BY THE STATE MACHINE, not
 * the sender: the action-layer (a simulated provider / real webhook) reports ONLY the raw provider status on
 * the {@code nba.disposition.*} fact ({@code value} = raw); the workflow classifies it here to decide its
 * transition. Keeping the taxonomy in the state machine means the dumb adapter never has to know the lifecycle
 * states, and there is one source of truth for "what does this raw status mean for the lifecycle."
 *
 * The taxonomy mirrors the Flink engine's copy ({@code StateMachineFn}/{@code DispositionClassifier}) verbatim so
 * the two flavors stay decision-equivalent. Raw fail/decline labels are globally unique per channel
 * (Bounced/Undelivered/Failed/NoAnswer, Unsubscribe/STOP/Dismissed/Declined), so a channel-agnostic lookup is
 * exact; every other delivery status (Delivered/Opened/LinkClicked/Answered/Completed/Presented/Accepted/…)
 * means "it reached the member" -> PRESENTED.
 */
public final class DispositionClassifier {
    public static final String IN_PROCESS = "IN_PROCESS", PRESENTED = "PRESENTED",
            DECLINED = "DECLINED", FAILED = "FAILED";

    public static final String SUPPRESSED = "SUPPRESSED", SUPPRESS_FAILED = "SUPPRESS_FAILED";

    private static final Map<String, String> CANONICAL = Map.ofEntries(
            Map.entry("Bounced", FAILED), Map.entry("Undelivered", FAILED),
            Map.entry("Failed", FAILED), Map.entry("NoAnswer", FAILED),
            Map.entry("Unsubscribe", DECLINED), Map.entry("STOP", DECLINED),
            Map.entry("Dismissed", DECLINED), Map.entry("Declined", DECLINED),
            // cancel responses to a SUPPRESS activation (the action-layer's raw labels for the two outcomes)
            Map.entry("Cancelled", SUPPRESSED), Map.entry("AlreadySent", SUPPRESS_FAILED));

    /** Classify a raw provider status into the canonical lifecycle state the workflow transitions on.
     *  Already-canonical states (SUPPRESSED/SUPPRESS_FAILED, or any known canonical) pass through unchanged, so
     *  this is safe to apply even if a producer sends canonical directly. Unknown delivery statuses
     *  (Delivered/Opened/LinkClicked/Answered/Completed/Presented/Accepted/…) mean "reached the member" -> PRESENTED. */
    public static String classify(String raw) {
        if (raw == null || raw.isEmpty()) return PRESENTED;
        if (SUPPRESSED.equals(raw) || SUPPRESS_FAILED.equals(raw)
                || DECLINED.equals(raw) || FAILED.equals(raw) || PRESENTED.equals(raw) || IN_PROCESS.equals(raw)) return raw;
        return CANONICAL.getOrDefault(raw, PRESENTED);
    }

    private DispositionClassifier() {}
}
