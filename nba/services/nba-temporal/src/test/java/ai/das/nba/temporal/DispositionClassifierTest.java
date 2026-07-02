package ai.das.nba.temporal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Pins the raw->canonical taxonomy the STATE MACHINE owns (the sender reports raw only). Kept in lockstep
 *  with the Flink engine's identical copy — the same table is asserted in StateMachineFnTest, so a divergence
 *  between the two flavors fails one of the suites. */
class DispositionClassifierTest {

    @Test void deliveryStatusesArePresented() {
        assertEquals("PRESENTED", DispositionClassifier.classify("Delivered"));
        assertEquals("PRESENTED", DispositionClassifier.classify("Opened"));
        assertEquals("PRESENTED", DispositionClassifier.classify("LinkClicked"));
        assertEquals("PRESENTED", DispositionClassifier.classify("Answered"));
        assertEquals("PRESENTED", DispositionClassifier.classify("Presented"));   // inbound funnel raw
        assertEquals("PRESENTED", DispositionClassifier.classify("Accepted"));    // inbound funnel raw
        assertEquals("PRESENTED", DispositionClassifier.classify("Completed"));   // inbound funnel raw (goal comes via rules)
    }

    @Test void failuresAndDeclines() {
        assertEquals("FAILED", DispositionClassifier.classify("Bounced"));
        assertEquals("FAILED", DispositionClassifier.classify("Undelivered"));
        assertEquals("FAILED", DispositionClassifier.classify("Failed"));
        assertEquals("FAILED", DispositionClassifier.classify("NoAnswer"));
        assertEquals("DECLINED", DispositionClassifier.classify("Unsubscribe"));
        assertEquals("DECLINED", DispositionClassifier.classify("STOP"));
        assertEquals("DECLINED", DispositionClassifier.classify("Dismissed"));
        assertEquals("DECLINED", DispositionClassifier.classify("Declined"));
    }

    @Test void cancelResponsesAndPassthrough() {
        assertEquals("SUPPRESSED", DispositionClassifier.classify("Cancelled"));
        assertEquals("SUPPRESS_FAILED", DispositionClassifier.classify("AlreadySent"));
        assertEquals("SUPPRESSED", DispositionClassifier.classify("SUPPRESSED"));         // already-canonical
        assertEquals("PRESENTED", DispositionClassifier.classify("PRESENTED"));
        assertEquals("PRESENTED", DispositionClassifier.classify("BrandNewProviderStatus"));  // unknown = delivered
        assertEquals("PRESENTED", DispositionClassifier.classify(""));
        assertEquals("PRESENTED", DispositionClassifier.classify(null));
    }
}
