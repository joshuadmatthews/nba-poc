package ai.das.nba.temporal;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Operator-suppression orchestrator — one run per suppress event (id nba-suppress:{target}:{eventTs}), started when
 * an ACTION_SUPPRESS lands on the definitions topic. It fans the pull out to EVERY running ChannelActionWorkflow for
 * the action via a Temporal Batch Operation (signal operatorSuppress over a Visibility query) — server-side, so it
 * scales to millions without the orchestrator signalling each child itself.
 *
 * Each reached workflow then suppresses in the way it can: PRE-send -> SUPPRESSED; POST-send -> a CANCEL activation
 * the unified activation layer answers SUPPRESSED (still held — e.g. an email batched for later in the day) or
 * SUPPRESS_FAILED (already physically sent -> the funnel proceeds). New dispatch is stopped at the source by the
 * rules-engine now marking suppressed action-channels ineligible; this workflow handles the in-flight ones.
 */
@WorkflowInterface
public interface SuppressionWorkflow {
    /** Suppress every running workflow for actionId. channel="" / null = the whole action; else only that channel. */
    @WorkflowMethod
    void suppress(String actionId, String channel);
}
