package ai.das.nba.temporal;

import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Per (member, channel) BATCH orchestrator. The router sends the top-N actions on the winning channel
 * as one CREATE (actions[]). This workflow debounces + dedupes them, dispatches the batch as ONE
 * activation to the action-layer (one comm, N content keys), spins up one tracking child workflow per
 * action (each follows that action's disposition through to sent/expired), then terminates — its job
 * was getting the batch out. Workflow id: nba-batch:{nbaId}:{channel}.
 */
@WorkflowInterface
public interface BatchOrchestratorWorkflow {
    @WorkflowMethod
    void orchestrate(long debounceSeconds);

    /** New / updated batch for this (member, channel) — resets the debounce so the latest top-N wins. */
    @SignalMethod
    void updateBatch(Activation batch);

    /** The whole batch fell out of eligibility before dispatch — cancel without sending. */
    @SignalMethod
    void suppress();
}
