package ai.das.nba.temporal;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface ActionActivities {
    /** Emit a workflow state-transition as a fact (nba.actionstate.{actionId}.{channel}) so it
     *  flows back up: fact -> snapshot -> rules -> eval, making in-flight state visible. */
    @ActivityMethod
    void emitState(Activation act, String state);

    /** Emit a state-transition with a REASON (e.g. suppressed because the channel saturated for the
     *  day = reason "throttle"). The snapshot-builder forwards throttle-reason suppressions to the
     *  definitions topic so the rules engine marks the channel ineligible. */
    @ActivityMethod
    void emitStateReason(Activation act, String state, String reason);

    /** Emit an activation command for the unified action layer onto nba.activations:
     *  op = "DISPATCH" (send) or "CANCEL". The state machine's "go send/cancel" is an activation. */
    @ActivityMethod
    void emitActivation(Activation act, String op);

    /** Ask the channel throttle gate to admit this action before it hands off to the action layer.
     *  Returns SEND (rate-window token reserved), WAIT (window full but the backlog still clears today —
     *  hold in THROTTLED and trickle), or SUPPRESS (backlog can't clear today — reroute). Unthrottled
     *  channels always SEND. */
    @ActivityMethod
    String throttleGate(Activation act);

    /** Join the channel's THROTTLED backlog (on first WAIT) — feeds the saturation prediction. */
    @ActivityMethod
    void throttleEnterBacklog(Activation act);

    /** Leave the backlog (admitted or suppressed). */
    @ActivityMethod
    void throttleExitBacklog(Activation act);

    /** Dispatch a whole BATCH as ONE activation (op=DISPATCH, channel + actions[] with per-action
     *  trackingId) so the action-layer sends one comm carrying all the content keys. */
    @ActivityMethod
    void emitBatchDispatch(Activation batch);

    /** Start one tracking child ChannelActionWorkflow (preDispatched) for an action in the batch. */
    @ActivityMethod
    void startTracker(Activation childAct);

    /** Debounce dedup at the end of the CREATED window, over the strongly-consistent nba_inflight table.
     *  Returns "LOSE" (a touch reached the member via a sibling, or a higher CREATED sibling exists -> self
     *  DEBOUNCE), "WAIT" (a sibling is dispatched-but-unconfirmed / mid-cancel -> hold another window + recheck),
     *  or "PROCEED" (I'm the winner -> send). */
    @ActivityMethod
    String debounceLost(Activation act);

    /** Operator suppression fan-out: start a Temporal Batch Operation that signals operatorSuppress() to every
     *  RUNNING ChannelActionWorkflow for this action (channel="" = all channels, else just that channel), found via
     *  the NbaActionId/NbaChannel search attributes. Server-side fan-out (scales to millions). Returns the batch jobId. */
    @ActivityMethod
    String suppressMatching(String actionId, String channel);
}
