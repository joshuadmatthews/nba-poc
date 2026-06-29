package ai.das.nba.temporal;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * One workflow per (member, action, channel) — the disposition-driven state machine for a single
 * activated ChannelAction. Started by the bridge on a router CREATE.
 *
 * States: CREATED -> SENT -> IN_PROCESS -> PRESENTED -> SOFT_COMPLETED (-> HARD_COMPLETED), with
 * FAILED / DECLINED branches off the disposition stream, SUPPRESSING -> SUPPRESSED on a cancel, and
 * SUPPRESSED for a pre-handoff cancel. The Unified Activation Layer walks us through the SEND-side
 * states by emitting one canonical-state disposition per provider event; HARD_COMPLETED is the goal.
 */
@WorkflowInterface
public interface ChannelActionWorkflow {
    @WorkflowMethod
    void activate(Activation act, long debounceSeconds);

    /** Request suppression. Pre-handoff = instant SUPPRESSED; post-handoff = SUPPRESSING (send a CANCEL
     *  activation, then resolve on the layer's SUPPRESSED / SUPPRESS_FAILED disposition). */
    @SignalMethod
    void suppress();

    /** OPERATOR pull (Command Center). Same recall as suppress(), but a PRE-handoff pull lands as a real
     *  SUPPRESSED (an operator pulled it), not DEBOUNCED (which means "a sibling out-raced me"). Fanned out to
     *  every running workflow for the action by the SuppressionWorkflow's Temporal batch signal. */
    @SignalMethod
    void operatorSuppress();

    /** A canonical DELIVERY disposition from the activation layer (SENT, IN_PROCESS, PRESENTED, DECLINED,
     *  FAILED) or a SUPPRESS response (SUPPRESSED / SUPPRESS_FAILED). correlationId identifies the run so a
     *  stale disposition (from a prior run reusing this workflow id) is ignored. */
    @SignalMethod
    void disposition(String status, String correlationId);

    /** Router bridge: the rules engine evaluated SOFT completion (the disposition hit the channel's bar).
     *  Drives the SOFT_COMPLETED state (non-terminal — we keep watching for HARD_COMPLETED). Idempotent. */
    @SignalMethod
    void softComplete(String correlationId);

    /** Router bridge: the rules engine evaluated HARD completion (the goal). Drives terminal HARD_COMPLETED.
     *  The rules-engine latch stays authoritative for eligibility; this is the state-machine reflection. */
    @SignalMethod
    void hardComplete(String correlationId);

    /** Debounce dedup: a sibling workflow (same member) queries this one at its timer to read
     *  "{currentState}|{score}", deciding whether to stand down. Live query = strongly-consistent state. */
    @QueryMethod
    String debounceInfo();
}
