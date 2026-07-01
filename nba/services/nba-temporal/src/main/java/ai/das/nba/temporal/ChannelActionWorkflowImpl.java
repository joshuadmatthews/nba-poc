package ai.das.nba.temporal;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;

/**
 * Disposition-driven state machine for one activated ChannelAction (one member, action, channel).
 *
 *   CREATED --debounce+dedup--> [throttle gate] --DISPATCH--> IN_PROCESS (activation sent, no response yet)
 *     IN_PROCESS -> PRESENTED (delivered) -> SOFT_COMPLETED (engaged) -> HARD_COMPLETED (goal)
 *                \-> FAILED                              \-> DECLINED
 *   DEBOUNCED (lost the debounce dedup) ; SUPPRESSED (throttle saturated) ; SUPPRESSING -> SUPPRESSED|resume
 *
 * The Unified Activation Layer dishes the DELIVERY dispositions (PRESENTED=delivered, DECLINED, FAILED) and
 * answers a CANCEL with SUPPRESSED or SUPPRESS_FAILED. This workflow emits CREATED, IN_PROCESS (on dispatch),
 * and records those. SOFT_COMPLETED and HARD_COMPLETED are RULE-decided by the rules engine off the raw
 * disposition + the goal and arrive via the softComplete / hardComplete signals (the router bridges them);
 * EXPIRED when the TTL window elapses with no HARD completion. The rules-engine latch stays authoritative.
 *
 * Terminal: FAILED, HARD_COMPLETED, EXPIRED, SUPPRESSED, DEBOUNCED. Non-terminal rest states
 * (PRESENTED/SOFT_COMPLETED/DECLINED) keep the workflow alive watching for HARD_COMPLETED until the TTL.
 */
public class ChannelActionWorkflowImpl implements ChannelActionWorkflow {

    private static final Logger log = Workflow.getLogger(ChannelActionWorkflowImpl.class);
    private static final Set<String> TERMINAL = Set.of("FAILED", "SUPPRESSED", "HARD_COMPLETED", "EXPIRED");

    private final ActionActivities activities = Workflow.newActivityStub(
            ActionActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(5).build())
                    .build());

    private static final long THROTTLE_RECHECK_SECONDS = 5;
    private static final int DEBOUNCE_MAX_RECHECKS = 4;   // bound the WAIT loop (uncertain sibling) so we never hang

    private boolean suppressRequested = false;
    private boolean operatorSuppressRequested = false;   // operator pull -> pre-send terminal is SUPPRESSED, not DEBOUNCED
    private boolean hardCompleted = false;
    private boolean softCompleted = false;
    private boolean emittedSoft = false;            // dedup: SOFT_COMPLETED is emitted at most once
    private boolean cancelSent = false;
    private final Deque<String> dispositions = new ArrayDeque<>();   // canonical DELIVERY states from the activation layer
    private String myCorr = null;
    private String currentState = "CREATED";   // last-emitted state — exposed via debounceInfo for sibling dedup
    private double myScore = 0;

    @Override
    public void activate(Activation act, long debounceSeconds) {
        myCorr = act.correlationId;
        myScore = act.score;

        // BATCH CHILD — the orchestrator dispatched the comm as a batch; this child OWNS the per-action lifecycle
        // and STARTS at IN_PROCESS ("dispatched, awaiting the provider's delivery confirmation"). It holds there
        // until the activation layer's first disposition (Delivered) walks it to PRESENTED. Without this the
        // batched action never records IN_PROCESS and is invisible to the throttle counter / daily send caps.
        if (act.preDispatched) { emit(act, "IN_PROCESS"); trackDispositions(act); return; }

        // CREATED — debounce window. The router is a blind bridge: a fact burst can dispatch a 2nd action
        // before this one's CREATED round-trips, so several workflows for the same member end up here at once.
        // The state machine — NOT the router — resolves it: at the end of the window each one checks its
        // siblings and the loser self-DEBOUNCEs (see debounceLost).
        emit(act, "CREATED");
        Workflow.await(Duration.ofSeconds(debounceSeconds), () -> suppressRequested);
        // Superseded DURING debounce by a router SUPPRESS — nothing was sent, so DEBOUNCED (not a real
        // SUPPRESSED, which pulls an in-flight action). Terminal + free to re-activate.
        if (suppressRequested) { String st = preSendSuppressState(); emit(act, st); log.info("{} (superseded pre-send) {}", st, act.slug()); return; }

        // Self-dedup against the other in-flight workflows for this member (Temporal visibility + a live query
        // to each — see debounceLost). LOSE -> DEBOUNCED (a sibling reached the member, or a higher one is
        // racing). WAIT -> an uncertain sibling (dispatched-but-unconfirmed / mid-cancel) might bounce and free
        // the slot, so hold another window and recheck; bounded so we don't hang. PROCEED -> I'm the winner.
        for (int look = 0; look < DEBOUNCE_MAX_RECHECKS && !suppressRequested; look++) {
            String verdict = activities.debounceLost(act);
            if ("LOSE".equals(verdict)) { emit(act, "DEBOUNCED"); log.info("debounced (lost sibling dedup) {}", act.slug()); return; }
            if (!"WAIT".equals(verdict)) break;                                   // PROCEED
            Workflow.await(Duration.ofSeconds(debounceSeconds), () -> suppressRequested);   // hold + recheck
        }
        if (suppressRequested) { String st = preSendSuppressState(); emit(act, st); log.info("{} (superseded during recheck) {}", st, act.slug()); return; }

        // SMART THROTTLE GATE: SEND / WAIT (hold in CREATED, trickle as the bucket refills) / SUPPRESS
        // (saturated for the day -> emit a throttle-reason suppression so the rules engine reroutes).
        String gate = "WAIT";
        boolean inBacklog = false;
        while (!suppressRequested) {
            gate = activities.throttleGate(act);
            if (!"WAIT".equals(gate)) break;
            if (!inBacklog) { activities.throttleEnterBacklog(act); inBacklog = true; }
            Workflow.await(Duration.ofSeconds(THROTTLE_RECHECK_SECONDS), () -> suppressRequested);
        }
        if (inBacklog) activities.throttleExitBacklog(act);
        if (suppressRequested || "SUPPRESS".equals(gate)) {
            // throttle saturated for the day -> a REAL suppression (rules engine reroutes off it);
            // superseded while still waiting in the backlog -> nothing sent yet, so DEBOUNCED (lost the race).
            if ("SUPPRESS".equals(gate)) { emitReason(act, "SUPPRESSED", "throttle"); log.info("suppressed (throttle saturated) {}", act.slug()); }
            else { String st = preSendSuppressState(); emit(act, st); log.info("{} (superseded in backlog) {}", st, act.slug()); }
            return;
        }

        // FINAL GATE before send: honor a LIVE operator suppression. The point-in-time suppress batch op only
        // cancels workflows already RUNNING when the operator pulled the action — one CREATED after that (a
        // backlogged CREATE drained later, or one that sat in the throttle backlog through the pull) would otherwise
        // send. Reading the live suppressed set here catches them; it's bidirectional, so an unsuppress lets the
        // action dispatch again. This is a pre-send terminal, so it's a real SUPPRESSED (operator pull), not DEBOUNCED.
        if (activities.operatorSuppressed(act)) {
            operatorSuppressRequested = true; emit(act, "SUPPRESSED");
            log.info("SUPPRESSED (operator-suppressed at dispatch) {}", act.slug());
            return;
        }

        // Hand off — emit IN_PROCESS (activation sent, no provider response yet); the activation layer then
        // walks us PRESENTED (delivered) -> SOFT_COMPLETED (engaged) via dispositions.
        activities.emitActivation(act, "DISPATCH");
        emit(act, "IN_PROCESS");
        trackDispositions(act);
    }

    /** Walk the disposition states + the router's soft/hard signals until a terminal state or the TTL window.
     *  Delivery dispositions (SENT/IN_PROCESS/PRESENTED/DECLINED/FAILED) come straight from the activation
     *  layer; SOFT_COMPLETED/HARD_COMPLETED come from the router (idempotent — emitted once, ignored after). */
    private void trackDispositions(Activation act) {
        long deadline = Workflow.currentTimeMillis() + Math.max(1L, act.ttlSeconds) * 1000L;
        while (true) {
            long remaining = deadline - Workflow.currentTimeMillis();
            if (remaining <= 0 && dispositions.isEmpty() && !hardCompleted
                    && !(softCompleted && !emittedSoft) && !(suppressRequested && !cancelSent)) {
                // The TTL conversion window elapsed with no HARD completion -> EXPIRED. This is a real terminal
                // outcome (the negative ML label, the counterpart to HARD_COMPLETED) AND it frees the slot:
                // EXPIRED is not in the rules engine's ACTIVE_STATES, so the action-channel is immediately
                // re-eligible — if the model still ranks it best, the router re-fires it. The high-water mark
                // (reached SOFT_COMPLETED? DECLINED? just PRESENTED?) lives in the state-transition history.
                emit(act, "EXPIRED");
                log.info("EXPIRED (window elapsed, no conversion) {}", act.slug());
                return;
            }
            Workflow.await(Duration.ofMillis(Math.max(1L, remaining)),
                    () -> !dispositions.isEmpty() || hardCompleted || (softCompleted && !emittedSoft) || (suppressRequested && !cancelSent));

            if (hardCompleted) { emit(act, "HARD_COMPLETED"); log.info("HARD_COMPLETED {}", act.slug()); return; }

            if (suppressRequested && !cancelSent) {           // post-handoff cancel: ask the layer, await its verdict
                cancelSent = true;
                emit(act, "SUPPRESSING");
                activities.emitActivation(act, "CANCEL");     // layer answers SUPPRESSED or SUPPRESS_FAILED
                continue;
            }

            if (!dispositions.isEmpty()) {                    // a DELIVERY disposition arrived (or a SUPPRESS response)
                String d = dispositions.poll();
                if ("SUPPRESS_FAILED".equals(d)) {            // already sent — suppress didn't catch it; the send proceeds
                    cancelSent = false; suppressRequested = false; operatorSuppressRequested = false;
                    log.info("suppress failed (already sent) {}", act.slug());
                    continue;
                }
                emit(act, d);                 // SENT / IN_PROCESS / PRESENTED / DECLINED / FAILED / SUPPRESSED
                if (TERMINAL.contains(d)) { log.info("{} {}", d, act.slug()); return; }
                continue;                                     // non-terminal — keep watching
            }

            if (softCompleted && !emittedSoft) {              // router bridged a SOFT completion (once)
                emittedSoft = true;
                emit(act, "SOFT_COMPLETED");
                log.info("SOFT_COMPLETED {}", act.slug());
                // non-terminal — keep watching for HARD_COMPLETED until the window
            }
        }
    }

    @Override
    public void suppress() { suppressRequested = true; }

    @Override
    public void operatorSuppress() { suppressRequested = true; operatorSuppressRequested = true; }

    /** Pre-handoff terminal for a suppress: an operator pull is a real SUPPRESSED; a router supersession (a higher
     *  sibling out-raced this one) is DEBOUNCED (nothing sent, free to re-activate). Post-handoff is identical for
     *  both — the CANCEL-activation recall in trackDispositions. */
    private String preSendSuppressState() { return preSendSuppressState(operatorSuppressRequested); }

    /** The pure decision behind {@link #preSendSuppressState()} (extracted so it is unit-testable without a workflow
     *  harness): operator pull -> SUPPRESSED, router supersession -> DEBOUNCED. Deterministic — safe in the sandbox. */
    static String preSendSuppressState(boolean operatorSuppressRequested) {
        return operatorSuppressRequested ? "SUPPRESSED" : "DEBOUNCED";
    }

    @Override
    public void disposition(String status, String correlationId) {
        if (myCorr != null && correlationId != null && !correlationId.equals(myCorr)) {
            log.info("ignoring stale disposition {} (corr {} != {})", status, correlationId, myCorr);
            return;
        }
        dispositions.add(status);
    }

    // soft/hard completion are member-action-level evaluations bridged by the router — NOT run-specific, and
    // the eval's correlationId is regenerated per snapshot, so we do NOT corr-gate them (unlike dispositions,
    // which carry this run's trackingId|corr). Idempotent: emittedSoft / the terminal guard the single emit.
    @Override
    public void softComplete(String correlationId) { softCompleted = true; }

    @Override
    public void hardComplete(String correlationId) { hardCompleted = true; }

    @Override
    public String debounceInfo() { return currentState + "|" + myScore; }

    /** Emit a state transition AND record it as currentState, so the debounceInfo query exposes the live state
     *  to sibling workflows doing dedup. (emitActivation is NOT a state — it stays a direct activity call.) */
    private void emit(Activation a, String s) { currentState = s; activities.emitState(a, s); }
    private void emitReason(Activation a, String s, String reason) { currentState = s; activities.emitStateReason(a, s, reason); }
}
