package ai.das.nba.temporal;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import org.slf4j.Logger;

import java.time.Duration;

public class BatchOrchestratorWorkflowImpl implements BatchOrchestratorWorkflow {

    private static final Logger log = Workflow.getLogger(BatchOrchestratorWorkflowImpl.class);

    private final ActionActivities activities = Workflow.newActivityStub(
            ActionActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(5).build())
                    .build());

    private Activation batch;
    private boolean changed = false;
    private boolean suppressRequested = false;

    @Override
    public void orchestrate(long debounceSeconds) {
        // The batch arrives via updateBatch (signalWithStart delivers the first one on start).
        Workflow.await(() -> batch != null || suppressRequested);
        if (suppressRequested) return;

        // Settle window — dedupe to the LATEST top-N: if a newer batch arrives, restart the debounce.
        do {
            changed = false;
            Workflow.await(Duration.ofSeconds(debounceSeconds), () -> changed || suppressRequested);
        } while (changed && !suppressRequested);

        if (suppressRequested) { log.info("batch suppressed pre-dispatch {}", batch.batchSlug()); return; }

        // Start one tracking child per action FIRST (so each is ready for its disposition), then dispatch
        // the batch as ONE activation. The children are independent (they outlive this orchestrator).
        for (Activation.BatchAction a : batch.actions) activities.startTracker(child(batch, a));
        activities.emitBatchDispatch(batch);
        log.info("batch dispatched {} x{}", batch.batchSlug(), batch.actions.size());
        // job done — terminate; the children track each disposition from here.
    }

    @Override
    public void updateBatch(Activation b) { this.batch = b; this.changed = true; }

    @Override
    public void suppress() { this.suppressRequested = true; }

    /** A single-action child Activation, flagged preDispatched so the child starts post-dispatch. */
    static Activation child(Activation b, Activation.BatchAction a) {
        Activation c = new Activation();
        c.nbaId = b.nbaId; c.entityType = b.entityType; c.entityId = b.entityId; c.memberId = b.memberId;
        c.op = "CREATE"; c.channel = b.channel; c.actionId = a.actionId; c.name = a.name;
        c.contentKey = a.contentKey; c.ttlSeconds = a.ttlSeconds; c.score = a.score;
        c.correlationId = b.correlationId; c.source = "batch-orchestrator"; c.eventTs = b.eventTs;
        c.preDispatched = true;
        return c;
    }
}
