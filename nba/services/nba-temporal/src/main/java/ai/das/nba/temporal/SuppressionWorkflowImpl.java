package ai.das.nba.temporal;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import org.slf4j.Logger;

import java.time.Duration;

public class SuppressionWorkflowImpl implements SuppressionWorkflow {
    private static final Logger log = Workflow.getLogger(SuppressionWorkflowImpl.class);

    private final ActionActivities activities = Workflow.newActivityStub(
            ActionActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(60))
                    .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(10).build())
                    .build());

    @Override
    public void suppress(String actionId, String channel) {
        // The fan-out itself is a server-side Temporal Batch Operation (operatorSuppress signal over a Visibility
        // query) — run from an activity since it touches the service stub. The signal is idempotent (sets a flag),
        // so a retried batch is harmless. Reaching zero workflows (all already terminal) is a no-op, not an error.
        String jobId = activities.suppressMatching(actionId, channel);
        log.info("operator suppression fanned out: action={} channel={} batchJob={}",
                actionId, (channel == null || channel.isEmpty()) ? "*" : channel, jobId);
    }
}
