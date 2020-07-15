package resourceleak;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.support.metrics.CoralMetricsThreadInstance;
import com.amazonaws.duckhawk.jobbroker.domain.JobIdentifier;
import com.amazonaws.duckhawk.jobbroker.domain.JobItem;
import org.joda.time.Instant;


public class TestCloseResourceEvenWhenDefinitionNodeIsNonNull {

    public void testCloseResourceEvenWhenDefinitionNodeIsNonNull(final String accountId, final JobIdentifier jobIdentifier) {
        executorService.execute(() -> {
            try (Metrics metrics = metricsFactory.newMetrics()) {
                CoralMetricsThreadInstance.setMetricsInstance(metrics);
                final JobItem jobItem = threadLocalAccountContextWrapper
                        .wrapBlockInAccountContext(accountId, () -> jobService.getJob(jobIdentifier));

                // if the event pending time is null, that means the job has been already processed and removed from
                // the index.
                if (jobItem.getEventPendingTime() == null) {
                    return;
                }

                final Instant jobProcessingCutOffTime = Instant.now().minus(minimumPendingAge);

                if (jobItem.getEventPendingTime().isBefore(jobProcessingCutOffTime)) {
                    terminalJobProcessor.finalizeJob(jobItem, metrics);
                }
            }
        });
    }
}
