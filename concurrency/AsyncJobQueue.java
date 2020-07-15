package concurrency;

import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * The asynchronous job queue tracks the state of jobs submitted to the Hieroglyph asynchronous APIs. The job queue
 * links different canaries via the consumer-producer pattern:
 * - StartXXXCanary will submit jobs to the system, and produce jobs for the queue.
 * - GetXXXCanary will consume jobs from the queue, and check state from the system.
 * <p>
 * This class is accessed from multiple threads, be thread-safe! We leave jobs on the queue even when a canary is
 * checking the job status in the system, as a consequence it might happen that 2 canaries check the same job status.
 * This should not be a problem.
 *
 * The queue will keep at least 1 job around to make sure GetXXX canary can keep calling the API with a valid job id.
 */
@RequiredArgsConstructor
@Slf4j
public class AsyncJobQueue {

    private final Consumer<Boolean> queueOverflowMetricEmitter;
    private final Consumer<Boolean> slowJobMetricEmitter;
    private final Consumer<Integer> jobCountMetricEmitter;
    private final Map<String, AsyncJob> jobs = new LinkedHashMap<>();

    /**
     * Adds a new job to the job queue.
     *
     * @param asyncJob Async job.
     */
    public void addJob(final AsyncJob asyncJob) {
        Preconditions.checkNotNull(asyncJob);

        log.info("Adding job {} to the queue, {} jobs left on the queue", asyncJob, jobs.size());

        synchronized (jobs) {
            jobs.put(asyncJob.getJobId(), asyncJob);
            jobCountMetricEmitter.accept(jobs.size());
        }
    }

    /**
     * Updates a job on the job queue. When the job is not found, this operation is a nop. If the job if in the
     * finished state, the job is removed from the queue.
     *
     * @param jobId     Job id as returned by the Hieroglyph APIs.
     * @param jobStatus Status of the job as returned by the Hieroglyph API.
     */
    public void updateJob(final String jobId, final AsyncJob.JobStatus jobStatus) {
        Preconditions.checkArgument(StringUtils.isNotBlank(jobId));
        Preconditions.checkNotNull(jobStatus);

        log.info("Updating job with id {} with status {}.", jobId, jobStatus);

        synchronized (jobs) {
            final AsyncJob job = jobs.get(jobId);
            if (job == null) {
                log.info("Job with id {} not found in the queue.", jobId);
                return;
            }

            log.info("Found job {} on the queue", job);

            if (jobStatus.isJobFinished()) {
                log.info("Job {} is finished, removing from the queue", job);
                removeJobFromQueue(job);
            } else {
                // Re-insert to make sure the job goes last in the order.
                moveJobToEndOfQueue(job);
                log.info("Job {} is not finished, moving to the end of the queue", job);
            }

            log.info("Queue has {} jobs", jobs.size());
            jobCountMetricEmitter.accept(jobs.size());
        }
    }

    /**
     * Returns the next unfinished job on the queue. The oldest job on the queue (i.e. the one submitted the longest
     * time ago with the Hierolgyph API) is returned first.
     */
    public Optional<AsyncJob> getNextUnfinishedJob() {
        log.info("Getting next unfinished job.");

        synchronized (jobs) {
            final Optional<AsyncJob> job = jobs.values().stream().findFirst();
            log.info("Next unfinished job is {}, queue size is {}", job, jobs.size());
            return job;
        }
    }

    /**
     * Returns the size of the job queue.
     */
    public int size() {
        synchronized (jobs) {
            return jobs.size();
        }
    }

   /**
     * Checks the queue for jobs that are taking longer as the passed in duration. Throw an exception when there is a
     * job on the queue that is taking too long. This exception will fail the canary. Additionally this method will
     * throw away the oldest job so the canary does not get "stuck".
     *
     * @param maxJobTime Maximum time a job can be in progress.
     */
    public void checkForSlowJobs(final Duration maxJobTime) {
        final List<AsyncJob> overTimeJobs = getOverTimeJobs(maxJobTime);
        final boolean hasOverTimeJobs = !overTimeJobs.isEmpty();
        slowJobMetricEmitter.accept(hasOverTimeJobs);
        if (hasOverTimeJobs) {
            // Remove the oldest job from the queue to make sure we don't keep jobs around forever.
            synchronized (jobs) {
                final AsyncJob oldestJob = overTimeJobs.get(0);
                log.info("Removing stale job {} from queue", oldestJob);
                removeJobFromQueue(oldestJob);
                jobCountMetricEmitter.accept(jobs.size());
            }

            throw new RuntimeException(
                    String.format("Too many jobs taking more than %s: %s", maxJobTime, overTimeJobs));
        }
    }

    /**
     * Returns the jobs that are running longer than the passed in time.
     *
     * @param maxJobTime Maximum job time
     * @return List of jobs that are running longer than maxJobTime.
     */
    private List<AsyncJob> getOverTimeJobs(final Duration maxJobTime) {
        Preconditions.checkNotNull(maxJobTime);

        log.info("Fetching jobs on the queue that took more than {}", maxJobTime);

        final Instant now = Clock.systemUTC().instant();

        synchronized (jobs) {
            final List<AsyncJob> overtimeJobs = jobs.values().stream()
                    .filter(job -> Duration.between(job.getSubmitTimestamp(), now).compareTo(maxJobTime) > 0)
                    .collect(Collectors.toList());
            log.warn("Jobs {}, took more than {}.", overtimeJobs, maxJobTime);
            return overtimeJobs;
        }
    }

    // Removes the passed in job from the queue. When the job is the last job on the queue, it will not be removed
    // from the queue. Reason to keep a job on the queue is so that the GetXXX canaries can keep calling the API
    // even if the job is already finished.
    private void removeJobFromQueue(final AsyncJob job) {
        if (jobs.size() > 1) {
            jobs.remove(job.getJobId());
        } else {
            log.info("Keeping job {} on the queue to prevent it from becoming empty", job);
        }
    }

    private void moveJobToEndOfQueue(final AsyncJob job) {
        jobs.remove(job.getJobId());
        jobs.put(job.getJobId(), job);
    }
}