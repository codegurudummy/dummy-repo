package concurrency;

import com.amazon.fws.periodic.PeriodicWorkProcessor;
import com.amazon.fws.periodic.WorkId;
import com.amazon.fws.periodic.Worker;
import com.amazon.fws.periodic.WorkerFactory;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.*;

/**
 * This PeriodicWorkProcessorImpl class is a wrapper around java's standard ScheduledThreadPoolExecutor.
 * This class does three things on top of STPE:
 *
 * First, it simplifies the API to just what we need
 * to periodically process work. STPE's footprint is quite large and flexible, so it's useful to trim it down.
 *
 * Second, we keep track of tasks in flight so that they can be individually managed.
 *
 * Third, we added some statistics getters so we know if the scheduler is falling behind.
 *
 * The way ScheduledThreadPoolExecutor implements periodic tasks (which this class in turn uses) is
 * a bit awkward, and some of the complexity here-- especially at construction time-- revolve around
 * masking those awkward semantics.
 *
 */
public class PeriodicWorkProcessorImpl implements PeriodicWorkProcessor {

    private static final Logger LOG = Logger.getLogger(PeriodicWorkProcessorImpl.class);

    private static final double NANOS_PER_MILLI = 1000000;
    private static final double NANOS_PER_SECOND = 1000 * NANOS_PER_MILLI;

    // We allow a brief delay so the alarm service has the chance to commit
    // it's transaction. This is just a stopgap until the alarm service is able
    // to initiate this work outside of the transaction.
    private static final double MIN_SCHEDULE_DELAY_NANOSECONDS = 0.5 * NANOS_PER_SECOND;

    ScheduledThreadPoolExecutor executor;

    private WorkerFactory workerFactory;

    private int secondsUntilSuicide = 0;

    private final ScheduledThreadPoolExecutor suicideTimerExecutor;
    private ScheduledFuture<?> suicideTimer;

    // A map from workId to the task that is processing that work.
    private final ConcurrentHashMap<WorkId, ScheduledFuture<?>> taskMap = new ConcurrentHashMap<WorkId, ScheduledFuture<?>>();

    public PeriodicWorkProcessorImpl(int poolSize) {

        suicideTimerExecutor = new ScheduledThreadPoolExecutor(1);

        RejectedExecutionHandler rejectedHandler = new RejectedExecutionHandler() {

            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                // When we shutdown we should always see this and it's ok, but before shutdown
                // we should never see this, so treat it as an assertion.
                if (!executor.isShutdown()) {
                    LOG.fatal("Work execution halted due to rejection, but " +
                            "a rejection was not expected. Work is currently not " +
                            "being completed for this runnable: " + r);
                }
            }

        };

        ThreadFactory periodicThreadFactory = new ThreadFactory() {
            @Override public Thread newThread(Runnable r) {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setName(PeriodicWorkProcessorImpl.class.getSimpleName());
                return t;
            }
        };

        // We subclass ScheduledThreadPoolExecutor only so that we can catch when tasks are completed,
        // so that we can remove the task bookkeeping that we maintain.
        // See also http://cs.oswego.edu/pipermail/concurrency-interest/2005-May/001548.html for a bit
        // more information on why this looks odd.
        executor = new ScheduledThreadPoolExecutor(poolSize, periodicThreadFactory, rejectedHandler) {

            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);

                // Every iteration will finish up in this path. Ignore any
                // iteration that wasn't canceled and didn't see an exception.
                ScheduledFuture<?> task = (ScheduledFuture<?>)r;

                try {
                    // There are three possibilities: First, the task has finished without exception, in which case
                    // task.get(0) will return a TimeoutException, since the task is still ongoing.
                    // Second, we got interrupted, in which case just set the flag and fall through.
                    // Third, the task threw an exception. When a task throws an exception we take it to mean
                    // that we should stop processing. STPE will no longer reschedule it, and we remove the
                    // task mapping from our own bookkeeping.
                    task.get(0, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    // This is the case where the task finished normally. It's still going
                    // to execute next period, so there is no void return yet.
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    removeTaskMapping(task);
                }
            }
        };

        LOG.info("Built a ScheduledThreadPoolExecutor with a corePoolSize of " + poolSize + " threads");
    }

    /**
     * Removes the internal bookkeeping for a given task.
     */
    private void removeTaskMapping(ScheduledFuture<?> task) {

        // O(n) removal is unfortunate...
        for (Entry<WorkId, ScheduledFuture<?>> entry: taskMap.entrySet()) {
            if (entry.getValue().equals(task)) {
                taskMap.remove(entry.getKey());
                return;
            }
        }
    }

    @Override
    public synchronized void updateWorkItem(WorkId workItem) {
        if (taskMap.keySet().contains(workItem.getId())) {
            rescheduleWorkImmediately(workItem);
        } else {
            addUnrecognizedWork(Lists.newArrayList(workItem), true);
        }
    }

    @Override
    public synchronized void updateWorkSet(List<WorkId> workList) {
        if (workerFactory == null) {
            throw new IllegalStateException("You must specify a worker factory before you update work");
        }

        stopSuicideTimer();

        removeUnlistedWork(workList);

        addUnrecognizedWork(workList, false);

        if (workList.size() > 0) {
            startSuicideTimer();
        }
    }

    private void removeUnlistedWork(List<WorkId> workList) {
        // Calculate the tasks to remove from the current working set
        Set<WorkId> toRemove = new HashSet<WorkId>(taskMap.keySet());
        Set<WorkId> workListHashSet = new HashSet<WorkId>(workList);
        toRemove.removeAll(workListHashSet);

        for (WorkId deadWork: toRemove) {
            LOG.debug("Removing work item " + deadWork.getId());

            ScheduledFuture<?> task = taskMap.get(deadWork);

            if (task != null) {
                try {
                    task.cancel(true);
                } finally {
                    taskMap.remove(deadWork);
                }
            }
        }
        executor.purge();
    }

    private void addUnrecognizedWork(List<WorkId> workList, boolean scheduleImmediateExecution) {
        // Calculate the tasks to add to the current working set
        Set<WorkId> toAdd = new HashSet<WorkId>(workList);
        toAdd.removeAll(taskMap.keySet());

        Map<WorkId, Worker> workerMap = workerFactory.createWorkers(toAdd);
        for (WorkId work : toAdd) {
            Worker worker = workerMap.get(work);
            scheduleWork(work, worker, scheduleImmediateExecution);
        }
    }

    private void rescheduleWorkImmediately(WorkId work) {
        LOG.debug("Canceling existing work to run it sooner: " + work.getId());
        ScheduledFuture<?> oldTask = taskMap.get(work);
        oldTask.cancel(false);

        Worker worker = workerFactory.createWorker(work);
        scheduleWork(work, worker, true);
    }

    private void scheduleWork(WorkId work, Worker worker, boolean scheduleImmediateExecution) {
        if (worker == null) {
            LOG.info("No Worker for work [" + work.getId() + "]. Work cannot be scheduled.");
            return;
        }
        LOG.debug("Scheduling work to start: " + work.getId());

        int workPeriodSeconds = worker.getWorkPeriodSeconds();
        double periodicIntervalNanoseconds = workPeriodSeconds * NANOS_PER_SECOND;
        LOG.debug("Work period seconds for work " + work.getId() + "= " + workPeriodSeconds);

        // We want work to happen at fixed intervals, but we don't want workers to bunch up.
        // For newly created or updated alarms, we start them as soon as possible (and rely on
        // entropy of creates and updates). For regular polled work, we stagger within the
        // work period.

        double startDelayNanoseconds = MIN_SCHEDULE_DELAY_NANOSECONDS;

        double nowNanoseconds = System.currentTimeMillis() * NANOS_PER_MILLI;
        double currentOffsetNanoseconds = nowNanoseconds % periodicIntervalNanoseconds;

        if (scheduleImmediateExecution) {
            // Reverse calculate the effective intraPeriodStaggerRatio so we can log it
            double perPeriodOffsetNanoseconds = startDelayNanoseconds + currentOffsetNanoseconds;
            double intraPeriodStaggerRatio = (perPeriodOffsetNanoseconds % periodicIntervalNanoseconds) / periodicIntervalNanoseconds;

            LOG.debug(work.getId() + " will be immediately scheduled, effectively at " + (int) (intraPeriodStaggerRatio * 100) + "% into the period; " +
                    "start delay is " + (int) (startDelayNanoseconds / NANOS_PER_MILLI) + "ms");
        } else {
            double intraPeriodStaggerRatio = (Math.max(Math.abs(work.getId().hashCode()), 0) % 1000) / 1000.0;
            double perPeriodOffsetNanoseconds = periodicIntervalNanoseconds * intraPeriodStaggerRatio;

            startDelayNanoseconds = perPeriodOffsetNanoseconds - currentOffsetNanoseconds;
            if (startDelayNanoseconds < MIN_SCHEDULE_DELAY_NANOSECONDS) {
                startDelayNanoseconds += periodicIntervalNanoseconds;
            }

            LOG.debug(work.getId() + " will be regularly scheduled at " + (int) (intraPeriodStaggerRatio * 100) + "% into the period; " +
                      "start delay is " + (int) (startDelayNanoseconds / NANOS_PER_MILLI) + "ms");
        }

        ScheduledFuture<?> task = executor.scheduleAtFixedRate(
                worker,
                (long)startDelayNanoseconds,
                (long)periodicIntervalNanoseconds,
                TimeUnit.NANOSECONDS
        );

        taskMap.put(work, task);
    }

    private void stopSuicideTimer() {
        // Delete the old timer if necessary
        if (suicideTimer != null) {
            suicideTimer.cancel(true);
            suicideTimerExecutor.purge();
        }
    }

    private void startSuicideTimer() {
        // Start a new timer if necessary
        if (secondsUntilSuicide > 0) {
            Runnable killer = new Runnable() {

                @Override
                public void run() {
                    LOG.error("Suicide: removing all work");
                    removeAllWork();
                }

            };

            suicideTimer = suicideTimerExecutor.schedule(killer, secondsUntilSuicide, TimeUnit.SECONDS);
        }
    }

    @Override
    public void removeAllWork() {
        updateWorkSet(new LinkedList<WorkId>());
    }

    @Override
    public int getActiveWorkersCount() {
        return executor.getActiveCount();
    }

    @Override
    public int getMaxWorkersLimit() {
        return executor.getCorePoolSize();
    }

    @Override
    public int getWorkBacklogCount() {
        // Count all futures that are in the queue and ready to run. There is no guarantee about the
        // ordering of the queue, so we have to visit every future. This shouldn't be too bad as
        // long as this method is only used for periodic monitoring.

        int count = 0;
        for (Runnable task: executor.getQueue()) {
            // Every element in the queue is guaranteed by the API to be a ScheduledFuture
            ScheduledFuture<?> future = (ScheduledFuture<?>)task;
            if (future.getDelay(TimeUnit.NANOSECONDS) <= 0) {
                ++count;
            }
        }

        return count;
    }

    @Override
    public void setWorkerFactory(WorkerFactory workerFactory) {
        this.workerFactory = workerFactory;
    }

    // This method exists for unit test convenience only
    /* package protected */ boolean containsWork(WorkId workId) {
        ScheduledFuture<?> task = taskMap.get(workId);

        return (task != null) && !task.isCancelled();
    }

    @Override
    public void setInactivitySuicideTimeout(int secondsUntilSuicide) {
        this.secondsUntilSuicide = secondsUntilSuicide;
    }
}