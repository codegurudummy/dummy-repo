package com.amazonaws.cognito.signin.migration.worker;


import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazonaws.cognito.signin.dao.UserImportDAO;
import com.amazonaws.cognito.signin.dao.UserPoolDAO;
import com.amazonaws.cognito.signin.domain.DAOList;
import com.amazonaws.cognito.signin.domain.UserImportJobDO;
import com.amazonaws.cognito.signin.migration.exception.AtMaximumLocalCapacityException;
import com.amazonaws.cognito.signin.migration.kinesis.KinesisLogger;
import com.amazonaws.cognito.signin.migration.util.Limits.Limits;
import com.amazonaws.cognito.signin.migration.util.UserImportUtil;
import com.amazonaws.cognito.signin.migration.util.clients.CognitoS3Bucket;
import com.amazonaws.cognito.signin.migration.util.clients.UserImportClientFactory;
import com.amazonaws.cognito.signin.migration.util.domain.CognitoUserImportJob;
import com.amazonaws.cognito.signin.migration.util.metrics.CognitoMetrics;
import com.amazonaws.cognito.signin.migration.util.metrics.MetricKeys;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.ReleaseLockOptions;
import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;
import com.google.common.base.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.measure.unit.SI;
import javax.measure.unit.Unit;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


@Component
public class UserImportJobPoller {

    protected static final Logger LOG = LogManager.getLogger(UserImportJobPoller.class);

    public static final String COGNITO_JOB_COUNT_IDENTIFIER = "Cognito_User_Pools_Count_Lock";

    public static final String IMPORT_REGION_LOCK_PREFIX = "ImportRegionLock";

    private Random rand = new Random();

    @Autowired
    @Qualifier("CSIUserImportDAO")
    private UserImportDAO userImportDAO;

    @Autowired
    @Qualifier("CSIUserPoolDAO")
    private UserPoolDAO userPoolDAO;

    @Autowired
    @Qualifier("UserImportClientFactory")
    private UserImportClientFactory userImportClientFactory;

    @Autowired
    @Qualifier("CSIUserImportBucket")
    private CognitoS3Bucket csvBucket;

    @Autowired
    @Qualifier("UserImportJobPollerThreadPool")
    private ExecutorService jobInitiatorExecutorService;

    @Autowired
    @Qualifier("UserImportJobManager")
    private UserImportManager jobManager;

    @Autowired
    @Qualifier("UserImportLockClient")
    private AmazonDynamoDBLockClient lockClient;

    @Autowired
    @Qualifier("MetricsFactory")
    private MetricsFactory metricsFactory;

    @Autowired
    private KinesisLogger kinesisLogger;

    // Check for new jobs every 30 seconds
    @Async
    @Scheduled(fixedRate = 30000)
    public void run() throws Throwable {

        CognitoMetrics metrics = CognitoMetrics.get(metricsFactory, MetricKeys.JOB_POLLER);
        metrics.addCount(MetricKeys.UNEXPECTED_EXCEPTION, 0, Unit.ONE);
        metrics.addCount(MetricKeys.MAX_LOCAL_JOBS, 0, Unit.ONE);

        // Fail Fast
        if (jobManager.isAtMaxJobs()) {
            metrics.addCount(MetricKeys.MAX_LOCAL_JOBS, 1, Unit.ONE);
            LOG.info("Not polling for jobs, as host is already at max jobs running");
            return;
        }


        LOG.info("Polling for jobs");

        // Host is not at max jobs, poll for jobs and start them
        String scope = "GetPotentialJobs";
        metrics.startScope(scope);

        List<UserImportJobDO> potentialJobs = null;
        try {
            potentialJobs = getPotentialJobs(metrics);
        } catch (RuntimeException e) {
            metrics.addCount(MetricKeys.UNEXPECTED_EXCEPTION, 1, Unit.ONE);
            metrics.addException(e);
            LOG.error("Unexpected error polling for potential jobs", e);
        } finally {
            metrics.endScope(scope);
        }

        String startRunnerScope = "StartRunner";
        metrics.startScope(startRunnerScope);
        try {
            if (potentialJobs != null) {
                LOG.info(String.format("found %d jobs", potentialJobs.size()));
                metrics.addCount(MetricKeys.TOTAL_POTENTIAL_JOBS, potentialJobs.size(), Unit.ONE);
                for (UserImportJobDO importJob : potentialJobs) {
                    LOG.info(String.format("Starting Job Runner for %s in %s", importJob.getJobId(), importJob.getUserPoolId()));
                    jobInitiatorExecutorService.execute(new JobRunner(importJob));
                }
            }
        } finally {
            metrics.endScope(startRunnerScope);
            metrics.addCount(MetricKeys.LOCAL_JOB_COUNT, jobManager.getCurrentJobCount(), Unit.ONE);
            metrics.close();
        }

    }


    public List<UserImportJobDO> getPotentialJobs(Metrics metrics) {
        List<UserImportJobDO> pendingJobs = listAllByStatus(CognitoUserImportJob.IN_PROGRESS_STATUS, metrics);
        pendingJobs.addAll(listAllByStatus(CognitoUserImportJob.PENDING_STATUS, metrics));

        // Sort oldest to newest
        Collections.sort(pendingJobs, (job1, job2) ->  (int) (job1.getStartDate() - job2.getStartDate()));

        return pendingJobs;

    }

    public List<UserImportJobDO> listAllByStatus(String jobStatus, Metrics metrics) {

        List<UserImportJobDO> jobsWithStatus = new ArrayList<>();
        String nextToken = null;
        do {
            DAOList<UserImportJobDO> found = userImportDAO.listImportJobsByStatus(jobStatus, 50, nextToken, metrics);
            jobsWithStatus.addAll(found.getListItems());
            nextToken = found.getPaginationKey();
        } while (nextToken != null);

        return jobsWithStatus;
    }

    public byte[] hostNameAsBytes() {
        byte[] hostNameAsBytes = null;
        try {
            String hostName = InetAddress.getLocalHost().getHostName();
            hostNameAsBytes = hostName.getBytes(Charsets.UTF_8);
        } catch (UnknownHostException e) {
            LOG.error("Error getting host name", e);
        }

        return hostNameAsBytes;
    }

    public int countJobs(Metrics metrics) {
        return userImportDAO.countJobsByStatus(CognitoUserImportJob.IN_PROGRESS_STATUS, metrics);
    }


    public class JobRunner implements Runnable {

        String userPoolId;
        String jobId;
        String accountId;
        JobRunner(UserImportJobDO importJob) {
            this.userPoolId = importJob.getUserPoolId();
            this.jobId = importJob.getJobId();
            this.accountId = importJob.getAwsAccountId();
        }

        public void run() {
            CognitoMetrics metrics = CognitoMetrics.get(metricsFactory, MetricKeys.JOB_RUNNER);
            metrics.addProperty(MetricKeys.USER_POOL_ID, userPoolId);
            metrics.addProperty(MetricKeys.IMPORT_JOB_ID, jobId);
            metrics.addProperty(MetricKeys.ACCOUNT_ID, accountId);
            metrics.addCount(MetricKeys.JOB_LOCK_EXCEPTION, 0, Unit.ONE);
            metrics.addCount(MetricKeys.UNEXPECTED_EXCEPTION, 0, Unit.ONE);
            metrics.addCount(MetricKeys.JOB_NO_LONGER_ENQUEUED, 0, Unit.ONE);
            metrics.addCount(MetricKeys.MAX_LOCAL_JOBS, 0, Unit.ONE);

            LOG.info(String.format("Attempting to start job %s in user pool %s account %s", jobId, userPoolId, accountId));

            LockItem regionLock = null;
            LockItem accountLock = null;
            LockItem countLock = null;
            boolean jobStarted = false;

            try {
                // Attempt to get the account lock first because each JobRunner is tied to an account
                // The account lock limits each account to one job at a time and is important to keep multiple workers from picking up the same job
                accountLock = lockClient.acquireLock(UserImportUtil.getUniqueLockString(accountId), hostNameAsBytes(), 1, 2, TimeUnit.SECONDS);
                LOG.info(String.format("Acquired account lock for job %s in user pool %s in account %s", jobId, userPoolId, accountId));
                // Get region lock to limit concurrent jobs in each region
                regionLock = acquireRegionalLock();
                LOG.info(String.format("Acquired region lock %s for job %s in user pool %s in account %s", regionLock.getKey(), jobId, userPoolId, accountId));

                // Now that this process has a lock on the job, make sure the job is still in progress/pending
                UserImportJobDO importJob = userImportDAO.getUserImportJob(userPoolId, jobId, metrics);
                if (importJob == null) {
                    metrics.addCount(MetricKeys.JOB_DELETED_BEFORE_START, 1, Unit.ONE);
                    LOG.info(String.format("Fail Fast: Job id %s was deleted before it could start for user pool %s, account %s", jobId, userPoolId, accountId));
                    return;
                }

                metrics.addTime(MetricKeys.JOB_ENQUEUED_TIME, System.currentTimeMillis() - importJob.getStartDate(), SI.MILLI(SI.SECOND));

                // Fail Fast
                if (jobManager.isAtMaxJobs()) {
                    metrics.addCount(MetricKeys.MAX_LOCAL_JOBS, 1, Unit.ONE);
                    LOG.info(String.format("Fail Fast: Unable to start job %s in user pool %s account %s because host is at max", jobId, userPoolId, accountId));
                    return;
                }

                LOG.info(String.format("Job %s in user pool %s account %s has a status %s", importJob.getJobId(), importJob.getUserPoolId(), importJob.getAwsAccountId(), importJob.getStatus()));


                // Check that job is still pending or in progress
                if (!(CognitoUserImportJob.IN_PROGRESS_STATUS.equals(importJob.getStatus()) || CognitoUserImportJob.PENDING_STATUS.equals(importJob.getStatus()))) {
                    metrics.addCount(MetricKeys.JOB_NO_LONGER_ENQUEUED, 1, Unit.ONE);
                    LOG.info(String.format("Job %s in user pool %s account %s not started because it is no longer pending or in progress. It has a status of %s", importJob.getJobId(), importJob.getUserPoolId(), importJob.getAwsAccountId(), importJob.getStatus()));
                    return;
                }

                boolean canStartJob = true;

                if (CognitoUserImportJob.PENDING_STATUS.equals(importJob.getStatus())) {
                    canStartJob = false;

                    String countingScope = "JobCountScope";
                    metrics.startScope(countingScope);
                    try {

                        countLock = lockClient.acquireLock(COGNITO_JOB_COUNT_IDENTIFIER, hostNameAsBytes(), 1, 2, TimeUnit.SECONDS);
                        LOG.info(String.format("Acquired count lock before starting job %s in user pool %s account %s", importJob.getJobId(), importJob.getUserPoolId(), importJob.getAwsAccountId()));

                        int numberOfJobsRunning = countJobs(metrics);
                        metrics.addCount(MetricKeys.TOTAL_RUNNING_JOBS, numberOfJobsRunning, Unit.ONE);

                        if (numberOfJobsRunning < Limits.MAX_CONCURRENT_RUNNING_JOBS) {
                            LOG.info(String.format("Counted %d jobs currently running", numberOfJobsRunning));

                            importJob.setStatus(CognitoUserImportJob.IN_PROGRESS_STATUS);
                            importJob.setStartDate(new Date().getTime());
                            userImportDAO.updateUserImportJob(importJob, metrics);
                            LOG.info(String.format("Updated status of job %s in user pool %s account %s to %s", importJob.getJobId(), importJob.getUserPoolId(), importJob.getAwsAccountId(), importJob.getStatus()));
                            canStartJob = true;
                        }
                    } finally {
                        metrics.endScope(countingScope);
                        IOUtils.closeQuietly(countLock);
                    }
                }

                if (canStartJob) {

                    LOG.info(String.format("Beginning import job %s in user pool %s account %s", importJob.getJobId(), importJob.getUserPoolId(), importJob.getAwsAccountId()));

                    UserImportConfiguration importConfig = new UserImportConfiguration();
                    importConfig.setUserPoolDAO(userPoolDAO);
                    importConfig.setUserImportDAO(userImportDAO);
                    importConfig.setS3Bucket(csvBucket);
                    importConfig.setMetricsFactory(metricsFactory);
                    importConfig.setImportJob(importJob);
                    importConfig.setRegionalLock(regionLock);
                    importConfig.setLock(accountLock);
                    importConfig.setClientFactory(userImportClientFactory);
                    importConfig.setLockClient(lockClient);
                    importConfig.setKinesisLogger(kinesisLogger);

                    jobManager.startImport(new UserImport(importConfig));
                    jobStarted = true;
                }

            } catch (LockNotGrantedException e) {
                if (accountLock == null) { // tried to get account lock first
                    LOG.error(String.format("Unable to acquire account lock for job %s, user pool %s, account %s", jobId, userPoolId, accountId), e);
                } else if (regionLock == null) { // tried to get region lock second
                    LOG.error(String.format("Unable to acquire region lock for job %s, user pool %s, account %s", jobId, userPoolId, accountId), e);
                } else { // fallback to log the exception if somehow the prior two checks do not cover all possibilities
                    LOG.error(String.format("Unable to acquire lock for job %s, user pool %s, account %s", jobId, userPoolId, accountId), e);
                }
                metrics.addCount(MetricKeys.JOB_LOCK_EXCEPTION, 1, Unit.ONE);
                metrics.addException(e);
            } catch (AtMaximumLocalCapacityException e) {
                LOG.info(String.format("Unable to start job %s in user pool %s account %s because host is at max", jobId, userPoolId, accountId));
                metrics.addCount(MetricKeys.MAX_LOCAL_JOBS, 1, Unit.ONE);
                metrics.addException(e);
            } catch (InterruptedException | RuntimeException e) {
                LOG.error(String.format("Unexpected Exception when starting job %s, user pool %s, account %s", jobId, userPoolId, accountId), e);
                metrics.addCount(MetricKeys.UNEXPECTED_EXCEPTION, 1, Unit.ONE);
                metrics.addException(e);
            } finally {
                IOUtils.closeQuietly(metrics);

                if (!jobStarted && accountLock != null) {
                    ReleaseLockOptions options = new ReleaseLockOptions(accountLock);
                    options.setDeleteLock(true);
                    options.setBestEffort(true);
                    lockClient.releaseLock(options);

                    LOG.info(String.format("Couldn't start job and releasing account lock %s for job %s in user pool %s account %s", accountLock.getKey(), jobId, userPoolId, accountId));
                }

                if (!jobStarted && regionLock != null) {
                    LOG.info(String.format("Couldn't start job and releasing region lock %s for job %s in user pool %s account %s", regionLock.getKey(), jobId, userPoolId, accountId));

                    ReleaseLockOptions options = new ReleaseLockOptions(regionLock);
                    options.setDeleteLock(true);
                    options.setBestEffort(true);
                    lockClient.releaseLock(options);
                }
            }
        }

        // Gets one of a finite number of locks per region
        LockItem acquireRegionalLock() throws InterruptedException{
            int maxJobsPerRegion = jobManager.getMaxJobsPerRegion();

            // Try acquiring the first lock that's currently free.
            // First use getLock() to see if the lock doesn't exist and then try to acquire it,
            // because acquireLock will block for the lease duration (20s) trying to get the lock
            for (int i = 0; i < maxJobsPerRegion; i++) {
                if (lockClient.getLock(IMPORT_REGION_LOCK_PREFIX + i) == null) {
                    return lockClient.acquireLock(IMPORT_REGION_LOCK_PREFIX + i, hostNameAsBytes(), 1, 2, TimeUnit.SECONDS);
                }
            }

            // Didn't find a free lock to try to acquire.
            // Now pick one at random and try to acquire it. We must try to acquire an existing
            // lock, or abandoned locks will never be reclaimed.
            int lockToAcquire = rand.nextInt(maxJobsPerRegion);
            return lockClient.acquireLock(IMPORT_REGION_LOCK_PREFIX + lockToAcquire, hostNameAsBytes(), 1, 2, TimeUnit.SECONDS);
        }
    }
}