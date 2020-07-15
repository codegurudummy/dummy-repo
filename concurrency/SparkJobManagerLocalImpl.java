package concurrency;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.seer.db.model.SparkClusterModel;
import com.amazonaws.seer.service.ClusterManager;
import com.amazonaws.seer.util.Arn;
import com.amazonaws.seer.util.SuppressFBWarnings;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang3.Validate;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *   _____                  _         _       _     __  __                                    _                       _
 *  / ____|                | |       | |     | |   |  \/  |                                  | |                     | |
 * | (___  _ __   __ _ _ __| | __    | | ___ | |__ | \  / | __ _ _ __   __ _  __ _  ___  _ __| |      ___   ___  __ _| |
 *  \___ \| '_ \ / _` | '__| |/ /_   | |/ _ \| '_ \| |\/| |/ _` | '_ \ / _` |/ _` |/ _ \| '__| |     / _ \ / __|/ _` | |
 *  ____) | |_) | (_| | |  |   <| |__| | (_) | |_) | |  | | (_| | | | | (_| | (_| |  __/| |  | |____| (_) | (__| (_| | |
 * |_____/| .__/ \__,_|_|  |_|\_\\____/ \___/|_.__/|_|  |_|\__,_|_| |_|\__,_|\__, |\___||_|  |______|\___/ \___|\__,_|_|
 *        | |                                                                 __/ |
 *        |_|                                                                |___/
 *
 * Run and Analyze Spark jobs locally
 */
public class SparkJobManagerLocalImpl implements SparkJobManager {

    static Map<Long, Thread> jobMap = new ConcurrentHashMap<>();
    static Map<String, Boolean> resultMap = new ConcurrentHashMap<>();

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    @Data
    @Builder
    private static class RunSparkClassMain implements Runnable {
        private Arn arn;
        private final Class<?> classToExecute;
        private final String[] arguments;
        public void run() {
            try {
                resultMap.put(arn.toString(), false);  // Start out with false in the result map, checker will look in jobMap first
                Method mainMethod = classToExecute.getMethod("main", String[].class);
                mainMethod.invoke(null, (Object) arguments);
                if (resultMap.containsKey(arn.toString())) {
                    resultMap.put(arn.toString(), true);  // If we made it here, it's a success
                }
            } catch (IllegalArgumentException e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            } catch (SecurityException e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            } catch (Exception e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
                throw e;
            } finally {
                Long currentId = java.lang.Thread.currentThread().getId();
                Validate.isTrue(jobMap.containsKey(currentId));
                jobMap.remove(currentId);
            }
        }
    }

    /*
     *  _____        _     _  _        _____       _               __
     * |  __ \      | |   | |(_)      |_   _|     | |             / _|
     * | |__) |_   _| |__ | | _  ___    | |  _ __ | |_  ___  _ __| |_  __ _  ___  ___
     * |  ___/| | | | '_ \| || |/ __|   | | | '_ \| __|/ _ \| '__|  _|/ _` |/ __|/ _ \
     * | |    | |_| | |_) | || | (__   _| |_| | | | |_|  __/| |  | | | (_| | (__|  __/
     * |_|     \__,_|_.__/|_||_|\___| |_____|_| |_|\__|\___||_|  |_|  \__,_|\___|\___|
     *
     *
     */

    /**
     * Run a Spark Job
     * @param credentials AWS credentials provider
     * @param customerId Customer ID
     * @param pathToJarFile Local or s3/s3a/s3n path to jar file to send to Spark as the job, will send to s3 if required
     * @param classToExecute Class within the jar to run
     * @param arguments Arguments to pass to main function of the given class oin the jar file
     * @return Arn of the Spark job created
     */
    @Override
    public Arn runSparkJob(AWSCredentialsProvider credentials,
                           String customerId,
                           String clusterTrackingGuid,
                           String pathToJarFile,
                           Class<?> classToExecute,
                           String ...arguments) {
        String[] args = new String[arguments.length + 1];
        args[0] = "local[*]";
        for (int i = 0; i < arguments.length; ++i) {
            args[i + 1] = arguments[i];
        }

        System.setProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "TRACE");
        RunSparkClassMain runner = RunSparkClassMain.builder()
                .arguments(args)
                .classToExecute(classToExecute)
                .build();
        Thread t = new Thread(runner);
        Arn sparkJobArn = Arn.forResource(customerId, Arn.Prefix.SPARK_JOB, "local/" + Long.valueOf(t.getId()).toString());
        runner.setArn(sparkJobArn);
        jobMap.put(t.getId(), t);
        t.start();
        return sparkJobArn;
    }

    /**
     * Stop Spark Job
     * @param credentials AWS Credentials
     * @param sparkJobArn Spark Job Arn
     * @return
     */
    public void stopSparkJob(AWSCredentialsProvider credentials, Arn sparkJobArn) {
        final String customerId = sparkJobArn.getCustomerId();
        if (sparkJobArn.getCustomerId().compareToIgnoreCase(customerId) != 0) {
            throw new InvalidArgumentException("Invalid customer ID " + customerId + " for Arn " + sparkJobArn.toString());
        }
        SparkJobStatus status = getJobStatus(credentials, sparkJobArn);
        if (status == SparkJobStatus .ACTIVE) {
            // We don't really have a good way to stop it...
            String[] split = sparkJobArn.getResourceId().split("/");
            Long threadId = Long.valueOf(split[split.length - 1]);
            Thread thread = jobMap.get(threadId);
            if (thread != null) {
                thread.interrupt();
                jobMap.remove(threadId);
            }
        }
    }

    /**
     * Get accumulated total Spark Job times for metering.
     * @param credentials AWS Credentials
     * @param sparkJobArn Spark Job Arn
     * @return totalJobTime in milliseconds
     */
    @Override
    public long getTotalJobTime(AWSCredentialsProvider credentials, Arn sparkJobArn) {
        return -1;
    }

    /**
     * Get Spark Job Status
     * @param credentials AWS Credentials
     * @param sparkJobArn Spark Job Arn
     * @return
     */
    @Override
    public SparkJobStatus getJobStatus(AWSCredentialsProvider credentials, Arn sparkJobArn) {
        final String customerId = sparkJobArn.getCustomerId();
        if (sparkJobArn.getCustomerId().compareToIgnoreCase(customerId) != 0) {
            throw new InvalidArgumentException("Invalid customer ID " + customerId + " for Arn " + sparkJobArn.toString());
        }
        String[] split = sparkJobArn.getResourceId().split("/");
        if (split.length > 0) {
            try {
                Long threadId = Long.valueOf(split[split.length - 1]);
                if (jobMap.containsKey(threadId)) {
                    return SparkJobStatus.ACTIVE;
                }
                if (resultMap.containsKey(sparkJobArn.toString())) {
                    return resultMap.get(sparkJobArn.toString()) ? SparkJobStatus.COMPLETED : SparkJobStatus.FAILED;
                }
            } catch (NumberFormatException e) {
                // Probably an EMR string, during development you switched to local, so return UNKNOWN
                return SparkJobStatus.UNKNOWN;
            }
        }
        return SparkJobStatus.UNKNOWN;
    }

    /**
     * Release any resources for job arn
     * @param sparkJobArn ARN of the spark job
     */
    @Override
    public void releaseJobArn(String customerId, AWSCredentialsProvider credentials, Arn sparkJobArn, ClusterManager clusterManager,
                              SparkClusterModel sparkClusterModel) {
        // Just take the result out of our in-memory map. so it doesn't grow forever
        if(sparkJobArn != null) {
            resultMap.remove(sparkJobArn.toString());
        }
        managePool(credentials, customerId, clusterManager);

    }

    /**
     * Do cluster pool management, if necessary
     */
    @Override
    public void managePool(AWSCredentialsProvider credentials, String customerId, ClusterManager clusterManager) {
        /* Do nothing for locasl Spark */
    }
}