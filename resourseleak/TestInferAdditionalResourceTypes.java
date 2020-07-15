package resourceleak;

import amazon.wapqa.processlauncher.exceptions.ProcessCanceledException;
import com.amazon.coral.metrics.Metrics;
import com.amazon.qtt.util.SimpleMetricsWrapper;
import com.amazon.tod.exception.TodDependencyException;
import com.amazon.tod.exception.TodWorkflowException;
import com.amazon.tod.workflow.Status;

import javax.measure.unit.Unit;
import java.io.File;


public class TestInferAdditionalResourceTypes {


    // MuGraph does not capture the context of whether the path to exit node is reached along return or throw statements.
    public void testInferAdditionalResourceTypes() {
        try (SimpleMetricsWrapper metricsWrapper = new SimpleMetricsWrapper(metricsFactory,"BuildExecutionArtifacts")){
            String timestamp = String.valueOf(System.currentTimeMillis());

            initWorkflow();

            // Keep this.  Customers will always need the material set for RRPs.
            if (isAReadRestrictedPackage && readRestrictedMaterialNotConfigured()) {
                String errorMessage = "Read-restricted packages must be given a material set for encryption"
                        + ". Please see https://tiny.amazon.com/zbtn7bkj/wamazindeToDUserSett";
                log.info(errorMessage);
                throw new TodWorkflowException(Status.INVALID, errorMessage);
            }

            // The finally for this outer try-catch is where we should attempt to clean all workspace dirs.
            try {

                log.info("Pulling extra packages.");
                // Uploading artifacts is not an optional step in TodBuilder.
                // Unlike TodWorker, in TodBuilder, exceptions during upload must cause retries or failure.
                // Do upload in sequence instead of in a finally block like TodWorker.
                try {

                    log.info("Uploading execution artifacts.");
                    reportStatus(getPostRunStatus(), "Build complete. Uploading build artifacts.");

                    // This call is here to ensure that the updated metadata containing the signature of the encrypted
                    // archive gets propagated.
                    heartbeat.updateTodWorkerHost(testRun);

                    // Prepare the message for the next step.  Once we've finished uploading
                    // artifacts, the workflow will move on to the RegionalTodWorker phase.
                    // This is always constant, and frequently people's RTWs get stuck for various
                    // reasons.  Seeing "Upload complete" while waiting for an RTW is misleading.
                    String statusMessage = String.format(
                            "Workspace is ready. Waiting for next available RegionalTodWorker in %s/%s or delayed due to https://tiny.amazon.com/12qs2whig/wamazindeToDUserFAQ",
                            testRun.getTestRunArguments().getTodWorkerApolloEnvironmentName(),
                            testRun.getTestRunArguments().getTodWorkerApolloStage()
                    );
                    reportStatus(getPostRunStatus(), statusMessage);
                } catch (TodWorkflowException e) {
                    log.error("Bubbling up workflow exception.", e);
                    throw e;
                } catch (Exception e) {
                    // Upload is expected to function.  Retry on all other exceptions.
                    String errorMessage = "Could not upload, retrying in TodBuilder: " + e.toString();
                    log.error(errorMessage, e);
                    throw new TodDependencyException("Upload", Status.FATAL, errorMessage, e);
                }

                // TODO Remove the concept of 'exitCode'.  For now, set it to passing if we reach here.
                exitCode = 0;

            } catch (TodDependencyException e) {
                // A TodDependencyException indicates a condition that prevents
                // us from proceeding right now, but is (presumably) a retryable
                // condition.  In this case, we don't want to report a status to
                // the workflow or to CD that causes them to think we've failed
                // and should close immediately.  The run will be sent back to the Decider,
                // where it'll be retried, possibly on a different TodBuilder host.
                willRetryInWorkflow = true;
                int retry_Count = Integer.parseInt(testRun.getMetadata().getOrDefault(RETRY_COUNT,"0"));
                if (retry_Count >= BUILDER_ACTIVITY_MAX_RETRY) {
                    Metrics isRetryMetrics = metricsWrapper.getMetrics();
                    isRetryMetrics.addCount("FINAL_FAILURE", 1, Unit.ONE);
                }
                testRun.getMetadata().put(RETRY_COUNT, String.valueOf(retry_Count + 1));
                heartbeat.updateTodWorkerHost(testRun);
                throw e;
            } finally {
                // Delete the customer and fake workspaces whether or not upload threw an exception.
                // Disable this to verify execution state post-run.
                log.info("Cleaning up workspaces in reverse order.");
                updateDebugStatus(getPostRunStatus(), "Cleaning up workspace");

                // Don't forget to delete the fake parent workspace
                workspaceDirs.add(0, innerFakeWorkspaceBasePath);
                // Reverse order so that we can get a warning if deleting a sub workspace fails.
                for (int i = workspaceDirs.size() - 1; i >= 0; i--) {
                    File wsDir = workspaceDirs.get(i);
                    try {
                        log.info("Cleaning up the workspace " + wsDir.getAbsolutePath());
                        cleanupWorkspace(wsDir);
                    } catch (Exception e) {
                        // If the delete fails, we should also try to delete the other directories.
                        String errorMessage = "Could not clean up the workspace, continuing.";
                        log.error(errorMessage, e);
                    }
                }
            }

        } catch (ProcessCanceledException e) {
            log.info("Caught ProcessCanceledException: " + e.getMessage());
            // If the process is canceled, the current step will throw a
            // ProcessCanceledException. We don't need to rethrow it further.
            // We can communicate the message to the user, though.
            if (cancellationMessage == null) {
                cancellationMessage = e.getMessage();
            }
            cancelRequested = true;

            // Although cancellations should be coming from the heartbeat,
            // if it originates here somehow, the heartbeat will still be
            // cancelled.  Also, no negative effect from calling this if
            // the heartbeat is already cancelled.
            heartbeat.reportCancellation(cancellationMessage);
            return;
        } catch (TodWorkflowException e) {
            log.info("Caught TodWorkflowException: " + e.toString());
            exception = e;
            heartbeat.reportException(e);
            log.debug("Reported exception, dying.");
            return;
        } catch (Exception e) {
            log.info("Unhandled exception: " + e.toString());
            // Unhandled exception.  Since this could be ANYTHING and the Decider
            // might not know how to interpret it, we should create an informative
            // message containing details about the exception, but not send the
            // exception itself across the boundary.
    }


}
