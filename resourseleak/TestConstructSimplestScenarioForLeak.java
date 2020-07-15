package resourceleak;

import com.amazon.qtt.vault.VaultFile;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;


public class TestConstructSimplestScenarioForLeak {

    // MuGraph does not capture the context of whether the path to exit node is reached along return or throw statements.

    @VisibleForTesting
    void testConstructSimplestScenarioForLeak(VaultFile uploadRoot, String key) {
        String testArtifactsMetadata = collectMetadataForTestArtifacts(uploadRoot);
        File metadataFile = null;
        OutputStreamWriter writer = null;
        try {
            metadataFile = File.createTempFile("_test_artifacts_metadata", ".json");
            writer = new OutputStreamWriter(new FileOutputStream(metadataFile), Charsets.UTF_8.newEncoder());
            writer.write(testArtifactsMetadata);
        } catch (IOException e) {
            log.error("Creating the local metadata file failed for test run: " + testRun.getTestRunId()
                    + " with error: " + e.toString());
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    log.warn("Closing writer after uploading metadata for test run "
                            + testRun.getTestRunId() + " failed with error: " + e.toString());
                }
            }
        }
        try {
            if (isTestRunForHighside()) {
                // This is not particularly necessary for TodBuilder uploads, and has never worked for highside anyway.
                log.info("Skipping metadata.json upload to highside");
                // pass
            } else {
                log.info("Putting metadata.json to lowside stack S3 location");
                execBucketProperties.todS3Client.putObject(execBucketProperties.s3BucketName, key + "_test_artifacts_metadata.json", metadataFile);
            }
        } catch (Exception e) {
            log.error("Uploading metadata file for test run " + testRun.getTestRunId()
                    + " failed with error: " + e.toString());
        }
    }

}
