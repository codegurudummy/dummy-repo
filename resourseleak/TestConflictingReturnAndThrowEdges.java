package resourceleak;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.io.IOUtils;

import javax.measure.unit.Unit;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.List;
import java.util.Vector;

public class TestConflictingReturnAndThrowEdges {

    public static InputStream testConflictingReturnAndThrowEdges (AmazonS3 amazonS3, String bucketName,
                                              String bucketPrefix, MetricsFactory metricsFactory) {
        Validate.notBlank(bucketName, "Bucket name cannot be blank.");
        Validate.notBlank(bucketPrefix, "Bucket prefix cannot be blank.");
        Metrics localMetrics = metricsFactory.newMetrics();
        localMetrics.addProperty(MetricsUtil.METRICS_PROPERTY_OPERATION_KEY, OPERATION_NAME);
        long startTime = System.currentTimeMillis();

        try {
            // List objects in bucket prefix.
            // Max-keys is 1000 by default.
            ListObjectsV2Result listResult = amazonS3.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName)
                    .withPrefix(bucketPrefix));
            List<S3ObjectSummary> objectSummaries = listResult.getObjectSummaries();
            if (objectSummaries.isEmpty()) {
                // No data in S3 for the requested data, we should have something to provide.
                throw new ExternalDataDownloadException(String.format(
                        "No files to download with provided bucket %s and prefix %s", bucketName, bucketPrefix));
            }
            Vector<InputStream> inputStreams = new Vector<>(objectSummaries.size());
            for (S3ObjectSummary summary : objectSummaries) {
                String s3Key = summary.getKey();
                // Download all objects in the prefix.
                try (S3Object s3Object = amazonS3.getObject(bucketName, s3Key)) {
                    int fileSize = (int) s3Object.getObjectMetadata().getContentLength();
                    try (InputStream inputStream = s3Object.getObjectContent()) {
                        byte[] fileData = IOUtils.toByteArray(inputStream, fileSize);
                        // This impl expects an EOF for each file downloaded.
                        inputStreams.add(new ByteArrayInputStream(fileData));
                    }
                    localMetrics.addCount(FEED_FAILED_COUNT, 0.0, Unit.ONE);
                } catch (IOException e) {
                    log.error("Unable to download mining pool feed \"{}\"", s3Key, e);
                    localMetrics.addCount(FEED_FAILED_COUNT, 1.0, Unit.ONE);
                }
            }
            return new SequenceInputStream(inputStreams.elements());
        } finally {
            MetricsUtil.closeAndEmitTiming(localMetrics, startTime);
        }
    }
}