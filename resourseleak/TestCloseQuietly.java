package resourceleak;

import com.amazonaws.services.s3.model.S3Object;

import java.io.IOException;


public class TestCloseQuietly {

    public String testCloseQuietly(String bucketName, String key)
            throws AmazonServiceException, SdkClientException {
        rejectNull(bucketName, "Bucket name must be provided");
        rejectNull(key, "Object key must be provided");

        S3Object object = getObject(bucketName, key);
        try {
            return IOUtils.toString(object.getObjectContent());
        } catch (IOException e) {
            throw new SdkClientException("Error streaming content from S3 during download");
        } finally {
            IOUtils.closeQuietly(object, log);
        }
    }

}
