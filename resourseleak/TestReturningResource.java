package resourceleak;

import com.amazonaws.services.s3.model.S3Object;

import java.io.InputStream;


public class TestReturningResource {

    public InputStream testReturningResource(String bucket, String key) {
        S3Object s3Object;
        try {
            s3Object = s3Client.getObject(bucket, key);
            return s3Object.getObjectContent();
        } catch (Exception e) {
            throw new RuntimeException(String.format("unable to download file [s3://%s/%s], download was aborted", bucket, key), e);
        }
    }
}
