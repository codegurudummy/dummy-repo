import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;

import example.foo.FooClient;

public class DummyFile {
    private AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
    private AmazonKinesisClient kinesisClient = AmazonKinesisClientBuilder.defaultClient();
    private FooClient fooClient;

    // Should flag and suggest v2
    public void s3ListObjects() {
        s3Client.listObjects(new ListObjectsRequest());
    }

    // Should not flag
    public void s3ListObjectsV2() {
        s3Client.listObjectsV2(new ListObjectsV2Request());
    }

    // Should flag and suggest Transfer Manager
    public void s3MultiPartUpload() {
        s3Client.initiateMultipartUpload(new InitiateMultipartUploadRequest());
    }

    // Should not flag
    public void wrongReceiver() {
        fooClient.listObjects(new ListObjectsRequest());
    }

    // Should flag
    public void kinesisEnhancedFanOut() {
        kinesisClient.getRecords(null);
    }
}
