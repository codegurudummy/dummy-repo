package resourceleak;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.io.IOUtils;

public class TestReAssignmentToResourceVariableBuggy {


    public String testReAssignmentToResourceVariableBuggy(String bucketName, String key) throws AmazonServiceException, SdkClientException {

        AmazonS3Client s3Client;
        S3Object s3Object = s3Client.getObject(bucketName, key);
        S3Object object = s3Object;

        try {
            return IOUtils.toString(object.getObjectContent());
        } finally {
            //object.close();
        }
    }
}
