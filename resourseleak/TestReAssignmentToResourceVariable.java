package resourceleak;

import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.io.IOUtils;

public class TestReAssignmentToResourceVariable {


    public String testReAssignmentToResourceVariable(String bucketName, String key) throws AmazonServiceException, SdkClientException {

        S3Object s3Object = getObject(bucketName, key);
        S3Object object = s3Object;

        try {
            return IOUtils.toString(object.getObjectContent());
        } finally {
            object.close();
        }
    }
}
