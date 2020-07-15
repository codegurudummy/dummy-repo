package resourceleak;

import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.io.IOUtils;

public class TestAssignmentToDataMember {

    S3Object s3Object;

    public String testAssignmentToDataMember(String bucketName, String key) throws AmazonServiceException, SdkClientException {

        s3Object = getObject(bucketName, key);
        String output = IOUtils.toString(object.getObjectContent());
        return output;
    }
}
