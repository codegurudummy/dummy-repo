package resourceleak;

import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.io.IOUtils;

public class TestClosingAliasedResource {


    public String testClosingAliasedResource(String bucketName, String key) throws AmazonServiceException, SdkClientException {

        S3Object s3Object = getObject(bucketName, key);

        try {
            return IOUtils.toString(s3Object.getObjectContent());
        } finally {
            s3Object.getObjectContent().close();
        }
    }
}
