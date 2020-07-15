package resourceleak;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.io.IOUtils;

public class TestClosingAliasedResourceBuggy {


    public String testClosingAliasedResourceBuggy(String bucketName, String key) throws AmazonServiceException, SdkClientException {

        AmazonS3Client s3client;
        S3Object s3Object = s3client.getObject(bucketName, key);

        try {
            return IOUtils.toString(s3Object.getObjectContent());
        } finally {
            //s3Object.getObjectContent().close();
        }
    }
}
