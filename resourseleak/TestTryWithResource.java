package resourceleak;

import com.amazonaws.services.s3.model.S3Object;

import java.io.InputStream;
import java.io.InputStreamReader;


public class TestTryWithResource {

    public String testTryWithResource(String bucket, String key) {

        LOG.info("Loading " + bucket + ":" + key + " to string");
        final GetObjectRequest request = new GetObjectRequest(bucket, key);
        try (final S3Object object = s3Client.getObject(request);
             final InputStream is = object.getObjectContent();
             final InputStreamReader reader = new InputStreamReader(is)) {
            return IOUtils.toString(reader);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
