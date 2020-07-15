package example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import lombok.extern.slf4j.Slf4j;


public class Example {
    AmazonS3 s3;
    S3ObjectMetadata objectMetadata;

    // Can be replaced
    public boolean doesObjectExist_replacable_1() {
        try {
            s3.getObjectMetadata();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // Can be replaced
    // Based on https://github.com/HubSpot/SlimFast/blob/59f74c9bf2d063d7d1e345fa1079d81435c5dcef/slimfast-plugin/src/main/java/com/hubspot/maven/plugins/slimfast/DefaultFileUploader.java#L44
    private boolean doesObjectExist_replacable_2(String bucket, String key) throws MojoFailureException {
        try {
            s3.getObjectMetadata(bucket, key);
            return true;
        } catch (SdkBaseException e) {
            if (e instanceof AmazonServiceException && ((AmazonServiceException) e).getStatusCode() == 404) {
                return false;
            } else {
                throw new MojoFailureException("Error getting object metadata for key " + key, e);
            }
        }
    }

    // Can be replaced
    // Based on https://code.amazon.com/packages/AWSSeerPlanSupport/blobs/ee7d0c39077b85bccdd94afed117a27ccfa12183/--/src/com/amazonaws/seer/spark/S3Util.java#L107-L121
    private boolean doesObjectExist_replacable_3(AWSCredentialsProvider credentials, String path) {
        AmazonS3URI s3BucketAndKey = new AmazonS3URI(path);
        try {
            AmazonS3 client = getClient(credentials);
            String bucketName = s3BucketAndKey.getBucket();
            client.getObject(bucketName, s3BucketAndKey.getKey());
            return true;
        } catch (AmazonServiceException e) {
            String errorCode = e.getErrorCode();
            if (!S3_INVALID_BUCKET_ERROR_CODE.equals(errorCode) && !S3_INVALID_KEY_ERROR_CODE.equals(errorCode)) {
                throw e;
            }
            return false;
        }
    }

    // returned
    public S3Object doesObjectExist_returned() {
        try {
            return s3.getObjectMetadata();
        } catch (Exception e) {
            return null;
        }
    }

    // non in try catch
    public boolean doesObjectExist_noTryCatch() {
        s3.getObjectMetadata();
    }

    // used
    public S3Object doesObjectExist_used_1() {
        try {
            System.out.println(s3.getObjectMetadata());
        } catch (Exception e) {
            return null;
        }
    }

    // used
    public S3Object doesObjectExist_used_2() {
        try {
            System.out.println(s3.getObjectMetadata());
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    // used
    // Based on
    // https://github.com/soabase/exhibitor/blob/d345d2d45c75b0694b562b6c346f8594f3a5d166/exhibitor-core/src/main/java/com/netflix/exhibitor/core/backup/s3/S3BackupProvider.java#L223
    public BackupStream doesObjectExist_used_3(Exhibitor exhibitor, BackupMetaData backup, Map<String, String> configValues) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        RetryPolicy     retryPolicy = makeRetryPolicy(configValues);
        S3Object        object = null;
        int             retryCount = 0;
        while ( object == null )
        {
            try
            {
                object = s3.getObject(configValues.get(CONFIG_BUCKET.getKey()), toKey(backup, configValues));
            }
            catch ( AmazonS3Exception e)
            {
                if ( e.getErrorType() == AmazonServiceException.ErrorType.Client )
                {
                    exhibitor.getLog().add(ActivityLog.Type.ERROR, "Amazon client error: " + ActivityLog.getExceptionMessage(e));
                    return null;
                }

                if ( !retryPolicy.allowRetry(retryCount++, System.currentTimeMillis() - startMs, RetryLoop.getDefaultRetrySleeper()) )
                {
                    exhibitor.getLog().add(ActivityLog.Type.ERROR, "Retries exhausted: " + ActivityLog.getExceptionMessage(e));
                    return null;
                }
            }
        }

        final Throttle      throttle = makeThrottle(configValues);
        final InputStream   in = object.getObjectContent();
        final InputStream   wrappedstream = new InputStream()
        {
            @Override
            public void close() throws IOException
            {
                in.close();
            }

            @Override
            public int read() throws IOException
            {
                throttle.throttle(1);
                return in.read();
            }

            @Override
            public int read(byte[] b) throws IOException
            {
                int bytesRead = in.read(b);
                if ( bytesRead > 0 )
                {
                    throttle.throttle(bytesRead);
                }
                return bytesRead;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException
            {
                int bytesRead = in.read(b, off, len);
                if ( bytesRead > 0 )
                {
                    throttle.throttle(bytesRead);
                }
                return bytesRead;
            }
        };

        return new BackupStream()
        {
            @Override
            public InputStream getStream()
            {
                return wrappedstream;
            }

            @Override
            public void close() throws IOException
            {
                in.close();
            }
        };
    }

    // used
    // Simplified version of used_3
    public BackupStream doesObjectExist_used_4() {
        S3Object object = null;
        while ( object == null ) {
            try {
                object = s3.getObject("foo", "bar");
            }
            catch ( AmazonS3Exception e) {
                return null;
            }
        }

        System.out.println(object.getObjectContent());
    }

    // Sets field
    public void doesObjectExist_setToField() {
        try {
            this.objectMetadata = s3.getObjectMetadata();
        }
        catch (Exception e) {
        }
        return null;
    }

    // Read to file
    public void doesObjectExist_readToFile() {
        try {
            s3.getObject(new GetObjectRequest("foo", "bar"), new File("my-file-path"));
        } catch (Exception e) {
        }
    }

    // Can be replaced
    public boolean doesBucketExist_headBucket() {
        try {
            s3.headBucket(new HeadBucketRequest());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // Can be replaced
    public boolean doesBucketExist_listObjects() {
        try {
            s3.listObjects();
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}