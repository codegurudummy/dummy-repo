package example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

// Based on this code:
// https://code.amazon.com/packages/DevHubProvisioningWorkflowLambda/blobs/df577d7cd4dd8749fdb044973822c874fa679d32/--/src/com/amazon/devhub/provisioningworkflow/lambda/processor/CreateProjectStackProcessor.java#L106-L128
// We received negative feedback from them several times:
// they can't use getObjectAsString() because it doesn't allow overriding credentials.
public class GetObjectAsStringOverrideCredentials {
    private final AmazonS3 amazonS3;

    public String getTemplateFromS31(@NonNull String s3BucketName, @NonNull String s3Key, @NonNull String accountId) throws IOException {
        GetObjectRequest getObjectRequest = new GetObjectRequest(s3BucketName, s3Key).withRequestCredentialsProvider(
            roleAssumer.getDevHubServiceRoleCredentialProvider(accountId));
        return overridesCredentials(s3BucketName, s3Key, getObjectRequest);
    }

    public String getTemplateFromS32(@NonNull String s3BucketName, @NonNull String s3Key, @NonNull FasCredentials fasCredentials) throws IOException {
        GetObjectRequest getObjectRequest = new GetObjectRequest(s3BucketName, s3Key).withRequestCredentialsProvider(
            fasCredentials.getAwsCredentialsProvider());
        return overridesCredentials(s3BucketName, s3Key, getObjectRequest);
    }

    private String overridesCredentials(@NonNull final String s3BucketName, @NonNull final String s3Key,
        final GetObjectRequest getObjectRequest) throws IOException {
        try {
            S3Object s3Object = amazonS3.getObject(getObjectRequest);
            return IOUtils.toString(s3Object.getObjectContent());
        } catch (AmazonS3Exception e) {
            log.warn(e);
            handleS3ExceptionBasedOnErrorCode(e, s3BucketName, s3Key);
            throw e;
        }
    }
}