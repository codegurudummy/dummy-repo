package example;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.*;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class Example {

    private final AmazonS3 s3Client;

    public static double conditionWithPagination1(List<String> inputURIs) {
        Long numBytes = 0L;
        for (String uri : inputURIs) {
            AmazonS3URI s3URI = new AmazonS3URI(uri);
            ObjectListing objects = null;
            do {
                if (objects == null) {
                    objects = s3Client.listObjects(s3URI.getBucket(), s3URI.getKey());
                } else {
                    //Should be flagged
                    objects = s3Client.listNextBatchOfObjects(objects);
                }
                for (S3ObjectSummary summary : objects.getObjectSummaries()) {
                    numBytes += summary.getSize();
                }
            } while (objects.isTruncated());
        }

        return null;
    }

    public void conditionWithPagination2(){
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(2);
        ListObjectsV2Result result;

        do {
            //Should be flagged
            result = s3Client.listObjectsV2(req);

            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                System.out.printf(" - %s (size: %d)\n", objectSummary.getKey(), objectSummary.getSize());
            }
            String token = result.getNextContinuationToken();
            System.out.println("Next Continuation Token: " + token);
            req.setContinuationToken(token);
        } while (token != null);
    }

    //Should not be flagged
    public static double notInLoop (){
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(2);
        result = s3Client.listObjectsV2(req);
    }

    //Should not be flagged
    public static double irreleventLoop (){
        while (true) {
            result = s3Client.listObjectsV2(req);
        }
    }

}