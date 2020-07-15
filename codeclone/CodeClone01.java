package codeclone;


import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3BucketAdapter {

  // move this to brazil config
  public static final String DEFAULT_BUCKET_NAME = "guru-review-bucket";

  private final AmazonS3Client s3;

  public S3BucketAdapter(AmazonS3Client s3) {
    this.s3 = s3;
  }

  public void demoGuru(String jsonBucket) {
    
    try {
      s3.createBucket(array.get(0).getAsString());
      s3.setBucketLifecycleConfiguration(array.get(0).getAsString(), endpointConfig);
    } catch (AmazonClientException ex) {
      String log = "Exception caught trying to create bucket";
      log.error(log, ex);
      throw new RuntimeException(logText, ex);
    }


    try {
      s3.createBucket(array.get(1).getAsString());
      s3.setBucketLifecycleConfiguration(array.get(1).getAsString(), endpointConfig);
    } catch (AmazonClientException ex) {
      String log = "Exception caught trying to create bucket";
      log.error(log, ex);
      throw new RuntimeException(logText, ex);
    }
  }

}