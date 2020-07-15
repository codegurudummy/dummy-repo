package example;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;

public class ParsingErrorMessage {

    private AmazonS3Client s3Client;

    public String normalLogging() {
        try {
            s3Client.getObject("foo", "bar");
        } catch (AmazonServiceException ase) {
            // Must not be flagged
            logger.info("Caught error: " + ase.getMessage());
        }
    }

    public String branching() {
        try {
            s3Client.getObject("foo", "bar");
        } catch (AmazonServiceException ase) {
            // Must be flagged
            if (ase.getMessage().contains("foo")) {
                logger.info("one thing");
            } else {
                logger.info("another thinking");
            }
        }
    }

    public String otherException() {
        try {
            s3Client.getObject("foo", "bar");
        } catch (Exception e) {
            // Must not be flagged - not the target exception
            if (ase.getMessage().contains("foo")) {
                logger.info("one thing");
            } else {
                logger.info("another thinking");
            }
        }
    }

    public void nullChecking() {
        try {
            s3Client.getObject("foo", "bar");
        } catch (AmazonServiceException e) {
            // Should not be flagged - simpy null-checked
            if (e.getMessage() != null) {
                throw new FileSystemException(e.getMessage(), e);
            } else {
                throw new FileSystemException(e);
            }

            // A tiny bit trickier, but should not be flagged either
            String s3message = e.getMessage();
            if (s3message != null) {
                throw new FileSystemException(s3message, e);
            } else {
                throw new FileSystemException(e);
            }
        }
    }
}