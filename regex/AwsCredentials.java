package regex;

public class Example {
    public void hardCodedAccessKey() {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials("ASIAJLVYNHUWCEXAMPLE", "foo");
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();
    }

    public void hardCodedSecretKey() {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials("foo", "Rh30BNyj+qNI4ftYRteoZbHJ3X4Ln71QtEXAMPLE");
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();
    }

    public void notHardCoded() {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
                System.getenv().get("access-key"), System.getenv().get("secret-key"));
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();
    }

    public void justStrings() {
        System.out.println("AAAAAAAAAAAAAAAAAAAA"); // 20 characters long string
        System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"); // 40 characters long string
    }
}
