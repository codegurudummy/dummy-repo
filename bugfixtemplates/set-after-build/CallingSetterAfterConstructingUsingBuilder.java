package bugfixtemplates;

class CallingSetterAfterConstructingUsingBuilder {
    
    public getAmazonS3ClientPositive(@Named("s3OdinMaterial") String odinMaterial) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new OdinAWSCredentialsProvider(odinMaterial))
                .build();
        s3Client.setRegion(Region.getRegion(Regions.US_WEST_2));
        return s3Client;
    }
    
    public getAmazonS3ClientNegative(@Named("s3OdinMaterial") String odinMaterial) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new OdinAWSCredentialsProvider(odinMaterial))
                .withRegion(Regions.US_WEST_2)
                .build();
        return s3Client;
    }

}
