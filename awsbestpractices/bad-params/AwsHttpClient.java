package example;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;

public class AwsHttpClientTestCases {

    public void awsHttpClient() {
        AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(credentialsProvider, clientConfiguration);
        // Http endpoint! Must be flagged
        dynamoDBClient.setEndpoint("http://dynamodb.eu-west-1.amazonaws.com/");

        // Http endpoint - everything's good
        dynamoDBClient.withEndpoint("https://dynamodb.eu-west-1.amazonaws.com/");

        MyClient hello = new MyClient();
        // Not aws endpoint - nothing to do.
        hello.setEndpoint("http://my-thing.com");

        // Not endpoint at all
        dynamoDBClient.setIrrelevantParameter("http://dynamodb.eu-west-1.amazonaws.com/");

        // EndpointConfiguration - should flag.
        dynamoDBClient = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration("http://dynamodb.eu-west-1.amazonaws.com/", "us-west-2"))
                .build();
    }
}
