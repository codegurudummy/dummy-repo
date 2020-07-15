package example;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.amazonaws.services.codedeploy.AmazonCodeDeployClient;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class LoopsToBatches {
    AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().build();

    public void batchablePutItemOnCollection_1(JobExecutionContext context) throws JobExecutionException {
        MessageConsumer consumer = session.createConsumer(queue);
        for (int i = 1; i < numMsgs + 1; i++) {
            Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
            TextMessage txtMessage = (TextMessage) consumerMessage;
            String msgBody = txtMessage.getText().toString();
            item.put("msg_body", new AttributeValue().withS(msgBody));
            // Can be batched
            dynamoDBClient.putItem(getTableName, item);
        }
    }

    // Based on https://github.com/locationtech/geowave/blob/5db4f2ba3be6094d6bedfb908afb5126469c76fa/extensions/datastores/dynamodb/src/main/java/org/locationtech/geowave/datastore/dynamodb/operations/DynamoDBWriter.java#L156
    private void batchablePutItemOnCollection_2(final Map<String, List<WriteRequest>> map) {
        for (final Entry<String, List<WriteRequest>> requests : map.entrySet()) {
            for (final WriteRequest r : requests.getValue()) {
                if (r.getPutRequest() != null) {
                    dynamoDBClient.putItem(requests.getKey(), r.getPutRequest().getItem());
                }
            }
        }
    }

    private void notInALoop(String tableName) {
        // Not in a loop
        dynamoDBClient.putItem(getTableName, new HashMap<>());
    }

    // Based on https://github.com/apache/metamodel/blob/3df020a31be3a4c4d82d1305670878b25d85bcef/dynamodb/src/main/java/org/apache/metamodel/dynamodb/DynamoDbDataContext.java#L288
    public void dynamoWrongGetItem() {
        final GetItemResult item = dynamoDBClient.getItem(getItemRequest);

        final Object[] values = new Object[selectItems.size()];
        for (int i = 0; i < values.length; i++) {
            // Wrong getItem! Must not be flagged
            final AttributeValue attributeValue = item.getItem().get(attributeNames.get(i));
            values[i] = DynamoDbUtils.toValue(attributeValue);
        }
    }

    // Based on https://github.com/awslabs/aws-codedeploy-plugin/blob/6b74409a2f44faf712ca704ba095cac95fdc5c42/src/main/java/com/amazonaws/codedeploy/AWSCodeDeployPublisher.java#L486
    private boolean waiter(AWSClients aws, String deploymentId) throws InterruptedException {
        AmazonCodeDeployClient codeDeployClient = new AmazonCodeDeployClient();

        GetDeploymentRequest deployInfoRequest = new GetDeploymentRequest();
        deployInfoRequest.setDeploymentId(deploymentId);

        DeploymentInfo deployStatus = codeDeployClient.getDeployment(deployInfoRequest).getDeploymentInfo();

        boolean success = true;
        long pollingTimeoutMillis = this.pollingTimeoutSec * 1000L;
        long pollingFreqMillis = this.pollingFreqSec * 1000L;

        while (deployStatus == null || deployStatus.getCompleteTime() == null) {

            if (deployStatus == null) {
                logger.println("Deployment status: unknown.");
            } else {
                DeploymentOverview overview = deployStatus.getDeploymentOverview();
                logger.println("Deployment status: " + deployStatus.getStatus() + "; instances: " + overview);
            }

            // Not batchable - the request is always the same
            deployStatus = codeDeployClient.getDeployment(deployInfoRequest).getDeploymentInfo();
            Date now = new Date();

            if (now.getTime() - startTimeMillis >= pollingTimeoutMillis) {
                this.logger.println("Exceeded maximum polling time of " + pollingTimeoutMillis + " milliseconds.");
                success = false;
                break;
            }

            Thread.sleep(pollingFreqMillis);
        }

        logger.println("Deployment status: " + deployStatus.getStatus() + "; instances: " + deployStatus.getDeploymentOverview());

        if (!deployStatus.getStatus().equals(DeploymentStatus.Succeeded.toString())) {
            this.logger.println("Deployment did not succeed. Final status: " + deployStatus.getStatus());
            success = false;
        }

        return success;
    }

    // Based on https://github.com/aws-samples/aws-dynamodb-examples/blob/c68fafca35fb4d46fe39364d66593f5ac0b67fdc/src/main/java/com/amazonaws/codesamples/gsg/StreamsRecordProcessor.java#L55
    void wrapper() {
        for (Record record : records) {
            // Wrapper/proxy - should not flag it.
            StreamsAdapterDemoHelper.putItem(dynamoDBClient, tableName, streamRecord.getDynamodb().getNewImage());
        }
    }

    // Based on https://github.com/travel-cloud/Cheddar/blob/43d8d3f6c26459ca9e3347682a2bcac2a79c0560/cheddar/cheddar-integration-aws/src/main/java/com/clicktravel/infrastructure/persistence/aws/dynamodb/AbstractDynamoDbTemplate.java#L158
    protected final <T extends Item> Collection<PropertyDescriptor> batchablePutInLongComplexLoop(final T item,
                                                                                                  final ItemConfiguration itemConfiguration,
                                                                                                  final Collection<PropertyDescriptor> constraintPropertyDescriptors) {
        final Set<PropertyDescriptor> createdConstraintPropertyDescriptors = new HashSet<>();
        ItemConstraintViolationException itemConstraintViolationException = null;
        for (final UniqueConstraint uniqueConstraint : itemConfiguration.uniqueConstraints()) {
            final String uniqueConstraintPropertyName = uniqueConstraint.propertyName();
            final PropertyDescriptor uniqueConstraintPropertyDescriptor = uniqueConstraint.propertyDescriptor();
            if (constraintPropertyDescriptors.contains(uniqueConstraintPropertyDescriptor)) {
                final AttributeValue uniqueConstraintAttributeValue = DynamoDbPropertyMarshaller.getValue(item,
                        uniqueConstraintPropertyDescriptor);
                if (uniqueConstraintAttributeValue == null) {
                    continue;
                }
                if (uniqueConstraintAttributeValue.getS() != null) {
                    uniqueConstraintAttributeValue.setS(uniqueConstraintAttributeValue.getS().toUpperCase());
                }
                final Map<String, AttributeValue> attributeMap = new HashMap<>();
                attributeMap.put("property", new AttributeValue(uniqueConstraintPropertyName));
                attributeMap.put("value", uniqueConstraintAttributeValue);
                final Map<String, ExpectedAttributeValue> expectedResults = new HashMap<>();
                expectedResults.put("value", new ExpectedAttributeValue(false));
                final String indexTableName = databaseSchemaHolder.schemaName() + "-indexes."
                        + itemConfiguration.tableName();
                final PutItemRequest itemRequest = new PutItemRequest().withTableName(indexTableName)
                        .withItem(attributeMap).withExpected(expectedResults);
                try {
                    dynamoDBClient.putItem(itemRequest);
                    createdConstraintPropertyDescriptors.add(uniqueConstraintPropertyDescriptor);
                } catch (final ConditionalCheckFailedException e) {
                    itemConstraintViolationException = new ItemConstraintViolationException(
                            uniqueConstraintPropertyName, "Unique constraint violation on property '"
                            + uniqueConstraintPropertyName + "' ('" + uniqueConstraintAttributeValue
                            + "') of item " + item.getClass());
                    break;
                } catch (final AmazonServiceException e) {
                    throw new PersistenceResourceFailureException(
                            "Failure while attempting DynamoDb put (creating unique constraint index entry)", e);
                }
            }
        }
        if (itemConstraintViolationException != null) {
            try {
                deleteUniqueConstraintIndexes(item, itemConfiguration, createdConstraintPropertyDescriptors);
            } catch (final Exception e) {
                logger.error(e.getMessage(), e);
            }
            throw itemConstraintViolationException;
        }
        return createdConstraintPropertyDescriptors;
    }

    private void batchabeS3Delete(final AmazonS3 s3, final List<String> keys) {
        for (final String key : keys) {
            String bucketName;
            final DeleteObjectRequest request = new DeleteObjectRequest("my-bucket", key);
            s3.deleteObject(request);
        }
    }
}