package example;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.glue.AWSGlueClient;
import com.amazonaws.services.glue.model.BatchDeleteTableRequest;
import com.amazonaws.services.glue.model.BatchDeleteTableResult;
import com.amazonaws.services.glue.model.BatchDeleteTableVersionRequest;
import com.amazonaws.services.glue.model.BatchDeleteTableVersionResult;

public class Example {
    private AWSGlueClient client;

    // Must be flagged
    public void ignoreOutput_1(final String databaseName, final List<String> tableNames) {
        client.batchDeleteTable(new BatchDeleteTableRequest().withDatabaseName(databaseName)
                .withTablesToDelete(tableNames));
    }

    // Must be flagged.
    // Based on
    // https://code.amazon.com/packages/IronmanAdminService/blobs/aa325ce9f79c445e5ff51b46cddf9259b97e7a9c/--//src/com/amazonaws/sagemaker/admin/activity/operational_support/DeadLetterSqsMessages.java#L124
    public void ignoreOutput_2(final String accountId,
                               final String queueName,
                               List<Message> messages) {
        AmazonSQS sqsClient = clientProvider.getSqsClientForRead(accountId);
        String dlqUrl = getQueueUrl(sqsClient, accountId, queueName).getDlq();

        List<DeleteMessageBatchRequestEntry> deleteMessages = messages.stream()
                .map(x -> new DeleteMessageBatchRequestEntry()
                        .withId(x.getMessageId())
                        .withReceiptHandle(x.getReceiptHandle()))
                .collect(Collectors.toList());

        sqsClient.deleteMessageBatch(dlqUrl, deleteMessages);
    }

    // Must be flagged.
    // Based on
    // https://code.amazon.com/packages/IronmanSearchService/blobs/8acfee2d8d34459c762b4fa4ee525cbf76aaa2b4/--//src/com/amazon/ironmansearchservice/activity/ProcessS3EventDlqMessagesActivity.java#L95
    private List<String> ignoreOutput_3(final int numberOfMessages, AmazonSQS amazonSQS) {
        List<String> messages = new ArrayList<>();
        if (!StringUtils.isBlank(queueURL)) {
            try {
                int batchSteps = 1;
                int maxForBatch = numberOfMessages;
                if ((numberOfMessages > MAX_MESSAGES)) {
                    batchSteps = (numberOfMessages / MAX_MESSAGES) + ((numberOfMessages % MAX_MESSAGES > 0) ? 1 : 0);
                    maxForBatch = MAX_MESSAGES;
                }

                for (int i = 0; i < batchSteps; i++) {
                    List<Message> list = amazonSQS.receiveMessage(new ReceiveMessageRequest(queueURL).
                            withMaxNumberOfMessages(maxForBatch).withWaitTimeSeconds(POLLING_INTERVAL)).getMessages();
                    if (list != null && !list.isEmpty()) {
                        List<DeleteMessageBatchRequestEntry> del = new ArrayList<>();
                        for (Message msg : list) {
                            messages.add(msg.getBody());
                            del.add(new DeleteMessageBatchRequestEntry(msg.getMessageId(), msg.getReceiptHandle()));
                        }
                        amazonSQS.deleteMessageBatch(queueURL, del);
                    }
                }
            } catch (AmazonServiceException ase) {
                logException(ase);
            } catch (AmazonClientException ace) {
                logger.error("Could not reach SQS. {}", ace.toString());
            }
        }
        return messages;
    }

    // Must be flagged.
    // Based on
    // https://github.com/Erudika/para/blob/5d7d776283cc16e370cd493f3070fb591c8ba685/para-server/src/main/java/com/erudika/para/queue/AWSQueueUtils.java#L224
    private List<String> ignoreOutput_4(final int numberOfMessages, AmazonSQS amazonSQS) {
        ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
                .withQueueUrl(dlqURL)
                .withMaxNumberOfMessages(numberOfMessages)
                .withVisibilityTimeout(2)
                .withWaitTimeSeconds(0);
        ReceiveMessageResult receiveMessageResult = amazonSQS.receiveMessage(receiveRequest);

        List<MessagePair> processedMessages = receiveMessageResult.getMessages().stream()
                .map(this::processMessage)
                .collect(Collectors.toList());

        if (!processedMessages.isEmpty()) {
            DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest()
                    .withQueueUrl(dlqURL)
                    .withEntries(processedMessages.stream()
                            .map(MessagePair::getMessage)
                            .map(x -> new DeleteMessageBatchRequestEntry()
                                    .withId(x.getMessageId())
                                    .withReceiptHandle(x.getReceiptHandle()))
                            .collect(Collectors.toList()));
            amazonSQS.deleteMessageBatch(deleteMessageBatchRequest);
        }

        return processedMessages.stream()
                .map(MessagePair::getS3Keys)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    // Printed - should not flag
    public void printOutput(final String databaseName, final List<String> tableNames) {
        final BatchDeleteTableResult result = client.batchDeleteTable(
                new BatchDeleteTableRequest().withDatabaseName(databaseName).withTablesToDelete(tableNames));
        System.out.println(result.getErrors());
    }

    // Returned - should not flag
    public List<TableError> returnOutput(final String databaseName, final List<String> tableNames) {
        final BatchDeleteTableResult result = client.batchDeleteTable(
                new BatchDeleteTableRequest().withDatabaseName(databaseName).withTablesToDelete(tableNames));
        return result.getErrors();
    }

}