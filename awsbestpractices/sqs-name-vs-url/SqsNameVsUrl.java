package com.amazon.promotions.stream.dlq;

import com.amazonaws.services.sqs.model.Message;

public class SqsNameVsQueue implements Callable<Message> {
    public void main() {
        // Name instead of url
        sqsMetricsClient.changeMessageVisibility(queueName, "foo", "bar");
        // Name instead of url
        sqsMetricsClient.changeMessageVisibility(name, "foo", "bar");
        // Name instead of url
        sqsMetricsClient.sendMessageBatch(name, "foo", "bar");
        // API accepts name - correct
        sqsMetricsClient.listQueues(name);
        // Url - correct
        sqsMetricsClient.changeMessageVisibility(queueUrl, "foo", "bar");
    }
}