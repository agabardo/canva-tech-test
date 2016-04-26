package com.example;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;

import java.util.concurrent.ConcurrentMap;

public class InMemoryQueueService implements QueueService {

    private ConcurrentMap<String, InMemoryQueue> queues;

    public InMemoryQueueService(ConcurrentMap<String, InMemoryQueue> queues) {
        this.queues = queues;
    }

    @Override
    public void sendMessage(String queueUrl, String messageBody) {
        this.getQueue(queueUrl).add(messageBody);
    }

    @Override
    public Message receiveMessage(String queueUrl) {
        return this.getQueue(queueUrl).poll();
    }

    @Override
    public void deleteMessage(String queueUrl, String receiptHandle) {
        this.getQueue(queueUrl).delete(receiptHandle);
    }

    private InMemoryQueue getQueue(String queueUrl) {
        InMemoryQueue queue = queues.get(queueUrl);
        if (queue == null) {
            throw new QueueDoesNotExistException("Queue " + queueUrl + " does not exists");
        }
        return queue;
    }
}
