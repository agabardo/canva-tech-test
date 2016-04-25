package com.example;

import com.amazonaws.services.sqs.model.Message;

public class FileQueueService implements QueueService {

    @Override
    public void sendMessage(String queueUrl, String messageBody) {

    }

    @Override
    public Message receiveMessage(String queueUrl) {
        return null;
    }

    @Override
    public void deleteMessage(String queueUrl, String receiptHandle) {

    }
}
