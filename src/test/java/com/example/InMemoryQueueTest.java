package com.example;

import com.amazonaws.services.sqs.model.Message;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

public class InMemoryQueueTest extends AbstractTest {

    private final int MAX_TIMEOUT_IN_SECONDS = 20;
    private InMemoryQueue queue;
    private ScheduledExecutorService scheduledExecutorService;

    @Before
    public void setUp() throws Exception {
        this.scheduledExecutorService = mock(ScheduledExecutorService.class);
        this.queue = new InMemoryQueue(scheduledExecutorService);
    }

    @Test
    public void shouldHandleAddToQueueSimultaneously() throws InterruptedException {
        int executionTimes = 1000;

        // Attempt adding a single element in the queue simultaneously
        List<Runnable> runnables = new ArrayList<>();
        for (int i = 0; i < executionTimes; i++) {
            runnables.add(() -> queue.add("test message body"));
        }
        assertConcurrent(runnables, MAX_TIMEOUT_IN_SECONDS);

        verify(scheduledExecutorService, never()).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        assertEquals(executionTimes, queue.size());
        assertEquals(0, queue.inFlightSize());
    }

    @Test
    public void shouldHandlePollOnQueueSimultaneously() throws InterruptedException {
        int executionTimes = 1000;
        int visibilityTimeout = 10;
        this.queue = queue.withVisibilityTimeout(visibilityTimeout);
        ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
        doReturn(mockFuture).when(this.scheduledExecutorService).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

        // Populate the queue with some messages
        List<String> sentMessages = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < executionTimes; i++) {
            String message = "test message body " + i;
            // Save the sent message list unto an array
            sentMessages.add(message);
            queue.add(message);
        }

        List<Runnable> runnables = new ArrayList<>();
        for (int i = 0; i < executionTimes; i++) {
            runnables.add(() -> {
                String messageBody = queue.poll().getBody();
                // Check every single one of them got polled once
                assertTrue(sentMessages.remove(messageBody));
            });
        }

        // Attempt polling element in the queue simultaneously
        assertConcurrent(runnables, MAX_TIMEOUT_IN_SECONDS);

        // Verify the executor being called and upon poll we set it to delete the message within a certain timeout
        verify(scheduledExecutorService, times(executionTimes)).schedule(isA(Runnable.class), eq(new Long(visibilityTimeout)), eq(TimeUnit.SECONDS));
        assertEquals(0, queue.size());
        assertEquals(0, sentMessages.size());
        assertEquals(executionTimes, queue.inFlightSize());
    }

    @Test
    public void shouldHandleDeleteOnQueueSimultaneously() throws InterruptedException {
        int executionTimes = 1000;
        int visibilityTimeout = 10;
        this.queue = queue.withVisibilityTimeout(visibilityTimeout);
        ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
        doReturn(mockFuture).when(this.scheduledExecutorService).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

        // Populate the queue with some messages
        for (int i = 0; i < executionTimes; i++) {
            queue.add("test message body");
        }

        // Poll the queue and store the receipts as list
        List<String> receiptHandles = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < executionTimes; i++) {
            Message message = queue.poll();
            receiptHandles.add(message.getReceiptHandle());
        }
        // Verify we are the executed service is being called within execution times
        verify(scheduledExecutorService, times(executionTimes)).schedule(isA(Runnable.class), eq(new Long(visibilityTimeout)), eq(TimeUnit.SECONDS));
        assertEquals(executionTimes, queue.inFlightSize());

        // Delete the message in almost simultaneous manner
        List<Runnable> runnables = new ArrayList<>();
        for (String receiptHandle : receiptHandles) {
            runnables.add(() -> queue.delete(receiptHandle));
        }

        // Attempt deleting element in the queue simultaneously
        assertConcurrent(runnables, MAX_TIMEOUT_IN_SECONDS);

        // Verify that our futures are being canceled since we now have deleted those messages
        verify(mockFuture, times(executionTimes)).cancel(anyBoolean());
        assertEquals(0, queue.size());
        assertEquals(0, queue.inFlightSize());
    }
}
