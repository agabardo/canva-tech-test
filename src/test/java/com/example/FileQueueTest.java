package com.example;

import com.amazonaws.services.sqs.model.Message;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FileQueueTest extends AbstractTest {

    @Rule public TemporaryFolder folder = new TemporaryFolder();
    private final int MAX_TIMEOUT_IN_SECONDS = 20;
    private FileQueue queue;

    @Before
    public void setUp() throws Exception {
        File parent = folder.getRoot();
        File file = new File(parent, "queue-file");
        queue = new FileQueue(file);
    }

    @Test
    public void shouldHandleAddToQueue() throws IOException {
        String expectedString = "test message";
        queue.add(expectedString);
        queue.add(expectedString);
        assertEquals(2, queue.size());
    }

    @Test
    public void shouldHandlePollToQueue() throws IOException {
        String expectedString = "test message";
        queue.add(expectedString);
        queue.add(expectedString);
        assertEquals(2, queue.size());

        Message message = queue.poll();
        assertEquals(expectedString, message.getBody());
        assertEquals(1, queue.size());
    }

    @Test
    public void shouldHandleAddToQueueSimultaneously() throws IOException, InterruptedException {
        String expectedString = "test message";
        int executionTimes = 500;

        // Attempt adding a single element in the queue simultaneously
        List<Runnable> runnables = new ArrayList<>();
        for (int i = 0; i < executionTimes; i++) {
            runnables.add(() -> {
                try {
                    queue.add(expectedString);
                } catch (IOException ignored) {}
            });
        }
        assertConcurrent(runnables, MAX_TIMEOUT_IN_SECONDS);
        assertEquals(executionTimes, queue.size());
    }
}
