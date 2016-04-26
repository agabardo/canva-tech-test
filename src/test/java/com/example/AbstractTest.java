package com.example;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

abstract public class AbstractTest {

    /**
     * Attempt to run each Runnable in threads that start processing almost simultaneously so we can have higher chances of finding synchronization bug
     * Inspired from: http://www.planetgeek.ch/2009/08/25/how-to-find-a-concurrency-bug-with-java/
     */
    protected void assertConcurrent(final List<? extends Runnable> runnables, final int maxTimeoutSeconds) throws InterruptedException {
        final int numThreads = runnables.size();
        final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<>());
        final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
        try {
            final CountDownLatch allExecutorThreadsReady = new CountDownLatch(numThreads);
            final CountDownLatch afterInitBlocker = new CountDownLatch(1);
            final CountDownLatch allDone = new CountDownLatch(numThreads);
            for (final Runnable submittedTestRunnable : runnables) {
                threadPool.submit((Runnable) () -> {
                    allExecutorThreadsReady.countDown();
                    try {
                        afterInitBlocker.await();
                        submittedTestRunnable.run();
                    } catch (final Throwable e) {
                        exceptions.add(e);
                    } finally {
                        allDone.countDown();
                    }
                });
            }
            // Wait until all threads are ready
            assertTrue("Timeout initializing threads! Perform long lasting initializations before passing runnables to assertConcurrent",
                    allExecutorThreadsReady.await(runnables.size() * 10, TimeUnit.MILLISECONDS));
            // Start all test runners
            afterInitBlocker.countDown();
            assertTrue("Timeout! More than " + maxTimeoutSeconds + " seconds", allDone.await(maxTimeoutSeconds, TimeUnit.SECONDS));
        } finally {
            // Wait until all thread finished processing before proceeding with our main thread
            threadPool.shutdown();
            threadPool.awaitTermination(maxTimeoutSeconds, TimeUnit.SECONDS);
        }
        assertTrue("Failed with exception(s)" + exceptions, exceptions.isEmpty());
    }
}
