package com.gigaspaces.internal.server.space.repartitioning;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CopyBarrier {

    private final int totalThreads;
    private volatile Exception exception;
    private final CountDownLatch countDownLatch;

    CopyBarrier(int totalThreads) {
        this.countDownLatch = new CountDownLatch(totalThreads);
        this.totalThreads = totalThreads;
    }

    public void complete() {
        countDownLatch.countDown();
    }

    void completeExceptionally(Exception e) {
        exception = e;
        while (countDownLatch.getCount() > 0) {
            countDownLatch.countDown();
        }
    }

    public void await(long millis) throws Exception {

        boolean await = countDownLatch.await(millis, TimeUnit.MILLISECONDS);

        if (exception != null) {
            throw exception;
        }

        if (!await) {//timed out
            throw new TimeoutException("Timeout while waiting for copy consumers , " + (totalThreads - countDownLatch.getCount()) + " out of " + totalThreads + " finished successfully");
        }
    }
}