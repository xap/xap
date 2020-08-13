package com.gigaspaces.internal.server.space.repartitioning;

public class CopyBarrier {

    private int totalThreads;
    private int count;
    private Exception exception;

    public CopyBarrier(int totalThreads) {
        this.totalThreads = totalThreads;
    }

    public synchronized void complete() {
        count++;
        notifyAll();
    }

    public synchronized void completeExceptionally(Exception e) {
        count++;
        exception = e;
        notifyAll();
    }

    public synchronized void await(long milis) throws Exception {
        long start = System.currentTimeMillis();
        while (count < totalThreads && exception == null && System.currentTimeMillis() - start < milis) {
            wait(milis);
        }

        if (exception != null) {
            throw exception;
        }
    }
}