package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.j_spaces.core.client.Modifiers;
import net.jini.core.lease.Lease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class CopyChunksConsumer implements Runnable {

    public static Logger logger = LoggerFactory.getLogger(CopyChunksConsumer.class);

    private Map<Integer, ISpaceProxy> proxyMap;
    private BlockingQueue<Batch> batchQueue;
    private CopyChunksResponseInfo responseInfo;
    private CopyBarrier copyBarrier;

    CopyChunksConsumer(Map<Integer, ISpaceProxy> proxyMap, BlockingQueue<Batch> batchQueue, CopyChunksResponseInfo responseInfo, CopyBarrier copyBarrier) {
        this.proxyMap = proxyMap;
        this.batchQueue = batchQueue;
        this.responseInfo = responseInfo;
        this.copyBarrier = copyBarrier;
    }

    @Override
    public void run() {
        Exception exception = null;
        try {
            while (!Thread.currentThread().isInterrupted()) {
                WriteBatch writeBatch = null;
                try {
                    Batch batch = batchQueue.poll(5, TimeUnit.SECONDS);
                    if (batch == Batch.EMPTY_BATCH) {
                        return;
                    }
                    if (batch != null) {
                        writeBatch = ((WriteBatch) batch);
                        ISpaceProxy spaceProxy = proxyMap.get(writeBatch.getPartitionId());
                        spaceProxy.writeMultiple(writeBatch.getEntries().toArray(), null, Lease.FOREVER, Modifiers.BACKUP_ONLY);
                        responseInfo.getMovedToPartition().get((short) writeBatch.getPartitionId()).addAndGet(writeBatch.getEntries().size());
                    }
                } catch (InterruptedException e) {
                    logger.error("Copy chunk producer thread " + Thread.currentThread().getId() + "  exited due to interruptedException");
                    exception = new IOException("Copy chunks consumer thread " + Thread.currentThread().getId() + "  was interrupted");
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    logger.error("Consumer thread " + Thread.currentThread().getId() + "  caught exception", e);
                    exception = new IOException("Caught exception while trying to write to partition " +
                            (writeBatch != null ? writeBatch.getPartitionId() : ""), e);
                    return;
                }
            }
        } finally {
            if (exception != null) {
                copyBarrier.completeExceptionally(exception);
            } else {
                copyBarrier.complete();
            }
        }
    }
}
