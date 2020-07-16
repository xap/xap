package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.query.IdsQuery;
import com.j_spaces.core.client.Modifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class DeleteChunksConsumer implements Runnable {

    public static Logger logger = LoggerFactory.getLogger("org.openspaces.admin.internal.pu.scale_horizontal.ScaleManager");

    private ISpaceProxy space;
    private BlockingQueue<Batch> batchQueue;
    private DeleteChunksResponseInfo responseInfo;

    DeleteChunksConsumer(ISpaceProxy space, BlockingQueue<Batch> batchQueue, DeleteChunksResponseInfo responseInfo) {
        this.space = space;
        this.batchQueue = batchQueue;
        this.responseInfo = responseInfo;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            DeleteBatch deleteBatch;
            try {
                Batch batch = batchQueue.poll(5, TimeUnit.SECONDS);
                if (batch == Batch.EMPTY_BATCH) {
                    return;
                }
                if (batch != null) {
                    deleteBatch = ((DeleteBatch) batch);
                    space.clear(new IdsQuery<>(deleteBatch.getType(), deleteBatch.getIds().toArray()), null, Modifiers.BACKUP_ONLY);
                    responseInfo.getDeleted().addAndGet(deleteBatch.getIds().size());
                }
            } catch (InterruptedException e) {
                responseInfo.setException(new IOException("Delete chunks consumer thread was interrupted", e));
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Consumer thread caught exception");
                    e.printStackTrace();
                }
                responseInfo.setException(new IOException("Caught exception while trying to Delete from partition " +
                        responseInfo.getPartitionId(), e));
                return;
            }
        }
    }
}
