package com.gigaspaces.internal.cluster.node.impl.replica;

import com.gigaspaces.internal.cluster.node.impl.ReplicationNode;
import com.gigaspaces.logger.Constants;

import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author yael nahon
 *
 * During space recovery this handler keeps all batched from primary that containd fifo types and serve the batches for
 * processing in order (by fifoId) to ensure original fifo order is saved
 *
 */
public class SpaceReplicaFifoBatchesHandler {

    private final ReplicationNode replicationNode;
    private final Logger logger = Logger.getLogger(Constants.LOGGER_REPLICATION_REPLICA);
    private final TreeMap<Integer, SpaceReplicaBatch> pendingBatches = new TreeMap<Integer, SpaceReplicaBatch>();
    private int lastProcessed = 0;
    private final Object lock = new Object();

    public SpaceReplicaFifoBatchesHandler(ReplicationNode node) {
        this.replicationNode = node;
    }

    /**
     * Check if new fifo batch from primary is the next to be processed, if yes it is processed,
     * otherwise it is inserted to pendingBatches
     * @param batch - new fifo batch from primary
     * @param consumer - SpaceCopyReplicaRunnable thread which responsible of processing incoming recovery batches
     *
     */
    void handleIncomingBatch(SpaceReplicaBatch batch, SpaceCopyReplicaRunnable consumer) {
        synchronized (getLock()) {
            addBatch(batch);
            if (logger.isLoggable(Level.FINE)) {
                logger.fine(replicationNode.getLogPrefix() + "inserting fifo batch " + batch.getFifoId() + " to queue, fifo state: " + this);
            }
        }//synchronized
        processNextIfPossible(consumer);
    }

    private SpaceReplicaBatch getNextBatchForProcessing() {
        return pendingBatches.remove(pendingBatches.firstKey());
    }

    private boolean hasNextBatch() {

        return !pendingBatches.isEmpty() && pendingBatches.firstKey() == lastProcessed + 1;

    }

    private Object getLock() {
        return lock;
    }

    private void addBatch(SpaceReplicaBatch batch) {
        pendingBatches.put(batch.getFifoId(), batch);
    }

    private void onProcessBatchCompletion(int id) {
        synchronized (getLock()) {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine(replicationNode.getLogPrefix() + "finished processing fifo batch " + id + ", fifo state: " + this);
            }

            if (id != (lastProcessed + 1)) {
                throw new IllegalStateException("completed processing batch " + id + " but was expecting " + (lastProcessed + 1));
            }

            lastProcessed = id;
        }
    }

    private void processNextIfPossible(SpaceCopyReplicaRunnable consumer) {
        while (true) {
            boolean process = false;
            SpaceReplicaBatch currentBatch = null;
            synchronized (getLock()) {
                if (hasNextBatch()) {
                    currentBatch = getNextBatchForProcessing();
                    process = true;
                }
            }//synchronized
            if (process) {
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine(replicationNode.getLogPrefix() + "processing fifo batch " + currentBatch.getFifoId() + ", fifo state: " + this);
                }
                consumer.processBatch(currentBatch.getBatch());
                onProcessBatchCompletion(currentBatch.getFifoId());
            } else {
                return;
            }
        }
    }

    @Override
    public String toString() {
        return "SpaceReplicaFifoBatchesHandler{" +
                "pendingBatches=" + pendingBatches.keySet() +
                ", lastProcessed=" + lastProcessed +
                '}';
    }
}
