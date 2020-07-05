package com.gigaspaces.internal.server.space;

import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.attribute_store.SharedLock;
import com.gigaspaces.internal.cluster.ChunksRoutingManager;
import com.gigaspaces.internal.cluster.PartitionToChunksMap;
import com.gigaspaces.internal.exceptions.ChunksMapMissingException;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.DirectPersistencyRecoveryException;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.admin.SpaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ZookeeperChunksMapHandler implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Constants.LOGGER_ZOOKEEPER);
    private final String puName;
    private final String attributeStoreKey;
    private final AttributeStore attributeStore;
    private final ExecutorService singleThreadExecutorService = Executors.newFixedThreadPool(1);
    private int partitionId;


    public ZookeeperChunksMapHandler(String puName, AttributeStore attributeStore) {
        this.puName = puName;
        this.attributeStoreKey = toPath(puName);
        this.attributeStore = attributeStore;
    }

    public static String toPath(String puName) {
        return "xap/pus/" + puName + "/chunks";
    }

    private ChunksRoutingManager toManager(byte[] bytes) throws IOException, ClassNotFoundException {
        if (bytes != null && bytes.length != 0) {
            return IOUtils.readObject(new ObjectInputStream(new ByteArrayInputStream(bytes)));
        }
        return null;
    }

    private byte[] toByteArray(ChunksRoutingManager chunksRoutingManager) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        IOUtils.writeObject(objectOutputStream, chunksRoutingManager);
        return byteArrayOutputStream.toByteArray();
    }

    public void addListener(ZookeeperClient zookeeperClient, SpaceConfig spaceConfig) {
        zookeeperClient.addConnectionStateListener(new ReconnectTask(spaceConfig), singleThreadExecutorService);
    }

    PartitionToChunksMap initChunksMap(int numberOfPartitions, int partitionId) throws ChunksMapMissingException {
        try {
            ChunksRoutingManager routingManager = getChunksRoutingManager();
            this.partitionId = partitionId;
            if (routingManager == null || isWrongPartitionCount(numberOfPartitions, routingManager, this.partitionId)) {
                SharedLock lock = attributeStore.getSharedLock(com.j_spaces.core.Constants.Space.spaceLockPath(puName));
                if (lock.acquire(30, TimeUnit.SECONDS)) {
                    try {
                        ChunksRoutingManager manager = getChunksRoutingManager();
                        if (manager == null || isWrongPartitionCount(numberOfPartitions, manager, partitionId)) {
                            PartitionToChunksMap chunksMap = new PartitionToChunksMap(numberOfPartitions, 0);
                            chunksMap.init();
                            ChunksRoutingManager chunksRoutingManager = new ChunksRoutingManager(chunksMap);
                            logger.info("Creating map");
                            setRoutingManager(chunksRoutingManager);
                            return chunksMap;
                        } else {
                            logger.info("Map already exist");
                            return manager.getMapForPartition(partitionId);
                        }
                    } finally {
                        lock.release();
                    }
                } else {
                    throw new ChunksMapMissingException("failed to acquire space lock in 30 seconds");
                }
            } else {
                logger.info("Map already exist");
                return routingManager.getMapForPartition(partitionId);
            }
        } catch (Exception e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to init chunks map", e);
            throw new ChunksMapMissingException(e);
        }
    }

    private boolean isWrongPartitionCount(int numberOfPartitions, ChunksRoutingManager routingManager, int partitionId) {
        PartitionToChunksMap partitionMap = routingManager.getMapForPartition(partitionId);
        return partitionMap == null || partitionMap.getNumOfPartitions() != numberOfPartitions;
    }

    PartitionToChunksMap getChunksMap(int partitionId) {
        try {
            ChunksRoutingManager chunksRoutingManager = toManager(attributeStore.getBytes(attributeStoreKey));
            return chunksRoutingManager.getMapForPartition(partitionId);
        } catch (IOException e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to get chunks manager", e);
            throw new DirectPersistencyRecoveryException("Failed to start [" + (puName)
                    + "] Failed to create attribute store.");
        } catch (ClassNotFoundException e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to get chunks manager", e);
            throw new DirectPersistencyRecoveryException("Failed to start [" + (puName)
                    + "] Failed deserialize chunks manager");
        }
    }

    public void setChunksMap(PartitionToChunksMap chunksMap) {
        try {
            ChunksRoutingManager manager = toManager(attributeStore.getBytes(attributeStoreKey));
            manager.addNewMap(chunksMap);
            attributeStore.setBytes(attributeStoreKey, toByteArray(manager));
        } catch (IOException e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to get chunks manager", e);
            throw new DirectPersistencyRecoveryException("Failed to start [" + (puName)
                    + "] Failed to create attribute store.");
        } catch (ClassNotFoundException e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to get chunks manager", e);
        }
    }

    public ChunksRoutingManager getChunksRoutingManager() {
        try {
            return toManager(attributeStore.getBytes(attributeStoreKey));
        } catch (IOException e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to get chunks manager", e);
            throw new DirectPersistencyRecoveryException("Failed to start [" + (puName)
                    + "] Failed to create attribute store.");
        } catch (ClassNotFoundException e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to get chunks manager", e);
            throw new DirectPersistencyRecoveryException("Failed to start [" + (puName)
                    + "] Failed deserialize chunks manager");
        }
    }

    public void setRoutingManager(ChunksRoutingManager newManager) {
        try {
            attributeStore.setBytes(attributeStoreKey, toByteArray(newManager));
        } catch (IOException e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to get chunks manager", e);
            throw new DirectPersistencyRecoveryException("Failed to start [" + (puName)
                    + "] Failed to create attribute store.");
        }
    }

    @Override
    public void close() throws IOException {
        singleThreadExecutorService.shutdownNow();
    }

    void removePath() {
        try {
            attributeStore.remove(attributeStoreKey);
        } catch (Exception e) {
            logger.warn("Failed to delete " + attributeStore, e);
        }
    }

    public class ReconnectTask implements Runnable {

        private SpaceConfig spaceConfig;
        private Logger logger = LoggerFactory.getLogger(Constants.LOGGER_ZOOKEEPER);

        ReconnectTask(SpaceConfig spaceConfig) {
            this.spaceConfig = spaceConfig;
        }

        @Override
        public void run() {
            PartitionToChunksMap map;
            map = getChunksMap(partitionId);
            int currentGeneration = spaceConfig.getClusterInfo().getChunksMap().getGeneration();
            if (map.getGeneration() > currentGeneration) {
                logger.warn(spaceConfig.getContainerName() + " is at chunks map generation " + currentGeneration + " but current generation in Zookeeper is " + map.getGeneration());
                spaceConfig.getClusterInfo().setChunksMap(map);
            }
        }
    }
}
