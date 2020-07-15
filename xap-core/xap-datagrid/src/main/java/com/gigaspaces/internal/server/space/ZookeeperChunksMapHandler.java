package com.gigaspaces.internal.server.space;

import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.internal.cluster.ChunksRoutingManager;
import com.gigaspaces.internal.cluster.PartitionToChunksMap;
import com.gigaspaces.internal.zookeeper.ZNodePathFactory;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.admin.SpaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ZookeeperChunksMapHandler implements Closeable {

    private final String puZkPath;
    private final AttributeStore attributeStore;
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    public ZookeeperChunksMapHandler(String puName, AttributeStore attributeStore) {
        this.puZkPath = getZkPath(puName);
        this.attributeStore = attributeStore;
    }

    public static String getZkPath(String puName) {
        return ZNodePathFactory.processingUnit(puName, "chunks");
    }

    public void addListener(ZookeeperClient zookeeperClient, SpaceConfig spaceConfig, int partitionId) {
        zookeeperClient.addConnectionStateListener(new ReconnectTask(spaceConfig, partitionId), executorService);
    }

    public ChunksRoutingManager getChunksRoutingManager() throws IOException {
        return attributeStore.getObject(puZkPath);
    }

    @Override
    public void close() throws IOException {
        executorService.shutdownNow();
    }

    public class ReconnectTask implements Runnable {

        private final SpaceConfig spaceConfig;
        private final int partitionId;
        private final Logger logger = LoggerFactory.getLogger(Constants.LOGGER_ZOOKEEPER);

        ReconnectTask(SpaceConfig spaceConfig, int partitionId) {
            this.spaceConfig = spaceConfig;
            this.partitionId = partitionId;
        }

        @Override
        public void run() {
            try {
                PartitionToChunksMap map = getChunksRoutingManager().getMapForPartition(partitionId);
                int currentGeneration = spaceConfig.getClusterInfo().getChunksMap().getGeneration();
                if (map.getGeneration() > currentGeneration) {
                    logger.warn(spaceConfig.getContainerName() + " is at chunks map generation " + currentGeneration + " but current generation in Zookeeper is " + map.getGeneration());
                    spaceConfig.getClusterInfo().setChunksMap(map);
                }
            } catch (IOException e) {
                logger.warn("Failed to get chunks routing mapping", e);
                throw new UncheckedIOException("Failed to get chunks routing mapping", e);
            }
        }
    }
}
