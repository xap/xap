package com.gigaspaces.internal.server.space;

import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.internal.cluster.ChunksRoutingManager;
import com.gigaspaces.internal.cluster.PartitionToChunksMap;
import com.gigaspaces.internal.exceptions.ChunksMapMissingException;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.DirectPersistencyRecoveryException;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.admin.SpaceConfig;
import com.j_spaces.kernel.ClassLoaderHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.j_spaces.core.Constants.DirectPersistency.ZOOKEEPER.ATTRIBUET_STORE_HANDLER_CLASS_NAME;
import static com.j_spaces.core.Constants.DirectPersistency.ZOOKEEPER.ZOKEEPER_CLIENT_CLASS_NAME;

public class ZookeeperChunksMapHandler implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Constants.LOGGER_ZOOKEEPER);
    private final String serviceName;
    private final String attributeStoreKey;
    private final AttributeStore attributeStore;
    private final ExecutorService singleThreadExecutorService = Executors.newFixedThreadPool(1);
    private ZookeeperClient zookeeperClient;

    public ZookeeperChunksMapHandler(String serviceName, AttributeStore attributeStore) {
        this.serviceName = serviceName;
        this.attributeStoreKey = toPath(serviceName);
        this.attributeStore = attributeStore;
    }

    public static String toPath(String spaceName) {
        return "xap/spaces/" + spaceName + "/chunks";
    }

    private AttributeStore createZooKeeperAttributeStore() {
        try {
            //noinspection unchecked
            Constructor constructor = ClassLoaderHelper.loadLocalClass(ATTRIBUET_STORE_HANDLER_CLASS_NAME)
                    .getConstructor(String.class);
            return (AttributeStore) constructor.newInstance("");

        } catch (Exception e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to create attribute store ");
            throw new DirectPersistencyRecoveryException("Failed to start [" + serviceName
                    + "] Failed to create attribute store.");
        }
    }

    private ZookeeperClient createZooKeeperClient(SpaceConfig spaceConfig) {

        try {
            //noinspection unchecked
            Constructor constructor = ClassLoaderHelper.loadLocalClass(ZOKEEPER_CLIENT_CLASS_NAME)
                    .getConstructor(SpaceConfig.class);
            return (ZookeeperClient) constructor.newInstance(spaceConfig);

        } catch (Exception e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to create zookeeper client");
            throw new DirectPersistencyRecoveryException("Failed to start [" + (serviceName)
                    + "] Failed to create zookeeper client.");
        }
    }

    private ChunksRoutingManager toManager(byte[] bytes) throws IOException, ClassNotFoundException {
        return IOUtils.readObject(new ObjectInputStream(new ByteArrayInputStream(bytes)));
    }

    private byte[] toByteArray(ChunksRoutingManager chunksRoutingManager) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        IOUtils.writeObject(objectOutputStream, chunksRoutingManager);
        return byteArrayOutputStream.toByteArray();
    }

    public void addListener(SpaceConfig spaceConfig) {
        zookeeperClient = createZooKeeperClient(spaceConfig);
        int currentGen = this.getChunksMap().getGeneration();
        zookeeperClient.addConnectionStateListener(new ReconnectTask(currentGen, spaceConfig), singleThreadExecutorService);
    }

    PartitionToChunksMap initChunksMap(int numberOfPartitions) throws ChunksMapMissingException {
        try {
            PartitionToChunksMap chunksMap = new PartitionToChunksMap(numberOfPartitions, 0);
            chunksMap.init();
            ChunksRoutingManager chunksRoutingManager = new ChunksRoutingManager(chunksMap);
            if (attributeStore.initFirst(attributeStoreKey, toByteArray(chunksRoutingManager))) {
                logger.info("init op succeeded");
                return chunksMap;
            } else {
                logger.info("init op failed - getting map");
                return getChunksMap();
            }
        } catch (Exception e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to init chunks map", e);
            throw new ChunksMapMissingException(e);
        }
    }

    public PartitionToChunksMap getChunksMap() {
        try {
            ChunksRoutingManager chunksRoutingManager = toManager(attributeStore.getBytes(attributeStoreKey));
            return chunksRoutingManager.getLastestMap();
        } catch (IOException e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to get chunks manager", e);
            throw new DirectPersistencyRecoveryException("Failed to start [" + (serviceName)
                    + "] Failed to create attribute store.");
        } catch (ClassNotFoundException e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to get chunks manager", e);
            throw new DirectPersistencyRecoveryException("Failed to start [" + (serviceName)
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
            throw new DirectPersistencyRecoveryException("Failed to start [" + (serviceName)
                    + "] Failed to create attribute store.");
        } catch (ClassNotFoundException e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to get chunks manager", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (zookeeperClient != null) {
                singleThreadExecutorService.shutdownNow();
                zookeeperClient.close();
            }
        } catch (Exception e) {
            logger.warn("Failed to close ZookeeperClient", e);
        }
    }

    public class ReconnectTask implements Runnable {

        private int currentGeneration;
        private SpaceConfig spaceConfig;
        private Logger logger = LoggerFactory.getLogger(Constants.LOGGER_ZOOKEEPER);

        ReconnectTask(int currentGeneration, SpaceConfig spaceConfig) {
            this.currentGeneration = currentGeneration;
            this.spaceConfig = spaceConfig;
        }

        @Override
        public void run() {
            PartitionToChunksMap map;
            map = getChunksMap();
            if (map.getGeneration() > this.currentGeneration) {
                logger.warn(spaceConfig.getContainerName() + " is at chunks map generation " + this.currentGeneration + " but current generation in Zookeeper is " + map.getGeneration());
                spaceConfig.getClusterInfo().setChunksMap(map);
                this.currentGeneration = map.getGeneration();
            }
        }
    }
}
