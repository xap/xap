package com.gigaspaces.internal.server.space;

import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.internal.cluster.PartitionToGrainsMap;
import com.gigaspaces.internal.exceptions.GrainsMapMissingException;
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

public class ZookeeperGrainsMapHandler implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Constants.LOGGER_ZOOKEEPER);
    private final String serviceName;
    private final String attributeStoreKey;
    private final AttributeStore attributeStore;
    private final ExecutorService singleThreadExecutorService = Executors.newFixedThreadPool(1);
    private ZookeeperClient zookeeperClient;

    public ZookeeperGrainsMapHandler(String serviceName, SpaceConfig spaceConfig) {
        this.serviceName = serviceName;
        this.attributeStoreKey = toPath(serviceName);
        this.attributeStore = createZooKeeperAttributeStore(spaceConfig);
    }


    public static String toPath(String spaceName) {
        return "xap/spaces/" + spaceName + "/chunks";
    }


    private AttributeStore createZooKeeperAttributeStore(SpaceConfig spaceConfig) {

        try {
            if (spaceConfig == null) {
                //noinspection unchecked
                Constructor constructor = ClassLoaderHelper.loadLocalClass(ATTRIBUET_STORE_HANDLER_CLASS_NAME)
                        .getConstructor(String.class);
                return (AttributeStore) constructor.newInstance("");

            } else {
                //noinspection unchecked
                Constructor constructor = ClassLoaderHelper.loadLocalClass(ATTRIBUET_STORE_HANDLER_CLASS_NAME)
                        .getConstructor(String.class, SpaceConfig.class);
                return (AttributeStore) constructor.newInstance("", spaceConfig);
            }
        } catch (Exception e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to create attribute store ");
            throw new DirectPersistencyRecoveryException("Failed to start [" + (serviceName)
                    + "] Failed to create attribute store.");
        }
    }

    public void addListener(SpaceConfig spaceConfig) throws GrainsMapMissingException {
        zookeeperClient = createZooKeeperClient(spaceConfig);
        int currentGen = this.getGrainsMap().getGeneration();
        zookeeperClient.addConnectionStateListener(new ReconnectTask(currentGen, spaceConfig), singleThreadExecutorService);
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

    public PartitionToGrainsMap getGrainsMap() throws GrainsMapMissingException {
        try {
            String generation = attributeStore.get(attributeStoreKey + "/generation");
            if(generation == null){
                throw new GrainsMapMissingException();
            }
            byte[] bytes = attributeStore.getBytes(attributeStoreKey + "/" + generation);
            return IOUtils.readObject(new ObjectInputStream(new ByteArrayInputStream(bytes)));
        } catch (IOException e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to get grains map", e);
            throw new DirectPersistencyRecoveryException("Failed to start [" + (serviceName)
                    + "] Failed to create attribute store.");
        } catch (ClassNotFoundException e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to get grains map", e);
            throw new DirectPersistencyRecoveryException("Failed to start [" + (serviceName)
                    + "] Failed deserialize grains map");
        }
    }

    public void setGrainsMap(PartitionToGrainsMap grainsMap) {
        try {
            int generation = grainsMap.getGeneration();
            attributeStore.set(attributeStoreKey + "/" + "generation", String.valueOf(generation));
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            IOUtils.writeObject(objectOutputStream, grainsMap);
            attributeStore.setBytes(attributeStoreKey + "/" + generation, byteArrayOutputStream.toByteArray());
        } catch (IOException e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to get grains map", e);
            throw new DirectPersistencyRecoveryException("Failed to start [" + (serviceName)
                    + "] Failed to create attribute store.");
        }
    }

    @Override
    public void close() throws IOException {
        try {
            attributeStore.close();
        } catch (Exception e) {
            logger.warn("Failed to close ZooKeeperAttributeStore", e);
        }
        try {
            zookeeperClient.close();
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
            PartitionToGrainsMap map;
            try {
                map = getGrainsMap();
            } catch (GrainsMapMissingException e) {
                logger.error("Failed to find grains map in zk",e);
                return;
            }
            if (map.getGeneration() > this.currentGeneration) {
                logger.warn(spaceConfig.getContainerName() + " is at grains map generation " + this.currentGeneration + " but current generation in Zookeeper is " + map.getGeneration());
                spaceConfig.getClusterInfo().setGrainsMap(map);
                this.currentGeneration = map.getGeneration();
            }
        }
    }
}
