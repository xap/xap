/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.internal.server.space;

import com.gigaspaces.attribute_store.AttributeStore;
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

    public ZookeeperChunksMapHandler(String serviceName) {
        this.serviceName = serviceName;
        this.attributeStoreKey = toPath(serviceName);
        this.attributeStore = createZooKeeperAttributeStore();
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

    public static String toPath(String spaceName) {
        return "xap/spaces/" + spaceName + "/chunks";
    }

    public void addListener(SpaceConfig spaceConfig) throws ChunksMapMissingException {
        zookeeperClient = createZooKeeperClient(spaceConfig);
        int currentGen = this.getChunksMap().getGeneration();
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

    public PartitionToChunksMap getChunksMap() throws ChunksMapMissingException {
        try {
            String generation = attributeStore.get(attributeStoreKey + "/generation");
            if(generation == null){
                throw new ChunksMapMissingException();
            }
            byte[] bytes = attributeStore.getBytes(attributeStoreKey + "/" + generation);
            return IOUtils.readObject(new ObjectInputStream(new ByteArrayInputStream(bytes)));
        } catch (IOException e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to get chunks map", e);
            throw new DirectPersistencyRecoveryException("Failed to start [" + (serviceName)
                    + "] Failed to create attribute store.");
        } catch (ClassNotFoundException e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to get chunks map", e);
            throw new DirectPersistencyRecoveryException("Failed to start [" + (serviceName)
                    + "] Failed deserialize chunks map");
        }
    }

    public void setChunksMap(PartitionToChunksMap chunksMap) {
        try {
            int generation = chunksMap.getGeneration();
            attributeStore.set(attributeStoreKey + "/" + "generation", String.valueOf(generation));
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            IOUtils.writeObject(objectOutputStream, chunksMap);
            attributeStore.setBytes(attributeStoreKey + "/" + generation, byteArrayOutputStream.toByteArray());
        } catch (IOException e) {
            if (logger.isErrorEnabled())
                logger.error("Failed to get chunks map", e);
            throw new DirectPersistencyRecoveryException("Failed to start [" + (serviceName)
                    + "] Failed to create attribute store.");
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (zookeeperClient != null){
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
            try {
                map = getChunksMap();
            } catch (ChunksMapMissingException e) {
                logger.error("Failed to find chunks map in zk",e);
                return;
            }
            if (map.getGeneration() > this.currentGeneration) {
                logger.warn(spaceConfig.getContainerName() + " is at chunks map generation " + this.currentGeneration + " but current generation in Zookeeper is " + map.getGeneration());
                spaceConfig.getClusterInfo().setChunksMap(map);
                this.currentGeneration = map.getGeneration();
            }
        }
    }
}
