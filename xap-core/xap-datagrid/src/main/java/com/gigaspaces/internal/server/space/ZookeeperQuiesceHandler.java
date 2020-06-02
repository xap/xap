package com.gigaspaces.internal.server.space;

import com.gigaspaces.admin.quiesce.QuiesceState;
import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.attribute_store.SharedLock;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.quiesce.InternalQuiesceDetails;
import com.gigaspaces.internal.server.space.quiesce.QuiesceHandler;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.server.space.suspend.SuspendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.j_spaces.core.Constants.Quiesce.quiescePath;

public class ZookeeperQuiesceHandler implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Constants.LOGGER_ZOOKEEPER);
    private final String puName;
    private final AttributeStore attributeStore;
    private final ExecutorService singleThreadExecutorService = Executors.newFixedThreadPool(1);

    ZookeeperQuiesceHandler(String puName, AttributeStore attributeStore) {
        this.puName = puName;
        this.attributeStore = attributeStore;
    }

    public void addListener(ZookeeperClient zookeeperClient, QuiesceHandler quiesceHandler) {
        zookeeperClient.addConnectionStateListener(new QuiesceReconnectTask(quiesceHandler), singleThreadExecutorService);
    }

    @Override
    public void close() throws IOException {
        singleThreadExecutorService.shutdownNow();
    }

    InternalQuiesceDetails getUpdatedQuiesceDetails() {
        try {
            byte[] bytes = attributeStore.getBytes(com.j_spaces.core.Constants.Quiesce.quiescePath(puName));
            if (bytes != null && bytes.length != 0) {
                return IOUtils.readObject(new ObjectInputStream(new ByteArrayInputStream(bytes)));
            }
        } catch (IOException | ClassNotFoundException e) {
            logger.warn("Could not get quiesce details from zookeeper");
        }
        return null;
    }

    private void updateDetailsInZK(InternalQuiesceDetails details) {
        //try acquiring lock again to update instancesState in zk
        InternalQuiesceDetails current = getUpdatedQuiesceDetails();
        if(current == null){
            throw new IllegalStateException("try to update instance details but details from ZK are null");
        }
        SharedLock lock = attributeStore.getSharedLock(com.j_spaces.core.Constants.Space.spaceLockPath(puName));
        try {
            if (lock.acquire(30, TimeUnit.SECONDS) && current.getStatus().equals(details.getStatus()) && current.getDescription().equals(details.getDescription())) {
                attributeStore.setBytes(quiescePath(puName), toByteArray(details));
                lock.release();
            } else {
                logger.warn("Could not update instances State");
            }
        } catch (Exception e) {
            logger.warn("Could not update instances State, exception = "+e);
        }
    }


    private byte[] toByteArray(InternalQuiesceDetails quiesceDetails) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        IOUtils.writeObject(objectOutputStream, quiesceDetails);
        return byteArrayOutputStream.toByteArray();
    }

    void updateInstanceState(InternalQuiesceDetails details) {
        if(details != null && details.getInstancesState().getFailedToQuiesceInstances()!= null && details.getInstancesState().getFailedToQuiesceInstances().containsKey(puName)){
            details.getInstancesState().getFailedToQuiesceInstances().remove(puName);
            details.getInstancesState().getQuiescedInstances().add(puName);
        } else if(details != null){
            details.getInstancesState().getQuiescedInstances().add(puName);
        }
        updateDetailsInZK(details);
    }

    private class QuiesceReconnectTask implements Runnable {

        private final QuiesceHandler quiesceHandler;

        QuiesceReconnectTask(QuiesceHandler quiesceHandler) {
            this.quiesceHandler = quiesceHandler;
        }

        @Override
        public void run() {
            logger.info("Detected reconnect event - will update quiesce state as needed");
            InternalQuiesceDetails details = getUpdatedQuiesceDetails();
            if(details != null){
                if (details.getStatus().equals(QuiesceState.QUIESCED) && quiesceHandler.getSuspendInfo().getSuspendType().equals(SuspendType.NONE)) {
                    logger.info("Current quiesced details = "+details);
                    quiesceHandler.quiesce(details.getDescription(), details.getToken());
                    updateInstanceState(details);
                } else if(quiesceHandler.getSuspendInfo().getSuspendType().equals(SuspendType.QUIESCED)){
                    quiesceHandler.unquiesce();
                    updateInstanceState(details);
                }
            }
        }
    }
}