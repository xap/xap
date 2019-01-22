package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.RdmaActiveEndpoint;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class RdmaResourceManager {

    private final Map<Short,RdmaResource> usedResources = new ConcurrentHashMap<>();
    private final ArrayBlockingQueue<RdmaResource> freeResources;

    public RdmaResourceManager(RdmaActiveEndpoint endpoint, int resourcesCount, int bufferSize) throws IOException {
        freeResources = new ArrayBlockingQueue<>(resourcesCount);
        for (short i = 0; i < resourcesCount; i++) {
            ByteBuffer direct = ByteBuffer.allocateDirect(bufferSize);
            RdmaResource resource = new RdmaResource(i, direct, ClientTransport.rdmaSendBuffer(i, direct, endpoint));
            freeResources.add(resource);
        }
    }

    //TODO synchronization
    public RdmaResource waitForFreeResource() throws InterruptedException {
        RdmaResource rdmaResource = freeResources.take();
        usedResources.put(rdmaResource.getId(), rdmaResource);
        return rdmaResource;
    }

    public void releaseResource(short id){
        RdmaResource rdmaResource = usedResources.remove(id);
        if(rdmaResource != null){
            rdmaResource.getBuffer().clear();
            freeResources.add(rdmaResource);
        }
    }
}
