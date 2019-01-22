package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.util.DiSNILogger;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

public class RdmaServerReceiver implements Runnable {

    private final BlockingQueue<GSRdmaServerEndpoint> pendingRequestQueue;
    private Function<RdmaMsg, RdmaMsg> process;

    public RdmaServerReceiver(BlockingQueue<GSRdmaServerEndpoint> pendinfRequestQueue, Function<RdmaMsg, RdmaMsg> process) {
        this.pendingRequestQueue = pendinfRequestQueue;
        this.process = process;
    }

    @Override
    public void run() {
        while (true) {
            try {
                GSRdmaServerEndpoint endpoint = pendingRequestQueue.take();
                ByteBuffer recvBuff = endpoint.getRecvBuff();
                recvBuff.clear();
                long reqId = recvBuff.getLong();
                DiSNILogger.getLogger().info("SERVER: reqId = " + reqId);
                RdmaMsg rdmaMsg = ClientTransport.readResponse(recvBuff);
                endpoint.getPostRecv().execute();
                RdmaMsg response = process.apply(rdmaMsg);
                RdmaResource resource = endpoint.getResourceManager().waitForFreeResource();
                ClientTransport.serializeToBuffer(resource.getBuffer(), response, reqId);
                resource.getPostSend().execute();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}