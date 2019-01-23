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
                if(!endpoint.isClosed()) {
                    ByteBuffer recvBuff = endpoint.getRecvBuff();
                    recvBuff.clear();
                    long reqId = recvBuff.getLong();
                    DiSNILogger.getLogger().info("SERVER: reqId = " + reqId);
                    RdmaMsg rdmaMsg = ClientTransport.readResponse(recvBuff);
                    DiSNILogger.getLogger().info("SERVER got msg: " + rdmaMsg.getPayload());
                    endpoint.getPostRecv().execute();
                    RdmaMsg response = process.apply(rdmaMsg);
                    DiSNILogger.getLogger().info("SERVER going to send response - waiting for resource for response: " + response.getPayload());
                    RdmaResource resource = endpoint.getResourceManager().waitForFreeResource();
                    DiSNILogger.getLogger().info("SERVER going to send response - after getting resource");
                    ClientTransport.serializeToBuffer(resource.getBuffer(), response, reqId);
                    resource.getPostSend().execute();

//                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                DiSNILogger.getLogger().error("SERVER: server receiver thread was interrupted");
                return;
            } catch (Exception e) {
                DiSNILogger.getLogger().error("SERVER: server got exception",e);
                e.printStackTrace();
            }
        }
    }
}