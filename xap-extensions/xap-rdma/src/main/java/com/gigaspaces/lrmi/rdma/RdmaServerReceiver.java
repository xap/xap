package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.util.DiSNILogger;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

public class RdmaServerReceiver implements Runnable {

    private final BlockingQueue<GSRdmaServerEndpoint> pendingRequestQueue;
    private Function<RdmaMsg, RdmaMsg> process;
    private Function<ByteBuffer, Object> deserialize;

    public RdmaServerReceiver(BlockingQueue<GSRdmaServerEndpoint> pendinfRequestQueue, Function<RdmaMsg, RdmaMsg> process, Function<ByteBuffer, Object> deserialize) {
        this.pendingRequestQueue = pendinfRequestQueue;
        this.process = process;
        this.deserialize = deserialize;
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

                    Object result = deserialize.apply(recvBuff);
                    if (result instanceof Throwable) {
                        throw (Throwable) result;
                    }



                    RdmaMsg rdmaMsg = new RdmaMsg(result);

                    DiSNILogger.getLogger().info("SERVER got msg: " + rdmaMsg.getPayload());
                    endpoint.getPostRecv().execute();
                    RdmaMsg response = process.apply(rdmaMsg);
                    DiSNILogger.getLogger().info("SERVER going to send response - waiting for resource for response: " + response.getPayload());
                    RdmaResource resource = endpoint.getResourceManager().waitForFreeResource();
                    DiSNILogger.getLogger().info("SERVER going to send response - after getting resource");
                    resource.serialize(reqId, response);
                    resource.getPostSend().execute();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                DiSNILogger.getLogger().error("SERVER: server receiver thread was interrupted");
                return;
            } catch (Throwable e) {
                DiSNILogger.getLogger().error("SERVER: server got exception",e);
                e.printStackTrace();
            }
        }
    }
}