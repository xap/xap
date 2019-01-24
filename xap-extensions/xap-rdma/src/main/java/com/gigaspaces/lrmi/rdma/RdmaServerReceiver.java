package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.util.DiSNILogger;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

public class RdmaServerReceiver implements Runnable {

    private final BlockingQueue<GSRdmaServerEndpoint> pendingRequestQueue;
    private Function<Object, Object> process;
    private Function<ByteBuffer, Object> deserialize;

    public RdmaServerReceiver(BlockingQueue<GSRdmaServerEndpoint> pendinfRequestQueue, Function<Object, Object> process, Function<ByteBuffer, Object> deserialize) {
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

                    Object request = deserialize.apply(recvBuff);
                    if (request instanceof Throwable) {
                        throw (Throwable) request;
                    }

                    DiSNILogger.getLogger().info("SERVER got request: " + request);

                    recvBuff.clear();
                    endpoint.getPostRecv().execute();
                    Object reply = process.apply(request);

                    if (reply == null) {
                        DiSNILogger.getLogger().info("reply is null, assuming oneway... Not sending result");
                        continue;
                    }

                    DiSNILogger.getLogger().info("SERVER going to send response - waiting for resource for reply: " + reply);
                    RdmaResource resource = endpoint.getResourceManager().waitForFreeResource();
                    DiSNILogger.getLogger().info("SERVER going to send response - after getting resource");
                    resource.getBuffer().putLong(reqId);
                    resource.serialize(reply);
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