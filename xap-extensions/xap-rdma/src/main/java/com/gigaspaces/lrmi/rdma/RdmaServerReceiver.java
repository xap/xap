package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.util.DiSNILogger;
import org.slf4j.Logger;

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
        Logger logger = DiSNILogger.getLogger();
        while (true) {
            try {
                GSRdmaServerEndpoint endpoint = pendingRequestQueue.take();
                if (!endpoint.isClosed()) {
                    ByteBuffer recvBuff = endpoint.getRecvBuff();
                    recvBuff.clear();
                    long reqId = recvBuff.getLong();
                    if (logger.isDebugEnabled()) {
                        logger.debug("SERVER: reqId = " + reqId);
                    }

                    Object request = deserialize.apply(recvBuff);
                    if (request instanceof Throwable) {
                        throw (Throwable) request;
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("SERVER got request: " + request);
                    }

                    recvBuff.clear();
                    endpoint.executePostRecv();
                    Object reply = process.apply(request);

                    if (reply == null) {
                        logger.info("reply is null, assuming oneway... Not sending result");
                        continue;
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("SERVER going to send response - waiting for resource for reply: " + reply);
                    }
                    RdmaResource resource = endpoint.getResourceManager().waitForFreeResource();
                    if (logger.isDebugEnabled()) {
                        logger.debug("SERVER going to send response - after getting resource");
                    }
                    resource.getBuffer().putLong(reqId);
                    resource.serialize(reply);
                    resource.execute();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("SERVER: server receiver thread was interrupted");
                return;
            } catch (Throwable e) {
                logger.error("SERVER: server got exception", e);
            }
        }
    }
}