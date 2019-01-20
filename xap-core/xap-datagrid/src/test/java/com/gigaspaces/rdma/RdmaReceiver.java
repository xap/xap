package com.gigaspaces.rdma;

import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.SVCPostRecv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class RdmaReceiver implements Runnable {

    private final Client.CustomClientEndpoint endpoint;
    private final BlockingQueue<IbvWC> recvCompletionEventQueue;
    private final ConcurrentHashMap<Long, CompletableFuture<RdmaMsg>> futureMap;
    private final SVCPostRecv postRecv;

    public RdmaReceiver(BlockingQueue<IbvWC> recvCompletionEventQueue,
                        ConcurrentHashMap<Long, CompletableFuture<RdmaMsg>> futureMap,
                        Client.CustomClientEndpoint endpoint) throws IOException {
        this.recvCompletionEventQueue = recvCompletionEventQueue;
        this.futureMap = futureMap;
        this.endpoint = endpoint;
        this.postRecv = endpoint.postRecv(endpoint.getWrList_recv());
    }

    @Override
    public void run() {
        while (true) {
            try {
                IbvWC event = recvCompletionEventQueue.take();
                ByteBuffer buff = findBuffer(event.getWr_id());
                long reqId = buff.getLong();
                CompletableFuture future = getFuture(reqId);
                try {
                    future.complete(ClientTransport.readResponse(buff));
                } catch (Exception e) {
                    future.completeExceptionally(e);
                }
                try {
                    this.postRecv.execute();
                } catch (IOException e) {
                    e.printStackTrace(); //TODO
                }
            } catch (InterruptedException e) {
                e.printStackTrace();//TODO
            }
        }
    }

    private CompletableFuture getFuture(long reqId) {
        return futureMap.remove(reqId);
    }

    private ByteBuffer findBuffer(long wr_id) {
        return endpoint.getRecvBuf();
    }
}