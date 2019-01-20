package com.gigaspaces.rdma;

import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.SVCPostRecv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static com.gigaspaces.rdma.RdmaConstants.BUFFER_SIZE;

public class RdmaReceiver implements Runnable {

    private final Client.CustomClientEndpoint endpoint;
    private final BlockingQueue<IbvWC> recvCompletionEventQueue;
    private final ConcurrentHashMap<Long, CompletableFuture<RdmaMsg>> futureMap;
    private final SVCPostRecv postRecv;
    private final ByteBuffer recvBuf;

    public RdmaReceiver(BlockingQueue<IbvWC> recvCompletionEventQueue,
                        ConcurrentHashMap<Long, CompletableFuture<RdmaMsg>> futureMap,
                        Client.CustomClientEndpoint endpoint) throws IOException {
        this.recvCompletionEventQueue = recvCompletionEventQueue;
        this.futureMap = futureMap;
        this.endpoint = endpoint;

        this.recvBuf = ByteBuffer.allocateDirect(BUFFER_SIZE);
        IbvMr recvMr = endpoint.registerMemory(recvBuf).execute().free().getMr();
        this.postRecv = endpoint.postRecv(ClientTransport.createRecvWorkRequest(2004, recvMr));

        try {
            postRecv.execute();
        } catch (IOException e) {
            e.printStackTrace();//TODO
        }
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
        return recvBuf;
    }
}