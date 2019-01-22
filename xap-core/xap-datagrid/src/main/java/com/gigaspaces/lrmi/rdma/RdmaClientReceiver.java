package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.SVCPostRecv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static com.gigaspaces.lrmi.rdma.RdmaConstants.BUFFER_SIZE;

public class RdmaClientReceiver implements Runnable {

    private final BlockingQueue<IbvWC> recvCompletionEventQueue;
    private final ConcurrentHashMap<Long, CompletableFuture<RdmaMsg>> futureMap;
    private final SVCPostRecv postRecv;
    private final ByteBuffer recvBuf;

    public RdmaClientReceiver(BlockingQueue<IbvWC> recvCompletionEventQueue,
                              ConcurrentHashMap<Long, CompletableFuture<RdmaMsg>> futureMap,
                              GSRdmaClientEndpoint endpoint) throws IOException {
        this.recvCompletionEventQueue = recvCompletionEventQueue;
        this.futureMap = futureMap;

        this.recvBuf = ByteBuffer.allocateDirect(BUFFER_SIZE);
        IbvMr recvMr = endpoint.registerMemory(recvBuf).execute().free().getMr();
        this.postRecv = endpoint.postRecv(ClientTransport.createRecvWorkRequest(RdmaConstants.nextId(), recvMr));

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
                long reqId = recvBuf.getLong();
                CompletableFuture future = getFuture(reqId);
                try {
                    future.complete(ClientTransport.readResponse(recvBuf));
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

}