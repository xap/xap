package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.SVCPostRecv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.gigaspaces.lrmi.rdma.RdmaConstants.BUFFER_SIZE;

public class RdmaClientReceiver implements Runnable {

    private final BlockingQueue<IbvWC> recvCompletionEventQueue;
    private final ConcurrentHashMap<Long, RdmaMsg> messageMap;
    private Function<ByteBuffer, Object> deserialize;
    private final SVCPostRecv postRecv;
    private final ByteBuffer recvBuf;

    public RdmaClientReceiver(BlockingQueue<IbvWC> recvCompletionEventQueue,
                              ConcurrentHashMap<Long, RdmaMsg> messageMap,
                              GSRdmaClientEndpoint endpoint, Function<ByteBuffer, Object> deserialize) throws IOException {
        this.recvCompletionEventQueue = recvCompletionEventQueue;
        this.messageMap = messageMap;
        this.deserialize = deserialize;
        this.recvBuf = ByteBuffer.allocateDirect(BUFFER_SIZE);
        IbvMr recvMr = endpoint.registerMemory(recvBuf).execute().free().getMr();
        this.postRecv = endpoint.postRecv(ClientTransport.createRecvWorkRequest(RdmaConstants.nextId(), recvMr));

        try {
            postRecv.execute();
            DiSNILogger.getLogger().info("Client ready to receive");
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
                RdmaMsg msg = messageMap.remove(reqId);
                Object res = deserialize.apply(recvBuf);
                msg.setReply(res);
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
}