package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvRecvWR;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.SVCPostRecv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class RdmaClientReceiver implements Runnable {

    private final BlockingQueue<IbvWC> recvCompletionEventQueue;
    private final ConcurrentHashMap<Long, RdmaMsg> messageMap;
    private final LinkedList<IbvRecvWR> recvList;
    private final GSRdmaClientEndpoint endpoint;
    private Function<ByteBuffer, Object> deserialize;
    private final SVCPostRecv postRecv;
    private final ByteBuffer recvBuf;

    public RdmaClientReceiver(BlockingQueue<IbvWC> recvCompletionEventQueue,
                              ConcurrentHashMap<Long, RdmaMsg> messageMap,
                              GSRdmaClientEndpoint endpoint, Function<ByteBuffer, Object> deserialize) throws IOException {
        this.endpoint = endpoint;
        this.recvCompletionEventQueue = recvCompletionEventQueue;
        this.messageMap = messageMap;
        this.deserialize = deserialize;
        this.recvBuf = ByteBuffer.allocateDirect(RdmaConstants.bufferSize());
        IbvMr recvMr = endpoint.registerMemory(recvBuf).execute().free().getMr();
        this.recvList = ClientTransport.createRecvWorkRequest(RdmaConstants.RDMA_CLIENT_RECV_ID, recvMr);
        this.postRecv = RdmaConstants.JNI_CACHE_ENABLED ? endpoint.postRecv(recvList) : null;

        try {
            if (RdmaConstants.JNI_CACHE_ENABLED)
                postRecv.execute();
            else {
                endpoint.postRecv(recvList).execute().free();
            }
            DiSNILogger.getLogger().info("Client ready to receive");
        } catch (IOException e) {
            e.printStackTrace();//TODO
        }
    }

    @Override
    public void run() {
        System.out.println(">> started client receiver");

        while (true) {
            try {
                IbvWC event = recvCompletionEventQueue.take();
                recvBuf.clear();
                long reqId = recvBuf.getLong();
                RdmaMsg msg = messageMap.remove(reqId);
                if(msg == null){
                    System.out.println("Msg is missing "+reqId);
                    throw new IllegalStateException();
                }
                Object res = null;
                try {
                    res = deserialize.apply(recvBuf);
                } catch(Throwable t){
                    System.out.println("deserialize is wrong "+reqId);
                    throw new IllegalStateException();
                }
                msg.setReply(res);
                try {
                    recvBuf.clear();
                    if (RdmaConstants.JNI_CACHE_ENABLED)
                        this.postRecv.execute();
                    else
                        endpoint.postRecv(recvList).execute().free();
                } catch (IOException e) {
                    e.printStackTrace(); //TODO
                }
            } catch (InterruptedException e) {
                e.printStackTrace();//TODO
            }
        }
    }
}