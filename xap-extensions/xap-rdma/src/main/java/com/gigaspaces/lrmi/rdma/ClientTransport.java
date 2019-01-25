package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.*;
import org.slf4j.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

// @todo - handle timeouts ?

public class ClientTransport {

    private final RdmaSender rdmaSender;
    private ConcurrentHashMap<Long, RdmaMsg> repMap = new ConcurrentHashMap<>();
    private AtomicLong nextId = new AtomicLong(0);
    private static final LinkedList<ByteBuffer> resources = new LinkedList<>();
    private final ArrayBlockingQueue<RdmaMsg> writeRequests = new ArrayBlockingQueue<>(100);
    private final ArrayBlockingQueue<IbvWC> recvEventQueue = new ArrayBlockingQueue<>(100);
    private final ExecutorService recvHandler = Executors.newFixedThreadPool(1);
    private final ExecutorService sendHandler = Executors.newFixedThreadPool(1);
    private final RdmaResourceManager resourceManager;
    private Logger logger = DiSNILogger.getLogger();

    public ClientTransport(GSRdmaClientEndpoint endpoint, RdmaResourceFactory factory, Function<ByteBuffer, Object> deserializer) throws IOException {
        resourceManager = new RdmaResourceManager(factory, 1);
        recvHandler.submit(new RdmaClientReceiver(recvEventQueue, repMap, endpoint, deserializer));
        rdmaSender = new RdmaSender(resourceManager, writeRequests);
        sendHandler.submit(rdmaSender);
    }

    public static LinkedList<IbvRecvWR> createRecvWorkRequest(long id, IbvMr mr) {
        LinkedList<IbvRecvWR> wr_list = new LinkedList<>();
        IbvRecvWR recvWR = new IbvRecvWR();
        recvWR.setWr_id(id);
        LinkedList<IbvSge> sgeLinkedList = new LinkedList<>();
        IbvSge sge = new IbvSge();
        sge.setAddr(mr.getAddr());
        sge.setLength(mr.getLength());
        sge.setLkey(mr.getLkey());
        sgeLinkedList.add(sge);
        recvWR.setSg_list(sgeLinkedList);
        wr_list.add(recvWR);
        return wr_list;
    }

    public <REQ, REP> CompletableFuture<REP> send(REQ request) {
        RdmaMsg<REQ, REP> rdmaMsg = new RdmaMsg<>(request);
        long id = nextId.incrementAndGet();
        rdmaMsg.setId(id);
        repMap.put(id, rdmaMsg);
        rdmaMsg.getFuture().whenComplete((T, throwable) -> repMap.remove(id));
        try {
            writeRequests.add(rdmaMsg);
        } catch (Exception e) {
            System.out.println("throwing exception " + e);
            rdmaMsg.getFuture().completeExceptionally(e);
        }
        return rdmaMsg.getFuture();
    }

    public synchronized void onCompletionEvent(IbvWC event) throws IOException {
        long wr_id = event.getWr_id();
        int opcode = event.getOpcode();
        if (logger.isDebugEnabled()) {
            logger.debug("CLIENT: op code = " + IbvWC.IbvWcOpcode.valueOf(opcode) + ", id = " + wr_id + ", err = " + event.getErr());
        }

        if (wr_id == RdmaConstants.RDMA_CLIENT_SEND_ID) {
            if(IbvWC.IbvWcOpcode.valueOf(opcode).equals(IbvWC.IbvWcOpcode.IBV_WC_RECV)){
                logger.error("got recv event with send wr id");
            }
            resourceManager.releaseResource((short) wr_id);
        }
        if (wr_id == RdmaConstants.RDMA_CLIENT_RECV_ID) {
            if(IbvWC.IbvWcOpcode.valueOf(opcode).equals(IbvWC.IbvWcOpcode.IBV_WC_SEND)){
                logger.error("got send event with recv wr id");
            }
            recvEventQueue.add(event); //TODO mybe offer? protect capacity
        }
    }

    public static LinkedList<IbvSendWR> createSendWorkRequest(long id, IbvMr mr) {
        LinkedList<IbvSendWR> wr_list = new LinkedList<>();
        IbvSendWR sendWR = new IbvSendWR();
        sendWR.setWr_id(id);
        //@todo sendMsg id in the envelop
        LinkedList<IbvSge> sgeLinkedList = new LinkedList<>();
        IbvSge sge = new IbvSge();
        sge.setAddr(mr.getAddr());
        sge.setLength(mr.getLength());
        sge.setLkey(mr.getLkey());
        sgeLinkedList.add(sge);
        sendWR.setSg_list(sgeLinkedList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        wr_list.add(sendWR);
        return wr_list;
    }


    static Object readResponse(ByteBuffer buffer) {
        byte[] arr = new byte[buffer.remaining()];
        buffer.get(arr);
        buffer.clear();
        try (ByteArrayInputStream ba = new ByteArrayInputStream(arr); ObjectInputStream in = new ObjectInputStream(ba)) {
            return in.readObject();
        } catch (Exception e) {
            DiSNILogger.getLogger().error(" failed to read object from stream", e);
            return e;
        }

    }

}
