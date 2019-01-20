package com.gigaspaces.rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

// @todo - handle timeouts ?

public class ClientTransport<Req extends Serializable, Rep extends Serializable> {

    private ConcurrentHashMap<Long, CompletableFuture<Rep>> repMap = new ConcurrentHashMap<>();
    private AtomicLong nextId = new AtomicLong(0);
    private RdmaActiveEndpoint endpoint;
    private final int BUFFER_SIZE = 1000;
    private final ByteBuffer recv_buf = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final LinkedList<ByteBuffer> resources = new LinkedList<>();

    public ClientTransport(RdmaActiveEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public CompletableFuture<Rep> send(Req req) {
        long id = nextId.incrementAndGet();
        CompletableFuture<Rep> future = new CompletableFuture<>();
        repMap.put(id, future);
        future.whenComplete((rep, throwable) -> repMap.remove(id));
        try {
            ByteBuffer buffer = serializeToBuffer(req);
            rdmaSendBuffer(id, buffer).execute().free();
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    private SVCPostRecv rdmaRecvBuffer(long id, ByteBuffer buffer) throws IOException {
        IbvMr mr = endpoint.registerMemory(buffer).execute().free().getMr();
        LinkedList<IbvRecvWR> wrs = createRecvWorkRequest(id, mr);
        return endpoint.postRecv(wrs);
    }

    private SVCPostSend rdmaSendBuffer(long id, ByteBuffer buffer) throws IOException {
        IbvMr mr = endpoint.registerMemory(buffer).execute().free().getMr();
        LinkedList<IbvSendWR> wr_list = createSendWorkRequest(id, mr);
        return endpoint.postSend(wr_list);
    }

    private LinkedList<IbvSendWR> createSendWorkRequest(long id, IbvMr mr) {
        LinkedList<IbvSendWR> wr_list = new LinkedList<>();
        IbvSendWR sendWR = new IbvSendWR();
        sendWR.setWr_id(id);
        //@todo send id in the envelop
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

    private LinkedList<IbvRecvWR> createRecvWorkRequest(long id, IbvMr mr) {
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

    private ByteBuffer serializeToBuffer(Req req) throws IOException {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bytesOut);
        oos.writeObject(req);
        oos.flush();
        byte[] bytes = bytesOut.toByteArray();
        bytesOut.close();
        oos.close();
        ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length);
        direct.put(bytes);
        resources.add(direct);
        return direct;
    }

    public void onCompletionEvent(IbvWC event) throws IOException {

        if (IbvWC.IbvWcOpcode.valueOf(event.getOpcode()).equals(IbvWC.IbvWcOpcode.IBV_WC_SEND)) {
            rdmaRecvBuffer(event.getWr_id(), this.recv_buf).execute().free();
        }
        if (IbvWC.IbvWcOpcode.valueOf(event.getOpcode()).equals(IbvWC.IbvWcOpcode.IBV_WC_RECV)) {
            CompletableFuture<Rep> future = repMap.get(event.getWr_id());
            try {
                Rep rep = readResponse(recv_buf);
                future.complete(rep);
            } catch (ClassNotFoundException e) {
                future.completeExceptionally(e);
            }
        }
    }

    static <T> T readResponse(ByteBuffer buffer) throws IOException, ClassNotFoundException {
        byte[] arr = new byte[buffer.remaining()];
        buffer.get(arr);
        buffer.clear();
        try (ByteArrayInputStream ba = new ByteArrayInputStream(arr);
             ObjectInputStream in = new ObjectInputStream(ba)) {
            return (T) in.readObject();
        } catch (Exception e) {
            DiSNILogger.getLogger().error(" failed to read object from stream", e);
            throw e;
        }

    }
}
