package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class GSRdmaServerEndpoint extends GSRdmaAbstractEndpoint {

    private final RdmaResourceFactory factory;
    private RdmaResourceManager resourceManager;
    private ByteBuffer recvBuffer;
    private SVCPostRecv postRecv;
    private ArrayBlockingQueue<GSRdmaServerEndpoint> pendingRequests;
    private Logger logger = DiSNILogger.getLogger();
    private LinkedList<IbvRecvWR> recvList;


    public GSRdmaServerEndpoint(RdmaActiveEndpointGroup<GSRdmaAbstractEndpoint> endpointGroup, RdmaCmId idPriv, RdmaResourceFactory factory) throws IOException {
        super(endpointGroup, idPriv, true);
        this.factory = factory;
    }

    public void init() throws IOException {
        super.init();
        factory.setEndpoint(this);

        this.resourceManager = new RdmaResourceManager(factory, 1);
        this.recvBuffer = ByteBuffer.allocateDirect(RdmaConstants.bufferSize());
        IbvMr recvMr = registerMemory(recvBuffer).execute().free().getMr();
        this.recvList = ClientTransport.createRecvWorkRequest(RdmaConstants.RDMA_SERVER_RECV_ID, recvMr);
        this.postRecv = RdmaConstants.JNI_CACHE_ENABLED ? postRecv(recvList) : null;
    }

    public synchronized void dispatchCqEvent(IbvWC event) {
        long wr_id = event.getWr_id();
        int opcode = event.getOpcode();
        if (logger.isDebugEnabled()) {
            logger.debug("SERVER: op code = " + IbvWC.IbvWcOpcode.valueOf(opcode) + ", id = " + wr_id + ", err = " + event.getErr());
        }

        if (wr_id == RdmaConstants.RDMA_SERVER_SEND_ID) {
            if(IbvWC.IbvWcOpcode.valueOf(opcode).equals(IbvWC.IbvWcOpcode.IBV_WC_RECV)){
                logger.error("got recv event with send wr id");
            }
            resourceManager.releaseResource((short) wr_id);
        }
        if (wr_id == RdmaConstants.RDMA_SERVER_RECV_ID) {
            if(IbvWC.IbvWcOpcode.valueOf(opcode).equals(IbvWC.IbvWcOpcode.IBV_WC_SEND)){
                logger.error("got send event with recv wr id");
            }
            pendingRequests.add(this);
        }
    }

    @Override
    public synchronized void dispatchCmEvent(RdmaCmEvent cmEvent) throws IOException {
        super.dispatchCmEvent(cmEvent);
        if (cmEvent.getEvent() == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal()) {
            if (logger.isDebugEnabled()) {
                logger.debug("SERVER: closing connection to " + getDstAddr());
            }
            this.resourceManager = null;
            if (RdmaConstants.JNI_CACHE_ENABLED)
                this.postRecv.free();
            try {
                close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public ByteBuffer getRecvBuff() {
        return recvBuffer;
    }

    public RdmaResourceManager getResourceManager() {
        return resourceManager;
    }

    public void init(ArrayBlockingQueue<GSRdmaServerEndpoint> pendingRequests) throws IOException {
        this.pendingRequests = pendingRequests;
        executePostRecv();
    }

    public void executePostRecv() throws IOException {
        if (RdmaConstants.JNI_CACHE_ENABLED) {
            postRecv.execute();
        } else {
            postRecv(recvList).execute().free();
        }
    }
}



