package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

import static com.gigaspaces.lrmi.rdma.RdmaConstants.BUFFER_SIZE;

public class GSRdmaServerEndpoint extends GSRdmaAbstractEndpoint {

    private RdmaResourceManager resourceManager;
    private ByteBuffer recvBuffer;
    private SVCPostRecv postRecv;
    private ArrayBlockingQueue<GSRdmaServerEndpoint> pendingRequests;


    public GSRdmaServerEndpoint(RdmaActiveEndpointGroup<GSRdmaAbstractEndpoint> endpointGroup, RdmaCmId idPriv) throws IOException {
        super(endpointGroup, idPriv, true);
    }

    public void init() throws IOException {
        super.init();
        this.resourceManager = new RdmaResourceManager(this, 1);
        this.recvBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        IbvMr recvMr = registerMemory(recvBuffer).execute().free().getMr();
        this.postRecv = postRecv(ClientTransport.createRecvWorkRequest(RdmaConstants.nextId(), recvMr));
    }

    public void dispatchCqEvent(IbvWC event) {
        DiSNILogger.getLogger().info("SERVER: op code = " + IbvWC.IbvWcOpcode.valueOf(event.getOpcode()) + ", id = " + event.getWr_id() + ", err = " + event.getErr());

        if (IbvWC.IbvWcOpcode.valueOf(event.getOpcode()).equals(IbvWC.IbvWcOpcode.IBV_WC_SEND)) {
            resourceManager.releaseResource((short) event.getWr_id());
        }
        if (IbvWC.IbvWcOpcode.valueOf(event.getOpcode()).equals(IbvWC.IbvWcOpcode.IBV_WC_RECV)) {
            pendingRequests.add(this);
        }
    }

    @Override
    public synchronized void dispatchCmEvent(RdmaCmEvent cmEvent) throws IOException {
        super.dispatchCmEvent(cmEvent);
        if(cmEvent.getEvent() == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal()){
            DiSNILogger.getLogger().info("SERVER: closing connection to "+getDstAddr());
            this.resourceManager = null;
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
        postRecv.execute();
    }

    public SVCPostRecv getPostRecv() {
        return postRecv;
    }
}



