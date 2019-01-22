package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import com.ibm.disni.verbs.SVCPostRecv;

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
        this.postRecv = postRecv(ClientTransport.createRecvWorkRequest(2004, recvMr));
        postRecv.execute();
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

    public ByteBuffer getRecvBuff() {
        return recvBuffer;
    }

    public RdmaResourceManager getResourceManager() {
        return resourceManager;
    }

    public void setPendingRequests(ArrayBlockingQueue<GSRdmaServerEndpoint> pendingRequests) {
        this.pendingRequests = pendingRequests;
    }

    public SVCPostRecv getPostRecv() {
        return postRecv;
    }
}



