package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;

import java.io.IOException;

public class GSRdmaClientEndpoint extends GSRdmaAbstractEndpoint {


    private ClientTransport transport;

    public GSRdmaClientEndpoint(RdmaActiveEndpointGroup<GSRdmaAbstractEndpoint> endpointGroup,
                                RdmaCmId idPriv) throws IOException {
        super(endpointGroup, idPriv, false);
    }

    //important: we override the init method to prepare some buffers (memory registration, post recv, etc).
    //This guarantees that at least one recv operation will be posted at the moment this endpoint is connected.
    public void init() throws IOException {
        super.init();

        transport = new ClientTransport(this);

    }


    public void dispatchCqEvent(IbvWC wc) throws IOException {
        DiSNILogger.getLogger().info("CLIENT: op code = " + IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) + ", id = " + wc.getWr_id());
        getTransport().onCompletionEvent(wc);
    }


    public ClientTransport getTransport() {
        return transport;
    }

}
