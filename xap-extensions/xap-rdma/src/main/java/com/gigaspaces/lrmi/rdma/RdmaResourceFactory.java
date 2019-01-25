package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.RdmaActiveEndpoint;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RdmaResourceFactory {

    private RdmaActiveEndpoint endpoint;

    public RdmaResourceFactory() {
    }

    public RdmaResource create() throws IOException {
        short id = (short) (endpoint.isServerSide() ? RdmaConstants.RDMA_SERVER_SEND_ID : RdmaConstants.RDMA_CLIENT_SEND_ID);
        ByteBuffer buffer = ByteBuffer.allocateDirect(RdmaConstants.bufferSize());
        return new RdmaResource(id, buffer, endpoint);
    }

    public void setEndpoint(RdmaActiveEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public RdmaActiveEndpoint getEndpoint() {
        return endpoint;
    }
}
