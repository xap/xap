package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.RdmaActiveEndpoint;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RdmaResourceFactory {

    private RdmaActiveEndpoint endpoint;

    public RdmaResourceFactory() {
    }

    public RdmaResource create() throws IOException {
        short id = (short) RdmaConstants.nextId();
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
