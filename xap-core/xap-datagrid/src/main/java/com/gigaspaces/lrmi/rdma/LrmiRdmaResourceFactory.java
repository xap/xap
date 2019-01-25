package com.gigaspaces.lrmi.rdma;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LrmiRdmaResourceFactory extends RdmaResourceFactory {

    @Override
    public RdmaResource create() throws IOException {
        short id = (short) RdmaConstants.nextId();
        ByteBuffer buffer = ByteBuffer.allocateDirect(RdmaConstants.bufferSize());
        return new LrmiRdmaResource(id, buffer, getEndpoint());
    }
}
