package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.verbs.SVCPostSend;

import java.nio.ByteBuffer;

public class RdmaResource {

    private final short id;
    private final ByteBuffer buffer;
    private final SVCPostSend postSend;

    public RdmaResource(short id, ByteBuffer buffer, SVCPostSend postSend) {
        this.id = id;
        this.buffer = buffer;
        this.postSend = postSend;
    }

    public short getId() {
        return id;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public SVCPostSend getPostSend() {
        return postSend;
    }
}
