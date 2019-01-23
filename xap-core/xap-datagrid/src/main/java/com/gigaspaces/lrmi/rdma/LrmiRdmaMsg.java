package com.gigaspaces.lrmi.rdma;

import com.gigaspaces.lrmi.nio.ByteBufferPacketSerializer;
import com.gigaspaces.lrmi.nio.IPacket;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LrmiRdmaMsg extends RdmaMsg {

    private IPacket payload;
    private ByteBufferPacketSerializer serializer;

    public LrmiRdmaMsg(IPacket payload, ByteBufferPacketSerializer serializer) {
        this.payload = payload;
        this.serializer = serializer;
    }

    @Override
    public Object getPayload() {
        return payload;
    }

    @Override
    public void setPayload(Object payload) {
        this.payload = (IPacket) payload;
    }


}
