package com.gigaspaces.lrmi.rdma;

import com.gigaspaces.lrmi.nio.IPacket;

public class LrmiRdmaMsg extends RdmaMsg {

    private IPacket payload;

    public LrmiRdmaMsg(IPacket payload) {
        this.payload = payload;
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
