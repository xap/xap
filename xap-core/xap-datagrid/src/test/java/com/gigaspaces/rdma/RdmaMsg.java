package com.gigaspaces.rdma;

import java.io.*;

public class RdmaMsg implements Externalizable {

    private Serializable payload;


    public RdmaMsg() {
    }

    public RdmaMsg(Serializable payload) {
        this.payload = payload;
    }

    public Serializable getPayload() {
        return payload;
    }

    public void setPayload(Serializable payload) {
        this.payload = payload;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(payload);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.payload = (Serializable) in.readObject();
    }
}
