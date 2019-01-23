package com.gigaspaces.lrmi.rdma;

public abstract class RdmaMsg {

    private transient long id;

    public abstract Object getPayload();

    public abstract void setPayload(Object payload);


    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}
