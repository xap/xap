package com.gigaspaces.lrmi.rdma;

public class RdmaMsg {

    private transient long id;
    private Object payload;


    public RdmaMsg(Object payload) {
        this.payload = payload;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }


    public Object getPayload() {
        return this.payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

}
