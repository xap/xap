package com.gigaspaces.lrmi.rdma;

import java.util.concurrent.CompletableFuture;

public class RdmaMsg<REQ, REP> {

    private long id;
    private REQ request;
    private CompletableFuture<REP> future = new CompletableFuture<>();

    public RdmaMsg(REQ request) {
        this.request = request;
    }

    public long getId() {
        return id;
    }
    public void setId(long id) {
        this.id = id;
    }

    public REQ getRequest() {
        return request;
    }

    public void setReply(REP reply) {
        if (reply instanceof Throwable) {
            future.completeExceptionally((Throwable) reply);
        } else {
            future.complete(reply);
        }
    }

    public CompletableFuture<REP> getFuture() {
        return future;
    }

    @Override
    public String toString() {
        return "RdmaMsg{" +
                "id=" + id +
                ", request=" + request +
                ", future=" + future +
                '}';
    }
}
