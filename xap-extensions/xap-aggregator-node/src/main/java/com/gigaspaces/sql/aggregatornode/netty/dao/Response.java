package com.gigaspaces.sql.aggregatornode.netty.dao;

import com.j_spaces.jdbc.ResponsePacket;

import java.io.Serializable;

public class Response implements Serializable {
    private Integer requestId;
    private ResponsePacket body;
    private Throwable throwable;

    public Response() {
    }

    public Response(Integer requestId, ResponsePacket body) {
        this.requestId = requestId;
        this.body = body;
    }

    public Response(Integer requestId, Throwable throwable) {
        this.requestId = requestId;
        this.throwable = throwable;
    }

    public Integer getRequestId() {
        return requestId;
    }

    public Response setRequestId(Integer requestId) {
        this.requestId = requestId;
        return this;
    }

    public ResponsePacket getBody() {
        return body;
    }

    public Response setBody(ResponsePacket body) {
        this.body = body;
        return this;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public Response setThrowable(Throwable throwable) {
        this.throwable = throwable;
        return this;
    }

    @Override
    public String toString() {
        return "Response{" +
                "requestId=" + requestId +
                ", body=" + body +
                ", throwable=" + throwable +
                '}';
    }
}
