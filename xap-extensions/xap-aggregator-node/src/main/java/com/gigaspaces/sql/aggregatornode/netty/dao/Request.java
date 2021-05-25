package com.gigaspaces.sql.aggregatornode.netty.dao;

import com.j_spaces.jdbc.RequestPacket;

import java.io.Serializable;

public class Request implements Serializable {
    private Integer requestId;
    private RequestPacket body;

    public Request() {
    }

    public Request(Integer requestId, RequestPacket body) {
        this.requestId = requestId;
        this.body = body;
    }

    public Integer getRequestId() {
        return requestId;
    }

    public Request setRequestId(Integer requestId) {
        this.requestId = requestId;
        return this;
    }

    public RequestPacket getBody() {
        return body;
    }

    public Request setBody(RequestPacket body) {
        this.body = body;
        return this;
    }

    @Override
    public String toString() {
        return "Request{" +
                "requestId=" + requestId +
                ", body=" + body +
                '}';
    }
}
