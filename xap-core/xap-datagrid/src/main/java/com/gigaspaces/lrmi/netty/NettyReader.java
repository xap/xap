package com.gigaspaces.lrmi.netty;

import com.gigaspaces.lrmi.nio.Reader;
import com.gigaspaces.lrmi.nio.SystemRequestHandler;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyReader extends Reader {
    public NettyReader() {
        super(null);
    }

    protected NettyReader(SystemRequestHandler systemRequestHandler) {
        super(systemRequestHandler);
    }

    @Override
    protected int readHeaderBlocking(ByteBuffer buffer, int slowConsumerLatency, AtomicInteger retries) throws IOException {
        return 0;
    }

    @Override
    protected void readPayloadBlocking(ByteBuffer buffer, int dataLength, int slowConsumerLatency, AtomicInteger retries) throws IOException {

    }

    @Override
    protected String getEndpointDesc() {
        return null;
    }

    @Override
    protected SocketAddress getEndPointAddress() {
        return null;
    }

    @Override
    protected String getEndPointAddressDesc() {
        return null;
    }

    @Override
    protected int directRead(ByteBuffer buffer) throws IOException {
        return 0;
    }
}
