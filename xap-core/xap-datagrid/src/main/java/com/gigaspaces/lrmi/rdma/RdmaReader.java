package com.gigaspaces.lrmi.rdma;

import com.gigaspaces.lrmi.nio.Reader;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class RdmaReader extends Reader {

    public RdmaReader() {
        super(null);
    }

    @Override
    protected int readHeaderBlocking(ByteBuffer buffer, int slowConsumerLatency, AtomicInteger retries) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void readPayloadBlocking(ByteBuffer buffer, int dataLength, int slowConsumerLatency, AtomicInteger retries) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String getEndpointDesc() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected SocketAddress getEndPointAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String getEndPointAddressDesc() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected int directRead(ByteBuffer buffer) throws IOException {
        throw new UnsupportedOperationException();
    }
}
