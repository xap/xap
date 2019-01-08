package com.gigaspaces.lrmi.rdma;

import com.gigaspaces.exception.lrmi.SlowConsumerException;
import com.gigaspaces.lrmi.nio.Writer;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

public class RdmaWriter extends Writer {

    public RdmaWriter() {
        super();
    }

    @Override
    public SocketAddress getEndPointAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOpen() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean hasQueuedContexts() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void writeNonBlocking(Context ctx, boolean restoreReadInterest) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void onWriteEventImpl() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeProtocolValidationHeader() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBytesToChannelBlocking(ByteBuffer dataBuffer) throws IOException, ClosedChannelException, SlowConsumerException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBlocking() {
        throw new UnsupportedOperationException();
    }
}
