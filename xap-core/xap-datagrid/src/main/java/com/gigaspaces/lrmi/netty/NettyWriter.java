package com.gigaspaces.lrmi.netty;

import com.gigaspaces.exception.lrmi.SlowConsumerException;
import com.gigaspaces.lrmi.nio.Writer;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

public class NettyWriter extends Writer {
    public NettyWriter() {
        super();
    }

    @Override
    public SocketAddress getEndPointAddress() {
        return null;
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    protected boolean hasQueuedContexts() {
        return false;
    }

    @Override
    protected void writeNonBlocking(Context ctx, boolean restoreReadInterest) throws IOException {

    }

    @Override
    protected void onWriteEventImpl() throws IOException {

    }

    @Override
    public void writeProtocolValidationHeader() throws IOException {

    }

    @Override
    public void writeBytesToChannelBlocking(ByteBuffer dataBuffer) throws IOException, ClosedChannelException, SlowConsumerException {

    }

    @Override
    public boolean isBlocking() {
        return false;
    }
}
