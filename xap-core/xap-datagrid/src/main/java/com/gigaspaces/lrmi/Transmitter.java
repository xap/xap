package com.gigaspaces.lrmi;

import com.gigaspaces.lrmi.nio.IWriteInterestManager;
import com.gigaspaces.lrmi.nio.Writer;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

public abstract class Transmitter {

    protected IWriteInterestManager _writeInterestManager;

    public abstract boolean isOpen();
    public abstract boolean isBlocking();

    public abstract SocketAddress getEndPointAddress();

    public abstract boolean hasQueuedContexts();

    public abstract void writeProtocolValidationHeader() throws IOException;

    public abstract void writeBytesToChannelBlocking(ByteBuffer dataBuffer) throws IOException;

    public abstract void writeBytesToChannelNoneBlocking(Writer.Context ctx, boolean restoreReadInterest) throws IOException;

    public abstract void onWriteEvent() throws IOException;

    public void setWriteInterestManager(IWriteInterestManager writeInterestManager) {
        _writeInterestManager = writeInterestManager;
    }
}
