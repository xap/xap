package com.gigaspaces.lrmi;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class Transmitter {

    public abstract boolean isOpen();
    public abstract boolean isBlocking();
    public abstract void writeBytesToChannelBlocking(ByteBuffer dataBuffer) throws IOException;
}
