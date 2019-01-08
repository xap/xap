package com.gigaspaces.lrmi;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

public abstract class Receiver {
    public abstract SocketAddress getEndPointAddress();

    public abstract int read(ByteBuffer buffer) throws IOException;
}
