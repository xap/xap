package com.gigaspaces.transport.client;

import com.gigaspaces.transport.NioChannel;

import java.io.Closeable;
import java.io.IOException;

public interface NioConnectionPool extends Closeable {
    NioChannel acquire() throws IOException;

    void release(NioChannel channel);
}
