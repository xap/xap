package com.gigaspaces.transport.client;

import com.gigaspaces.transport.NioChannel;
import com.gigaspaces.transport.PocSettings;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public class NioConnectionPoolSingleton implements NioConnectionPool {

    private final InetSocketAddress address;
    private final int connectionTimeout;
    private NioChannel instance;

    public NioConnectionPoolSingleton(InetSocketAddress address) {
        this(address, 10_000);
    }

    public NioConnectionPoolSingleton(InetSocketAddress address, int connectionTimeout) {
        this.address = address;
        this.connectionTimeout = connectionTimeout;
    }

    public NioChannel acquire() throws IOException {
        if (instance == null)
            instance = new NioChannel(createChannel(address, connectionTimeout));
        return instance;
    }

    public void release(NioChannel channel) {
    }

    @Override
    public void close() throws IOException {
        instance.close();
    }
}
