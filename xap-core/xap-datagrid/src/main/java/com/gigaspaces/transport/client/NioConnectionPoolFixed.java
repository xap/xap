package com.gigaspaces.transport.client;

import com.gigaspaces.transport.NioChannel;
import com.gigaspaces.transport.PocSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public class NioConnectionPoolFixed implements NioConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(NioConnectionPoolFixed.class);

    private final InetSocketAddress serverAddress;
    private final NioChannel[] pool;
    private final int connectionTimeout;

    public NioConnectionPoolFixed() {
        this(new InetSocketAddress(PocSettings.host, PocSettings.port), PocSettings.clientConnectionPoolSize, 10_000);
    }

    public NioConnectionPoolFixed(InetSocketAddress address, int capacity, int connectionTimeout) {
        this.serverAddress = address;
        this.connectionTimeout = connectionTimeout;
        this.pool = new NioChannel[capacity];
        for (int i = 0; i < capacity; i++) {
            try {
                pool[i] = new NioChannel(createChannel());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public NioChannel acquire() throws IOException {
        for (NioChannel channel : pool) {
            if (channel.tryAcquire())
                return channel;
        }
        throw new IOException("No free channel");
    }

    public void release(NioChannel channel) {
        channel.release();
    }

    @Override
    public void close() {
        for (NioChannel channel : pool) {
            if (channel != null)
                closeSilently(channel);
        }
    }

    private SocketChannel createChannel() throws IOException {
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(true);
        //LRMIUtilities.initNewSocketProperties(socketChannel);
        socketChannel.socket().connect(serverAddress, connectionTimeout);
        return socketChannel;
    }

    private void closeSilently(NioChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            logger.warn("Failed to close socket channel", e);
        }
    }
}
