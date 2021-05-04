package com.gigaspaces.transport.client;

import com.gigaspaces.transport.NioChannel;
import com.gigaspaces.transport.PocSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class NioConnectionPoolFixed implements NioConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(NioConnectionPoolFixed.class);

    private final NioChannel[] pool;

    public NioConnectionPoolFixed(InetSocketAddress address) {
        this(address, PocSettings.clientConnectionPoolSize, 10_000);
    }

    public NioConnectionPoolFixed(InetSocketAddress address, int capacity, int connectionTimeout) {
        this.pool = new NioChannel[capacity];
        for (int i = 0; i < capacity; i++) {
            pool[i] = new NioChannel(createChannel(address, connectionTimeout));
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

    private void closeSilently(NioChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            logger.warn("Failed to close socket channel", e);
        }
    }
}
