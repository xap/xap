package com.gigaspaces.internal.space.transport.xnio.client;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.space.transport.xnio.XNioChannel;
import com.gigaspaces.internal.space.transport.xnio.XNioSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author Niv Ingberg
 * @since 16.0
 */
@ExperimentalApi
public class XNioConnectionPoolFixed implements XNioConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(XNioConnectionPoolFixed.class);

    private final XNioChannel[] pool;

    public XNioConnectionPoolFixed(InetSocketAddress address) {
        this(address, XNioSettings.CLIENT_CONNECTION_POOL_SIZE, 10_000);
    }

    public XNioConnectionPoolFixed(InetSocketAddress address, int capacity, int connectionTimeout) {
        this.pool = new XNioChannel[capacity];
        for (int i = 0; i < capacity; i++) {
            pool[i] = new XNioChannel(createChannel(address, connectionTimeout));
        }
    }

    public XNioChannel acquire() throws IOException {
        for (XNioChannel channel : pool) {
            if (channel.tryAcquire())
                return channel;
        }
        throw new IOException("No free channel");
    }

    public void release(XNioChannel channel) {
        channel.release();
    }

    @Override
    public void close() {
        for (XNioChannel channel : pool) {
            if (channel != null)
                closeSilently(channel);
        }
    }

    private void closeSilently(XNioChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            logger.warn("Failed to close socket channel", e);
        }
    }
}
