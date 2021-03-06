package com.gigaspaces.internal.space.transport.xnio.client;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.space.transport.xnio.XNioChannel;

import com.gigaspaces.internal.space.transport.xnio.XNioSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author Niv Ingberg
 * @since 16.0
 */
@ExperimentalApi
public class XNioConnectionPoolDynamic implements XNioConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(XNioConnectionPoolDynamic.class);

    private final InetSocketAddress serverAddress;
    private final Queue<XNioChannel> pool;
    private final int connectionTimeout;

    public XNioConnectionPoolDynamic(InetSocketAddress address) {
        this(address, XNioSettings.CLIENT_CONNECTION_POOL_SIZE, 10_000);
    }

    public XNioConnectionPoolDynamic(InetSocketAddress address, int capacity, int connectionTimeout) {
        this.serverAddress = address;
        this.pool = new LinkedBlockingDeque<>(capacity);
        this.connectionTimeout = connectionTimeout;
    }

    public XNioChannel acquire() throws IOException {
        XNioChannel result = pool.poll();
        if (result == null) {
            logger.debug("No pooled resource - creating a new one");
            result = new XNioChannel(createChannel(serverAddress, connectionTimeout));
        }
        return result;
    }

    public void release(XNioChannel channel) {
        if (!pool.offer(channel)) {
            logger.debug("Resource pool is full - closing released resource");
            closeSilently(channel);
        }
    }

    @Override
    public void close() {
        while (!pool.isEmpty()) {
            XNioChannel connection = pool.poll();
            if (connection != null)
                closeSilently(connection);
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
