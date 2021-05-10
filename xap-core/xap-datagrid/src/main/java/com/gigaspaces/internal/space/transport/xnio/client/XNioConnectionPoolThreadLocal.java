package com.gigaspaces.internal.space.transport.xnio.client;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.space.transport.xnio.XNioChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author Niv Ingberg
 * @since 16.0
 */
@ExperimentalApi
public class XNioConnectionPoolThreadLocal implements XNioConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(XNioConnectionPoolThreadLocal.class);

    private final ThreadLocal<XNioChannel> threadLocal;

    public XNioConnectionPoolThreadLocal(InetSocketAddress address) {
        this(address, 10_000);
    }

    public XNioConnectionPoolThreadLocal(InetSocketAddress address, int connectionTimeout) {
        threadLocal = ThreadLocal.withInitial(() -> new XNioChannel(createChannel(address, connectionTimeout)));
    }

    public XNioChannel acquire() throws IOException {
        return threadLocal.get();
    }

    public void release(XNioChannel channel) {
    }

    @Override
    public void close() throws IOException {
        threadLocal.get().close();
        threadLocal.remove();
    }

    private void closeSilently(XNioChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            logger.warn("Failed to close socket channel", e);
        }
    }
}
