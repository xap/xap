package com.gigaspaces.transport.client;

import com.gigaspaces.transport.NioChannel;
import com.gigaspaces.transport.PocSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class NioConnectionPoolThreadLocal implements NioConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(NioConnectionPoolThreadLocal.class);

    private final ThreadLocal<NioChannel> threadLocal;

    public NioConnectionPoolThreadLocal() {
        this(new InetSocketAddress(PocSettings.host, PocSettings.port), 10_000);
    }

    public NioConnectionPoolThreadLocal(InetSocketAddress address, int connectionTimeout) {
        threadLocal = ThreadLocal.withInitial(() -> new NioChannel(createChannel(address, connectionTimeout)));
    }

    public NioChannel acquire() throws IOException {
        return threadLocal.get();
    }

    public void release(NioChannel channel) {
    }

    @Override
    public void close() throws IOException {
        threadLocal.get().close();
        threadLocal.remove();
    }

    private void closeSilently(NioChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            logger.warn("Failed to close socket channel", e);
        }
    }
}
