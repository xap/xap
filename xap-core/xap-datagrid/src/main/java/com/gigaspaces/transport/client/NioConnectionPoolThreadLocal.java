package com.gigaspaces.transport.client;

import com.gigaspaces.transport.NioChannel;
import com.gigaspaces.transport.PocSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public class NioConnectionPoolThreadLocal implements NioConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(NioConnectionPoolThreadLocal.class);

    private final InetSocketAddress serverAddress;
    private final ThreadLocal<NioChannel> threadLocal;
    private final int connectionTimeout;

    public NioConnectionPoolThreadLocal() {
        this(new InetSocketAddress(PocSettings.host, PocSettings.port));
    }

    public NioConnectionPoolThreadLocal(InetSocketAddress address) {
        this(address, PocSettings.clientConnectionPoolSize, 10_000);
    }

    public NioConnectionPoolThreadLocal(InetSocketAddress address, int capacity, int connectionTimeout) {
        this.serverAddress = address;
        this.connectionTimeout = connectionTimeout;
        threadLocal = ThreadLocal.withInitial(() -> new NioChannel(createChannel()));
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

    private SocketChannel createChannel()  {
        try {
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(true);
            //LRMIUtilities.initNewSocketProperties(socketChannel);
            socketChannel.socket().connect(serverAddress, connectionTimeout);
            return socketChannel;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
