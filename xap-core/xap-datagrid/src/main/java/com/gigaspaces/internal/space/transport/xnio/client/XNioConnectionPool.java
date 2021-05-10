package com.gigaspaces.internal.space.transport.xnio.client;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.space.transport.xnio.XNioChannel;
import com.gigaspaces.internal.space.transport.xnio.XNioSettings;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 * @author Niv Ingberg
 * @since 16.0
 */
@ExperimentalApi
public interface XNioConnectionPool extends Closeable {
    XNioChannel acquire() throws IOException;

    void release(XNioChannel channel);

    default SocketChannel createChannel(InetSocketAddress serverAddress, int connectionTimeout) {
        try {
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(true);
            //LRMIUtilities.initNewSocketProperties(socketChannel);
            XNioSettings.initSocketChannel(socketChannel);
            socketChannel.socket().connect(serverAddress, connectionTimeout);
            return socketChannel;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
