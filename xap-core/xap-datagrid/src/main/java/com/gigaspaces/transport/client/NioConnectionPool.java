package com.gigaspaces.transport.client;

import com.gigaspaces.transport.NioChannel;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public interface NioConnectionPool extends Closeable {
    NioChannel acquire() throws IOException;

    void release(NioChannel channel);

    default SocketChannel createChannel(InetSocketAddress serverAddress, int connectionTimeout) {
        try {
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(true);
            //LRMIUtilities.initNewSocketProperties(socketChannel);
            socketChannel.socket().connect(serverAddress, connectionTimeout);
            // write code to init space read processor on server side:
            socketChannel.socket().getOutputStream().write(1);
            return socketChannel;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
