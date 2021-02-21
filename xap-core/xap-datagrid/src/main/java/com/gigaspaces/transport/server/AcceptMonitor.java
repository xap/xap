package com.gigaspaces.transport.server;

import com.gigaspaces.transport.NioChannel;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class AcceptMonitor extends SelectMonitor {
    private final NioServer server;

    public AcceptMonitor(NioServer server) throws IOException {
        super("accept");
        this.server = server;
    }

    @Override
    protected void onReady(SelectionKey key) throws IOException {
        SocketChannel newSocket = server.accept();
        logger.info("Accepting new connection: {}", newSocket);
        newSocket.configureBlocking(false);
        newSocket.socket().setTcpNoDelay(true);
        SelectMonitor readMonitor = server.getNextReadMonitor();
        readMonitor.register(newSocket, SelectionKey.OP_READ, new NioChannel(newSocket));
    }
}
