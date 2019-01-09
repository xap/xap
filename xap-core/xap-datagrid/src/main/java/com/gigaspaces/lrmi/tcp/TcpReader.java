package com.gigaspaces.lrmi.tcp;

import com.gigaspaces.lrmi.nio.Reader;
import com.gigaspaces.lrmi.nio.SystemRequestHandler;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class TcpReader extends Reader {

    public TcpReader(SocketChannel sockChannel, int slowConsumerRetries) {
        super(sockChannel, slowConsumerRetries, null);
    }

    public TcpReader(SocketChannel sockChannel, SystemRequestHandler systemRequestHandler) {
        super(sockChannel, Integer.MAX_VALUE, systemRequestHandler);
    }

    @Override
    protected SocketAddress getEndPointAddress() {
        return _socketChannel.socket().getRemoteSocketAddress();
    }

    @Override
    protected int directRead(ByteBuffer buffer) throws IOException {
        return _socketChannel.read(buffer);
    }
}
