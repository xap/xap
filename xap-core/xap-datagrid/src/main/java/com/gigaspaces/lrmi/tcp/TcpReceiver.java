package com.gigaspaces.lrmi.tcp;

import com.gigaspaces.lrmi.Receiver;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class TcpReceiver extends Receiver {
    private final SocketChannel _socketChannel;

    public TcpReceiver(SocketChannel socketChannel) {
        this._socketChannel = socketChannel;
    }

    @Override
    public SocketAddress getEndPointAddress() {
        return _socketChannel.socket().getRemoteSocketAddress();
    }

    @Override
    public int read(ByteBuffer buffer) throws IOException {
        return _socketChannel.read(buffer);
    }
}
