package com.gigaspaces.lrmi.tcp;

import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.lrmi.nio.LrmiChannel;
import com.gigaspaces.lrmi.nio.NIOUtils;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

public class TcpChannel extends LrmiChannel {
    private final SocketChannel socketChannel;
    private final String socketDisplayString;

    public TcpChannel(SocketChannel socketChannel, ITransportConfig config) {
        super(new TcpWriter(socketChannel, config), new TcpReader(socketChannel, config.getSlowConsumerRetries()));
        this.socketChannel = socketChannel;
        this.socketDisplayString = NIOUtils.getSocketDisplayString(socketChannel);
    }

    @Override
    public String getSocketDisplayString() {
        return socketDisplayString;
    }

    @Override
    public String getCurrSocketDisplayString() {
        return NIOUtils.getSocketDisplayString(socketChannel);
    }

    @Override
    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    @Override
    public boolean isBlocking() {
        return socketChannel.isBlocking();
    }

    @Override
    public String getSocketDesc() {
        return socketChannel.socket().toString();
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
        Socket socket = socketChannel.socket();
        return socket != null ? socket.getLocalSocketAddress() : null;
    }

    @Override
    public Integer getLocalPort() {
        Socket socket = socketChannel.socket();
        return socket != null ? socket.getLocalPort() : null;
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        Socket socket = socketChannel.socket();
        return socket != null ? socket.getRemoteSocketAddress() : null;
    }

    @Override
    public void close() throws IOException {
        socketChannel.close();
    }
}
