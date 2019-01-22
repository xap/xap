package com.gigaspaces.lrmi.rdma;

import com.gigaspaces.lrmi.ServerAddress;
import com.gigaspaces.lrmi.nio.LrmiChannel;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

public class RdmaChannel extends LrmiChannel {

    private RdmaChannel(RdmaWriter writer, RdmaReader reader) {
        super(writer, reader);
    }

    public static RdmaChannel create(ServerAddress address) {
        // TODO: connect/create RDMa endpoint via address
        RdmaWriter writer = new RdmaWriter();
        RdmaReader reader = new RdmaReader();
        return new RdmaChannel(writer, reader);
    }

    @Override
    public SocketChannel getSocketChannel() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBlocking() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSocketDesc() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer getLocalPort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSocketDisplayString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCurrSocketDisplayString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException();
    }
}
