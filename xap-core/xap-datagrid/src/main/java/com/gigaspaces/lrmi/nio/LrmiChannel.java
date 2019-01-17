package com.gigaspaces.lrmi.nio;

import java.io.Closeable;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

public abstract class LrmiChannel implements Closeable {
    private final Writer writer;
    private final Reader reader;

    public LrmiChannel(Writer writer, Reader reader) {
        this.writer = writer;
        this.reader = reader;
    }

    public Writer getWriter() {
        return writer;
    }

    public Reader getReader() {
        return reader;
    }

    public abstract SocketChannel getSocketChannel();

    public abstract boolean isBlocking();

    public abstract String getSocketDesc();

    public abstract SocketAddress getLocalSocketAddress();

    public abstract Integer getLocalPort();

    public abstract SocketAddress getRemoteSocketAddress();

    public abstract String getSocketDisplayString();

    public abstract String getCurrSocketDisplayString();
}
