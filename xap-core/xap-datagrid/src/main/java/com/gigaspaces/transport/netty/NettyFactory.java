package com.gigaspaces.transport.netty;

import com.gigaspaces.transport.PocSettings;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.incubator.channel.uring.IOUring;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringServerSocketChannel;

public abstract class NettyFactory {

    public static NettyFactory getDefault() {
        if (IOUring.isAvailable())
            return new IOUringFactory();
        if (Epoll.isAvailable())
            return new EpollFactory();
        return new NioFactory();
    }

    public static NettyFactory getDefault(PocSettings.ServerType serverType) {
        switch (serverType) {
            case NETTY: return getDefault();
            case NETTY_NIO: return new NioFactory();
            case NETTY_EPOLL: return new EpollFactory();
            case NETTY_IOURING: return new IOUringFactory();
            default: throw new IllegalArgumentException("Unsupported: " + serverType);
        }
    }

    public abstract Class<? extends ServerSocketChannel> getServerSocketChannel();

    public EventLoopGroup createEventLoopGroup() {
        return createEventLoopGroup(0);
    }

    public abstract EventLoopGroup createEventLoopGroup(int nThreads);

    private static class NioFactory extends NettyFactory {
        @Override
        public EventLoopGroup createEventLoopGroup(int nThreads) {
            return new NioEventLoopGroup(nThreads);
        }

        @Override
        public Class<NioServerSocketChannel> getServerSocketChannel() {
            return NioServerSocketChannel.class;
        }
    }

    private static class EpollFactory extends NettyFactory {
        @Override
        public EventLoopGroup createEventLoopGroup(int nThreads) {
            return new EpollEventLoopGroup(nThreads);
        }

        @Override
        public Class<? extends ServerSocketChannel> getServerSocketChannel() {
            return EpollServerSocketChannel.class;
        }
    }

    private static class IOUringFactory extends NettyFactory {
        @Override
        public EventLoopGroup createEventLoopGroup(int nThreads) {
            return new IOUringEventLoopGroup(nThreads);
        }

        @Override
        public Class<? extends ServerSocketChannel> getServerSocketChannel() {
            return IOUringServerSocketChannel.class;
        }
    }
}
