/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.transport.netty;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public abstract class NettyFactory {

    public static NettyFactory getDefault() {
        return Epoll.isAvailable() ? new EpollFactory() : new NioFactory();
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
}
