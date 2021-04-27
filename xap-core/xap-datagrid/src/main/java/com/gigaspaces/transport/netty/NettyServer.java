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

import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.lrmi.LRMIUtilities;
import com.gigaspaces.transport.PocSettings;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executor;

public class NettyServer implements Closeable {
    private final Logger logger = LoggerFactory.getLogger(NettyServer.class);
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final int maxFrameLength = 100 * 1024; // TODO: configurable
    private final int lengthBytes = 4; // Number of bytes for length (int == 4 bytes)
    private final Channel channel;

    public NettyServer(SpaceImpl space, PocSettings.ServerType serverType) {
        this(space, new InetSocketAddress(PocSettings.host, PocSettings.port), serverType);
    }

    public NettyServer(SpaceImpl space, SocketAddress address, PocSettings.ServerType serverType) {
        NettyFactory factory = NettyFactory.getDefault(serverType);
        int workers = PocSettings.serverReaderPoolSize;
        boolean lrmiExecutor = PocSettings.serverLrmiExecutor;
        logger.info("Starting NettyServer (address: {}, channel: {}, workers: {}, lrmiExecutor: {})",
                address, factory.getServerSocketChannel(), workers, lrmiExecutor);
        // Configure the server.
        this.bossGroup = factory.createEventLoopGroup(1);
        this.workerGroup = factory.createEventLoopGroup(workers);
        Executor executor = lrmiExecutor ? LRMIRuntime.getRuntime().getThreadPool() : null;
        ServerBootstrap b = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(factory.getServerSocketChannel())
                .option(ChannelOption.SO_BACKLOG, 100)
                //.handler(new LoggingHandler(LogLevel.INFO))
                .childOption(ChannelOption.TCP_NODELAY, LRMIUtilities.TCP_NO_DELAY_MODE)
                .childOption(ChannelOption.SO_KEEPALIVE, LRMIUtilities.KEEP_ALIVE_MODE)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(
                                newFrameDecoder(),
                                newFrameEncoder(),
                                new NettySpaceOperationHandler(space, executor));
                    }
                });

        // Start the server.
        //ChannelFuture f = b.bind(address).syncUninterruptibly();
        ChannelFuture future = b.bind(address);
        this.channel = future.channel();
        future.awaitUninterruptibly();
        // Wait until the server socket is closed.
        //f.channel().closeFuture().sync();
    }

    private ChannelInboundHandler newFrameDecoder() {

        return new LengthFieldBasedFrameDecoder(
                maxFrameLength,
                0, // no offset - length is first
                lengthBytes,
                0, // no adjustment required
                lengthBytes); // strip off length from frame
    }

    private ChannelOutboundHandler newFrameEncoder() {
        return new LengthFieldPrepender(lengthBytes);
    }

    @Override
    public void close() throws IOException {
        logger.info("Closing");
        // Shut down all event loops to terminate all threads.
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        channel.close().awaitUninterruptibly();
    }
}
