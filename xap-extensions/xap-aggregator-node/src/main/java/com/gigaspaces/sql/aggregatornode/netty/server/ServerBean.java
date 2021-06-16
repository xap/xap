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
package com.gigaspaces.sql.aggregatornode.netty.server;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.sql.aggregatornode.netty.authentication.AuthenticationProvider;
import com.gigaspaces.sql.aggregatornode.netty.query.QueryProviderImpl;
import com.gigaspaces.start.SystemInfo;
import com.j_spaces.core.client.SpaceFinder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.openspaces.pu.container.jee.JeeServiceDetails;
import org.openspaces.pu.container.jee.JeeType;
import org.openspaces.pu.service.ServiceDetails;
import org.openspaces.pu.service.ServiceDetailsProvider;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

public final class ServerBean implements AutoCloseable, ServiceDetailsProvider {
    static final boolean SSL = System.getProperty("ssl") != null;
    static final int PORT = Integer.parseInt(System.getProperty("port", "5432"));

    private String spaceName;
    private ISpaceProxy space;

    AuthenticationProvider authProvider;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public ServerBean() {
    }

    public ServerBean(String spaceName) {
        this.spaceName = spaceName;
    }

    @PostConstruct
    public void init() throws Exception {
        space = (ISpaceProxy) SpaceFinder.find("jini://*/*/" + spaceName);

        authProvider = AuthenticationProvider.NO_OP_PROVIDER; // TODO

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .handler(new LoggingHandler(LogLevel.INFO))
         .childHandler(new ChannelInitializer<SocketChannel>() {
             @Override
             public void initChannel(SocketChannel ch) {
                 ChannelPipeline pipeline = ch.pipeline();

                 if (SSL)
                     ch.pipeline().addLast("ssl_processor", new SslProcessor());
                 pipeline
                         .addLast("msg_delimiter", new MessageDelimiter())
                         .addLast("msg_processor", new MessageProcessor(new QueryProviderImpl(space), authProvider));
             }
         });

        // Bind and start to accept incoming connections.
        b.bind(PORT).sync().channel().closeFuture();
    }

    @PreDestroy
    public void close() {
        if (bossGroup != null)
            bossGroup.shutdownGracefully();

        if (workerGroup != null)
            workerGroup.shutdownGracefully();
    }

    @Override
    public ServiceDetails[] getServicesDetails() {
        final String host = SystemInfo.singleton().network().getHostId();
        JeeServiceDetails details = new JeeServiceDetails(host, PORT, 0, "/", false, "jetty", JeeType.CUSTOM, 0);
        return new ServiceDetails[]{details};
    }
}