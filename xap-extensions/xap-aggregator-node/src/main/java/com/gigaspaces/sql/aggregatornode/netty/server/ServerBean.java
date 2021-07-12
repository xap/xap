package com.gigaspaces.sql.aggregatornode.netty.server;

import com.gigaspaces.sql.aggregatornode.netty.authentication.AuthenticationProvider;
import com.gigaspaces.sql.aggregatornode.netty.query.QueryProviderImpl;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

public final class ServerBean implements AutoCloseable {
    static final boolean SSL = System.getProperty("ssl") != null;
    private int port = 5432;

    AuthenticationProvider authProvider;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public ServerBean() {
    }

    public ServerBean(int port) {
        this.port = port;
    }


    @PostConstruct
    public void init() throws Exception {
        // TODO use real authentication provider
        authProvider = AuthenticationProvider.NO_OP_PROVIDER;

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
                         .addLast("msg_processor", new MessageProcessor(new QueryProviderImpl(), authProvider));
             }
         });

        // Bind and start to accept incoming connections.
        b.bind(port).sync().channel().closeFuture();
    }

    @PreDestroy
    public void close() {
        if (bossGroup != null)
            bossGroup.shutdownGracefully();

        if (workerGroup != null)
            workerGroup.shutdownGracefully();
    }
}