package com.gigaspaces.lrmi.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;

public class Server implements Callable {
    private int port;
    private Function<Object, Object> process;
    private Function<ByteBuf, Object> deserialize;
    private BiFunction<Object, ByteBuf, IOException> serialize;

    public Server(int port, Function<Object, Object> process, Function<ByteBuf, Object> deserialize, BiFunction<Object, ByteBuf, IOException> serialize) {
        this.port = port;
        this.process = process;
        this.deserialize = deserialize;
        this.serialize = serialize;
    }
    @Override
    public Object call() throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new MsgDecoder(deserialize));
                            ch.pipeline().addLast(new MsgEncoder(serialize));
                            ch.pipeline().addLast(new ServerHandler(process));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)          // (5)
                    .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(port).sync(); // (7)

            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
        int port = 8092;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }

        new Server(port, o -> o.toString().toUpperCase(), NettyRMI::simpleDeserialize, NettyRMI::simpleSerialize).call();
    }
}

