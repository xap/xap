package com.gigaspaces.sql.aggregatornode.netty.client;

import com.j_spaces.jdbc.RequestPacket;
import com.j_spaces.jdbc.ResponsePacket;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

public final class EchoClient {

    static final boolean SSL = System.getProperty("ssl") != null;
    static final String HOST = System.getProperty("host", "127.0.0.1");
    static final int PORT = Integer.parseInt(System.getProperty("port", "8007"));

    private NioEventLoopGroup group;
    private Channel ch;

    public EchoClient() {
    }

    public void init() throws Exception {
        // Configure SSL.
        final SslContext sslCtx;
        if (SSL) {
            sslCtx = SslContextBuilder.forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        } else {
            sslCtx = null;
        }

        group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
             .channel(NioSocketChannel.class)
             .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline p = ch.pipeline();
                    if (sslCtx != null) {
                        p.addLast(sslCtx.newHandler(ch.alloc(), HOST, PORT));
                    }
                    p.addLast(
                            new ObjectEncoder(),
                            new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                            new ObjectEchoClientHandler(queue));
                }
             });

            // Start the connection attempt.
            ch = b.connect(HOST, PORT).sync().channel();

//
//            // Read commands from the stdin.
//            ChannelFuture lastWriteFuture = null;
//            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
//            for (;;) {
//                String line = in.readLine();
//                if (line == null) {
//                    break;
//                }
//
//                System.out.println("writing line: " + line);
//                // Sends the received line to the server.
//
//                RequestPacket req = new RequestPacket();
//                req.setStatement(line);
//                req.setType(RequestPacket.Type.STATEMENT);
//                lastWriteFuture = ch.writeAndFlush(req);
//
//                // If user typed the 'bye' command, wait until the server closes
//                // the connection.
//                if ("bye".equals(line.toLowerCase())) {
//                    ch.closeFuture().sync();
//                    break;
//                }
//            }
//
//            // Wait until all messages are flushed before closing the channel.
//            if (lastWriteFuture != null) {
//                lastWriteFuture.sync();
//            }
//

//            ch.closeFuture().sync();
        } finally {

        }
    }

    private Queue<CompletableFuture<ResponsePacket>> queue = new LinkedBlockingQueue<>();

    public CompletableFuture<ResponsePacket> send(String sql) throws InterruptedException {
        RequestPacket req = new RequestPacket();
        req.setStatement(sql);
        req.setType(RequestPacket.Type.STATEMENT);
        ChannelFuture lastWriteFuture = ch.writeAndFlush(req).sync();
        CompletableFuture<ResponsePacket> cf = new CompletableFuture<>();
        queue.add(cf);
        return cf;
    }

    public void close() {
        group.shutdownGracefully();
    }
}