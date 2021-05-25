package com.gigaspaces.sql.aggregatornode.netty.client;

import com.gigaspaces.sql.aggregatornode.netty.dao.Request;
import com.gigaspaces.sql.aggregatornode.netty.dao.Response;
import com.j_spaces.jdbc.RequestPacket;
import com.j_spaces.jdbc.ResponsePacket;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public final class Client {

    static final boolean SSL = System.getProperty("ssl") != null;
    static final String HOST = System.getProperty("host", "127.0.0.1");
    static final int PORT = Integer.parseInt(System.getProperty("port", "8007"));

    private NioEventLoopGroup group;
    private Channel ch;

    private final Map<Integer, CompletableFuture<ResponsePacket>> requests = new ConcurrentHashMap<>();
    private final AtomicInteger count = new AtomicInteger(0);

    public Client() {
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
                                new ObjectClientHandler(requests));
                    }
                });

        // Start the connection attempt.
        ch = b.connect(HOST, PORT).sync().channel();

    }

    public CompletableFuture<ResponsePacket> send(String sql) throws InterruptedException {
        RequestPacket req = new RequestPacket();
        req.setStatement(sql);
        req.setType(RequestPacket.Type.STATEMENT);

        Request request = new Request(count.getAndIncrement(), req);
        ChannelFuture lastWriteFuture = ch.writeAndFlush(request).sync();
        CompletableFuture<ResponsePacket> cf = new CompletableFuture<>();
        requests.put(request.getRequestId(), cf);
        return cf;
    }

    public void close() {
        group.shutdownGracefully();
    }
}