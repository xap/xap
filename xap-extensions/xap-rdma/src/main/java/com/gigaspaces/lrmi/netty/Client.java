package com.gigaspaces.lrmi.netty;

import com.gigaspaces.lrmi.rdma.RdmaMsg;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.epoll.EpollSocketChannel;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

public class Client implements Closeable {
    private final Channel channel;
    private final Bootstrap bootstrap;
    private final Map<Long, CompletableFuture<RdmaMsg>> map;
    private final AtomicLong nextId = new AtomicLong();
    private BiFunction<Object, ByteBuf, IOException> serialize;

    public Client(InetSocketAddress inetSocketAddress, Function<ByteBuf, Object> deserialize,
                  BiFunction<Object, ByteBuf, IOException> serialize) throws InterruptedException {
        this.serialize = serialize;
        map = new ConcurrentHashMap<>();
        bootstrap = new Bootstrap();
        bootstrap.group(NettyRMI.getInstance().getEventLoopGroup());
        bootstrap.channel(EpollSocketChannel.class);
        bootstrap.remoteAddress(inetSocketAddress);
        bootstrap.handler(new ChannelInitializer<EpollSocketChannel>() {
            protected void initChannel(EpollSocketChannel socketChannel) {
                socketChannel.pipeline().addLast(new MsgDecoder(deserialize));
                socketChannel.pipeline().addLast(new MsgEncoder(serialize));
                socketChannel.pipeline().addLast(new ClientHandler(map));
            }
        });
        channel = bootstrap.connect().sync().channel();
    }


    CompletableFuture<RdmaMsg> send(RdmaMsg msg){
        long id = nextId.incrementAndGet();
        msg.setId(id);
        CompletableFuture<RdmaMsg> future = new CompletableFuture<>();
        map.put(id, future);
        channel.writeAndFlush(msg);
        return future;
    }

    @Override
    public void close() throws IOException {

    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
        Client client = new Client(new InetSocketAddress("barak-nixos", 8092),
                NettyRMI::simpleDeserialize,
                NettyRMI::simpleSerialize);
        CompletableFuture<RdmaMsg> future = client.send(new RdmaMsg("I am the client 1"));
        RdmaMsg msg = future.get();
        System.out.println("--> Client got " + msg);
        future = client.send(new RdmaMsg("I am the client 2"));
         msg = future.get();
        System.out.println("--> Client got " + msg);

    }
}
