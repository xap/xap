package com.gigaspaces.sql.aggregatornode.netty.client;

import com.gigaspaces.sql.aggregatornode.netty.dao.Response;
import com.j_spaces.jdbc.ResponsePacket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Handler implementation for the object echo client.  It initiates the
 * ping-pong traffic between the object echo client and server by sending the
 * first message to the server.
 */
public class ObjectClientHandler extends ChannelInboundHandlerAdapter {
    private final Map<Integer, CompletableFuture<ResponsePacket>> requests;

    /**
     * Creates a client-side handler.
     *
     * @param requests
     */
    public ObjectClientHandler(Map<Integer, CompletableFuture<ResponsePacket>> requests) {
        this.requests = requests;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // Echo back the received object to the server.
        Response res = ((Response) msg);
        requests.remove(res.getRequestId()).complete(res.getBody());
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}