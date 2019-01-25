package com.gigaspaces.lrmi.netty;

import com.gigaspaces.lrmi.rdma.RdmaMsg;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ClientHandler extends SimpleChannelInboundHandler<RdmaMsg> {
    private Map<Long, RdmaMsg> map;

    public ClientHandler(Map<Long, RdmaMsg> map) {
        this.map = map;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RdmaMsg reply) {
        RdmaMsg request = map.remove(reply.getId());
        CompletableFuture future = request != null ? request.getFuture() : null;
        if (request != null)
            request.setReply(reply.getRequest());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        System.out.println("got exception: "+ cause);
    }
}