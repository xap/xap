package com.gigaspaces.lrmi.netty;

import com.gigaspaces.lrmi.rdma.RdmaMsg;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Map;
import java.util.concurrent.CompletableFuture;


public class ClientHandler extends SimpleChannelInboundHandler<RdmaMsg> {
    private Map<Long, CompletableFuture<RdmaMsg>> map;

    public ClientHandler(Map<Long, CompletableFuture<RdmaMsg>> map) {
        this.map = map;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RdmaMsg msg) {
        System.out.println("Client read msg=" + msg.getId());
        CompletableFuture<RdmaMsg> future = map.remove(msg.getId());
        System.out.println("Client found future =" +  future + " for id " + msg.getId());
        if(future != null){
            future.complete(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        System.out.println("got exception: "+ cause);
    }
}