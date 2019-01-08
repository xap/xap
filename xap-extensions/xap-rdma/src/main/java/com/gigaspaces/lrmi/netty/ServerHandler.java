package com.gigaspaces.lrmi.netty;

import com.gigaspaces.lrmi.rdma.RdmaMsg;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.function.Function;

public class ServerHandler extends ChannelInboundHandlerAdapter {
    private Function<Object, Object> process;

    public ServerHandler(Function<Object, Object> process) {
        this.process = process;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        RdmaMsg request = (RdmaMsg) msg;
        Object reply = process.apply(request.getRequest());
        RdmaMsg replyMsg = new RdmaMsg(reply);
        replyMsg.setId(request.getId());
        ctx.writeAndFlush(replyMsg);
    }
}
