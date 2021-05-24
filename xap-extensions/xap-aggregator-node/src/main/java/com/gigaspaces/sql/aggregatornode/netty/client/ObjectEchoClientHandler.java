package com.gigaspaces.sql.aggregatornode.netty.client;

import com.gigaspaces.sql.aggregatornode.netty.client.output.DumpUtils;
import com.j_spaces.jdbc.ResponsePacket;
import com.j_spaces.jdbc.driver.GResultSet;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * Handler implementation for the object echo client.  It initiates the
 * ping-pong traffic between the object echo client and server by sending the
 * first message to the server.
 */
public class ObjectEchoClientHandler extends ChannelInboundHandlerAdapter {
    private final Queue<CompletableFuture<ResponsePacket>> queue;

//    private final List<Integer> firstMessage;

    /**
     * Creates a client-side handler.
     * @param queue
     */
    public ObjectEchoClientHandler(Queue<CompletableFuture<ResponsePacket>> queue) {
        this.queue = queue;
//        firstMessage = new ArrayList<Integer>(ObjectEchoClient.SIZE);
//        for (int i = 0; i < ObjectEchoClient.SIZE; i ++) {
//            firstMessage.add(Integer.valueOf(i));
//        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // Send the first message if this handler is a client-side handler.
//        ChannelFuture future = ctx.writeAndFlush(firstMessage);
//        future.addListener(FIRE_EXCEPTION_ON_FAILURE); // Let object serialisation exceptions propagate.
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // Echo back the received object to the server.
        ResponsePacket res = ((ResponsePacket) msg);
//        GResultSet resultSet = new GResultSet(null, res.getResultEntry());

        queue.remove().complete(res);
//        try {
//            DumpUtils.dump(resultSet);
//        } catch (SQLException throwables) {
//            throwables.printStackTrace();
//        }
//        ctx.close();
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