package com.gigaspaces.sql.aggregatornode.netty.server;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.sql.aggregatornode.netty.dao.Request;
import com.gigaspaces.sql.aggregatornode.netty.dao.Response;
import com.j_spaces.core.client.SpaceFinder;
import com.j_spaces.jdbc.ConnectionContext;
import com.j_spaces.jdbc.IQueryProcessor;
import com.j_spaces.jdbc.QueryProcessorFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Handles both client-side and server-side handler depending on which
 * constructor was called.
 */
public class ObjectServerHandler extends ChannelInboundHandlerAdapter {

    private ConnectionContext context;
    private IQueryProcessor qp;
    private ISpaceProxy space;

    public ObjectServerHandler(String spaceName) {
        try {
            this.space = (ISpaceProxy) SpaceFinder.find("jini://*/*/" + spaceName);
            this.qp = QueryProcessorFactory.newLocalInstance(space, space, new Properties(), null);
            this.context = qp.newConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // Echo back the received object to the client.
        System.out.println(msg);
        Request request = (Request) msg;
        try {
            Response res = new Response(request.getRequestId(), qp.executeQuery(request.getBody(), context));
            ctx.write(res);
        } catch (RemoteException | SQLException e) {
            e.printStackTrace();
            ctx.write(new Response(request.getRequestId(), e));
        }
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