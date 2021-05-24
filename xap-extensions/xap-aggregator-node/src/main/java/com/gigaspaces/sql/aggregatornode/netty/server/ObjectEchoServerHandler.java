package com.gigaspaces.sql.aggregatornode.netty.server;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.SpaceFinder;
import com.j_spaces.jdbc.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Handles both client-side and server-side handler depending on which
 * constructor was called.
 */
public class ObjectEchoServerHandler extends ChannelInboundHandlerAdapter {

    private ConnectionContext context;
    private IQueryProcessor qp;
    private ISpaceProxy space;

    public ObjectEchoServerHandler() {
        try {
            this.space = (ISpaceProxy) SpaceFinder.find("jini://*/*/mySpace?groups=yohanaPC");
            this.qp = QueryProcessorFactory.newLocalInstance(space, space, new Properties(), null);
            this.context = qp.newConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)  {
        // Echo back the received object to the client.
        System.out.println(msg);
        ResponsePacket res = null;
        try {
            res = qp.executeQuery((RequestPacket) msg, context);
            ctx.write(res);
        } catch (RemoteException | SQLException e) {
            e.printStackTrace();
            ctx.write(new ResponsePacket());
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