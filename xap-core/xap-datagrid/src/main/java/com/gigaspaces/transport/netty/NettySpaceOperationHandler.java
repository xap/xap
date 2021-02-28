package com.gigaspaces.transport.netty;

import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.transport.LightMarshalInputStream;
import com.gigaspaces.transport.LightMarshalOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.Executor;

public class NettySpaceOperationHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(NettySpaceOperationHandler.class);

    private final SpaceImpl space;
    private final Executor executor;
    private final LightMarshalInputStream.Context misContext = new LightMarshalInputStream.Context();
    private final LightMarshalOutputStream.Context mosContext = new LightMarshalOutputStream.Context();

    public NettySpaceOperationHandler(SpaceImpl space, Executor executor) {
        this.space = space;
        this.executor = executor;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("Activated {} (thread: {})", ctx.channel(), Thread.currentThread().getName());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        //logger.info("channelRead (thread: {}, length: {})", Thread.currentThread().getName(), ((ByteBuf)msg).readableBytes());
        OperationTask task = new OperationTask((ByteBuf) msg, ctx);
        if (executor != null)
            executor.execute(task);
        else
            task.run();
    }

    private class OperationTask implements Runnable {
        final ByteBuf requestBuffer;
        final ChannelHandlerContext ctx;

        private OperationTask(ByteBuf requestBuffer, ChannelHandlerContext ctx) {
            this.requestBuffer = requestBuffer;
            this.ctx = ctx;
        }

        @Override
        public void run() {
            Object response;
            try {
                RemoteOperationRequest<?> request = deserialize(requestBuffer);
                response = space.executeOperation(request);
            } catch (Exception e) {
                logger.info("error: {}", e.toString());
                response = e;
            }
            try {
                final ByteBuf responseBuffer = ctx.alloc().buffer();
                serialize(responseBuffer, response);
                ctx.writeAndFlush(responseBuffer);
            } catch (IOException e) {
                logger.error("Failed to write result to channel", e);
            }
        }

        private RemoteOperationRequest<?> deserialize(ByteBuf buffer) throws IOException, ClassNotFoundException {
            try (InputStream is = new ByteBufInputStream(buffer, true);
                 ObjectInputStream ois = new LightMarshalInputStream(is, misContext)) {
                return (RemoteOperationRequest<?>) ois.readObject();
            }
        }

        private void serialize(ByteBuf buffer, Object obj) throws IOException {
            try (OutputStream os = new ByteBufOutputStream(buffer);
                 ObjectOutputStream mos = new LightMarshalOutputStream(os, mosContext)) {
                mos.writeObject(obj);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        logger.error("Exception caught, closing channel {}", ctx.channel(), cause);
        ctx.close();
    }
}
