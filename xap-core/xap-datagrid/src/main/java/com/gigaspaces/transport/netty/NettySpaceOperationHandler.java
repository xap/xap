/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.transport.netty;

import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.transport.serializers.NettyReusableMarshalSerializer;
import com.gigaspaces.transport.serializers.NettySerializer;
import io.netty.buffer.ByteBuf;
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
    private final NettySerializer serializer = new NettyReusableMarshalSerializer();

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
                RemoteOperationRequest<?> request = serializer.deserialize(requestBuffer);
                requestBuffer.release();
                response = space.executeOperation(request);
            } catch (Exception e) {
                logger.info("error: {}", e.toString());
                response = e;
            }
            try {
                final ByteBuf responseBuffer = ctx.alloc().buffer();
                serializer.serialize(responseBuffer, response);
                ctx.writeAndFlush(responseBuffer);
            } catch (IOException e) {
                logger.error("Failed to write result to channel", e);
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
