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
package com.gigaspaces.internal.space.transport.xnio.server;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.transport.xnio.XNioChannel;
import com.gigaspaces.lrmi.LRMIRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

/**
 * @author Niv Ingberg
 * @since 16.0
 */
@ExperimentalApi
public class SpaceReadProcessor extends ReadProcessor {
    private final SpaceImpl space;
    private final Executor executorService = LRMIRuntime.getRuntime().getThreadPool();

    public SpaceReadProcessor(SpaceImpl space) {
        this.space = space;
    }

    @Override
    public String getName() {
        return "space-operation-executor-" + (executorService == null ? "sync" : "async");
    }

    @Override
    public void read(XNioChannel channel) throws IOException {
        ByteBuffer requestBuffer = channel.readNonBlocking();
        if (requestBuffer != null) {
            Runnable task = new SpaceOperationTask(requestBuffer, space, channel);
            executorService.execute(task);
        }
    }

    private static class SpaceOperationTask implements Runnable {
        private static final Logger logger = LoggerFactory.getLogger(SpaceOperationTask.class);
        private final ByteBuffer requestBuffer;
        private final SpaceImpl space;
        private final XNioChannel channel;

        private SpaceOperationTask(ByteBuffer buffer, SpaceImpl space, XNioChannel channel) {
            this.requestBuffer = buffer;
            this.space = space;
            this.channel = channel;
        }

        @Override
        public void run() {
            Object response;
            try {
                RemoteOperationRequest<?> request = (RemoteOperationRequest<?>) channel.deserialize(requestBuffer);
                response = space.executeOperation(request);
            } catch (Exception e) {
                response = e;
            }
            try {
                ByteBuffer responseBuffer = channel.serialize(response);
                channel.writeBlocking(responseBuffer);
            } catch (IOException e) {
                logger.error("Failed to write result to channel", e);
            }
        }
    }
}
