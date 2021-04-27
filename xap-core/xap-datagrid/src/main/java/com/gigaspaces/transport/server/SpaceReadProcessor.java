package com.gigaspaces.transport.server;

import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntrySpaceOperationRequest;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.transport.NioChannel;
import com.gigaspaces.transport.PocSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

public class SpaceReadProcessor extends ReadProcessor {
    private final SpaceImpl space;
    private final Executor executorService = PocSettings.serverLrmiExecutor ? LRMIRuntime.getRuntime().getThreadPool() : null;

    public SpaceReadProcessor(SpaceImpl space) {
        this.space = space;
    }

    @Override
    public String getName() {
        return "space-operation-executor-" + (executorService == null ? "sync" : "async");
    }

    @Override
    public void read(NioChannel channel) throws IOException {
        ByteBuffer requestBuffer = channel.readNonBlocking();
        if (requestBuffer != null) {
            Runnable task = PocSettings.cacheResponse
                    ? new CacheableSpaceOperationTask(requestBuffer, space, channel)
                    : new SpaceOperationTask(requestBuffer, space, channel);
            if (executorService != null)
                executorService.execute(task);
            else
                task.run();
        }
    }

    private static class SpaceOperationTask implements Runnable {
        private static final Logger logger = LoggerFactory.getLogger(SpaceOperationTask.class);
        private final ByteBuffer requestBuffer;
        private final SpaceImpl space;
        private final NioChannel channel;

        private SpaceOperationTask(ByteBuffer buffer, SpaceImpl space, NioChannel channel) {
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

    private static class CacheableSpaceOperationTask implements Runnable {
        private static final Logger logger = LoggerFactory.getLogger(CacheableSpaceOperationTask.class);
        private final ByteBuffer requestBuffer;
        private final SpaceImpl space;
        private final NioChannel channel;

        private CacheableSpaceOperationTask(ByteBuffer buffer, SpaceImpl space, NioChannel channel) {
            this.requestBuffer = buffer;
            this.space = space;
            this.channel = channel;
        }

        @Override
        public void run() {
            ByteBuffer responseBuffer;
            byte[] cachedResponse = channel.getCachedResponse();
            try {
                if (cachedResponse != null) {
                    responseBuffer = ByteBuffer.wrap(cachedResponse);
                } else {
                    try {
                        RemoteOperationRequest<?> request = (RemoteOperationRequest<?>) channel.deserialize(requestBuffer);
                        logger.info("Executing {}", request.getClass().getSimpleName());
                        Object response = space.executeOperation(request);
                        responseBuffer = channel.serialize(response);
                        if (request instanceof ReadTakeEntrySpaceOperationRequest) {
                            cachedResponse = toByteArray(channel.serialize(response));
                            channel.setCachedResponse(cachedResponse);
                            logger.info("Cached response for future executions - length: {}", cachedResponse.length);
                        }
                    } catch (Exception e) {
                        responseBuffer = channel.serialize(e);
                    }
                }
                channel.writeBlocking(responseBuffer);
            } catch (IOException e) {
                logger.error("Failed to write result to channel", e);
            }
        }

        private byte[] toByteArray(ByteBuffer buffer) {
            byte[] result = new byte[buffer.limit()];
            buffer.get(result);
            return result;
        }
    }
}
