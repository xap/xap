package com.gigaspaces.transport.server;

import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.transport.NioChannel;
import com.gigaspaces.transport.PocSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

public class ReadMonitor extends SelectMonitor {
    private final SpaceImpl space;
    private final Executor executorService = PocSettings.serverLrmiExecutor ? LRMIRuntime.getRuntime().getThreadPool() : null;

    public ReadMonitor(NioServer server, int id) throws IOException {
        super("read-" + id);
        this.space = server.getSpace();
    }

    @Override
    protected void onReady(SelectionKey key) throws IOException {
        NioChannel nioChannel = (NioChannel) key.attachment();
        ByteBuffer requestBuffer = nioChannel.readNonBlocking();
        if (requestBuffer != null) {
            SpaceOperationTask task = new SpaceOperationTask(requestBuffer, space, nioChannel);
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
}
