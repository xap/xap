package com.gigaspaces.transport.server;

import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.transport.NioChannel;
import com.gigaspaces.transport.PocSettings;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

public class EchoReadProcessor extends ReadProcessor {
    private final Executor executorService = PocSettings.serverLrmiExecutor ? LRMIRuntime.getRuntime().getThreadPool() : null;
    private final int expectedPayload = PocSettings.payload;

    @Override
    public String getName() {
        return "echo-" + (executorService == null ? "sync" : "async" + "-" + expectedPayload);
    }

    @Override
    public void read(NioChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(expectedPayload);
        int data = channel.getSocketChannel().read(buffer);
        if (data == -1)
            throw new EOFException("No data in channel " + channel);
        if (data != expectedPayload)
            logger.warn("Unexpected payload length: expected {}, actual {}", expectedPayload, data);
        buffer.flip();

        if (executorService == null)
            tryWrite(channel, buffer);
        else {
            executorService.execute(() -> tryWrite(channel, buffer));
        }
    }

    protected void tryWrite(NioChannel channel, ByteBuffer buffer) {
        try {
            channel.getSocketChannel().write(buffer);
        } catch (IOException e) {
            logger.error("Failed to write buffer", e);
        }
        if (buffer.hasRemaining()) {
            logger.warn("failed to write to buffer - {} bytes remaining", buffer.remaining());
        }
    }
}
