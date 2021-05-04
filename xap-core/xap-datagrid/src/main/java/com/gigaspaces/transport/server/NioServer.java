package com.gigaspaces.transport.server;

import com.gigaspaces.config.lrmi.nio.NIOConfiguration;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.lrmi.ProtocolAdapter;
import com.gigaspaces.transport.NioChannel;
import com.gigaspaces.transport.PocSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class NioServer implements Closeable {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final ServerSocketChannel ssc;
    private final SelectorHandler boss;
    private final SelectorHandler[] workers;
    private final SpaceImpl space;
    private int nextWorker;

    public NioServer(SpaceImpl space) throws IOException {
        InetSocketAddress bindAddress = calcBindAddress();
        int numOfWorkers = PocSettings.serverReaderPoolSize;
        logger.info("Listening to incoming connections at {} (I/O workers: {})", bindAddress, numOfWorkers);
        this.space = space;
        ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        ssc.socket().bind(bindAddress);
        boss = new SelectorHandler("boss");
        workers = numOfWorkers != -1 ? initWorkers(numOfWorkers) : new SelectorHandler[] {boss};
        ssc.register(boss.selector, SelectionKey.OP_ACCEPT);
        initDaemon(boss, "boss");
    }

    public static InetSocketAddress calcBindAddress() {
        ProtocolAdapter<?> protocolAdapter = LRMIRuntime.getRuntime().getProtocolRegistry().get(NIOConfiguration.PROTOCOL_NAME);
        if (protocolAdapter == null) {
            LRMIRuntime.getRuntime().initServerSide();
            protocolAdapter = LRMIRuntime.getRuntime().getProtocolRegistry().get(NIOConfiguration.PROTOCOL_NAME);
        }
        return new InetSocketAddress(protocolAdapter.getHostName(), protocolAdapter.getPort() + PocSettings.portDelta);
    }

    @Override
    public void close() throws IOException {
        // TODO: Implement close
    }

    private SelectorHandler[] initWorkers(int numOfWorkers) throws IOException {
        SelectorHandler[] result = new SelectorHandler[numOfWorkers];
        for (int i = 0; i < numOfWorkers; i++) {
            String name = "worker-" + i;
            result[i] = new SelectorHandler(name);
            initDaemon(result[i], name);
        }
        return result;
    }

    private void initDaemon(Runnable runnable, String name) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(true);
        thread.start();
    }

    private SelectorHandler getWorker() {
        SelectorHandler result = workers[nextWorker++];
        if (nextWorker == workers.length)
            nextWorker = 0;
        return result;
    }

    private void processAccept(SelectionKey key) throws IOException {
        SocketChannel clientSocket = ssc.accept();
        NioChannel channel = new NioChannel(clientSocket);
        byte code = (byte) clientSocket.socket().getInputStream().read();
        channel.setReadProcessor(initReader(code, space));
        clientSocket.configureBlocking(false);
        PocSettings.initSocketChannel(clientSocket);
        getWorker().register(channel);
    }

    private void processRead(SelectionKey key) throws IOException {
        NioChannel channel = (NioChannel) key.attachment();
        channel.getReadProcessor().read(channel);
    }

    public static void main(String[] args) throws IOException {
        //Settings.parseArgs(args);
        new NioServer(null).boss.run();
    }

    private class SelectorHandler implements Runnable {
        private final Logger logger;
        private final Selector selector;
        private final BlockingQueue<RegistrationRequest> registrationRequests = new LinkedBlockingQueue<>();

        public SelectorHandler(String name) throws IOException {
            logger = LoggerFactory.getLogger(this.getClass().getName() + "." + name);
            selector = Selector.open();
        }

        public void register(NioChannel channel) throws ClosedChannelException {
            if (this == boss)
                registerImpl(channel);
            else
                registrationRequests.offer(new RegistrationRequest(channel));
        }

        private void registerImpl(NioChannel channel) throws ClosedChannelException {
            channel.getSocketChannel().register(selector, SelectionKey.OP_READ, channel);
            logger.info("Added new client {} with processor {}", channel.getSocketChannel(), channel.getReadProcessor().getName());
        }

        @Override
        public void run() {
            try {
                while (true) {
                    while (!registrationRequests.isEmpty()) {
                        RegistrationRequest request = registrationRequests.poll();
                        registerImpl(request.channel);
                    }
                    if (selector.select(100) != 0) {
                        Set<SelectionKey> readySet = selector.selectedKeys();
                        for (Iterator<SelectionKey> it = readySet.iterator(); it.hasNext(); ) {
                            final SelectionKey key = it.next();
                            it.remove();
                            try {
                                if (key.isAcceptable()) {
                                    processAccept(key);
                                } else if (key.isReadable()) {
                                    processRead(key);
                                }
                            } catch (IOException e) {
                                logger.warn("Failed to read from {} - cancelling key", key.channel(), e);
                                key.cancel();
                            }
                        }
                    }
                }
            } catch(IOException e) {
                logger.error("Failed to process selector", e);
            }
        }
    }

    private static class RegistrationRequest {
        private final NioChannel channel;

        private RegistrationRequest(NioChannel channel) {
            this.channel = channel;
        }
    }

    private static ReadProcessor initReader(byte code, SpaceImpl space) {
        switch (code) {
            case 0:
                LoggerFactory.getLogger(ReadProcessor.class).info("Received exit command - terminating...");
                System.exit(0);
            case 1: return new SpaceReadProcessor(space);
            case 2: return new EchoReadProcessor();
            default: throw new IllegalArgumentException("Unsupported code: " + code);
        }
    }
}
