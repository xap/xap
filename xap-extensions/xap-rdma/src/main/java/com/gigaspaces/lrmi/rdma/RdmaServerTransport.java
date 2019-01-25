package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.util.DiSNILogger;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class RdmaServerTransport implements Runnable {


    private final ExecutorService executorService;
    private final ArrayBlockingQueue<GSRdmaServerEndpoint> pendingRequests;
    private final RdmaServerEndpoint<GSRdmaAbstractEndpoint> serverEndpoint;


    public RdmaServerTransport(InetSocketAddress address, Function<Object, Object> process,
                               int executorsCount, Function<ByteBuffer, Object> deserialize, RdmaResourceFactory resourceFactory) throws Exception {
        executorService = Executors.newFixedThreadPool(executorsCount);
        pendingRequests = new ArrayBlockingQueue<>(RdmaConstants.MAX_INCOMMING_REQUESTS);

        GSRdmaEndpointFactory factory = new GSRdmaEndpointFactory(resourceFactory, deserialize);
        serverEndpoint = factory.getEndpointGroup().createServerEndpoint();

        serverEndpoint.bind(address, 10);

        for (int i = 0; i < executorsCount; i++) {
            executorService.submit(new RdmaServerReceiver(pendingRequests, process, deserialize));
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                GSRdmaServerEndpoint rdmaEndpoint = (GSRdmaServerEndpoint) serverEndpoint.accept();
                rdmaEndpoint.init(pendingRequests);
                DiSNILogger.getLogger().info("destination: " + rdmaEndpoint.getDstAddr() + ", source: " + rdmaEndpoint.getSrcAddr());
            } catch (Exception e) {
                DiSNILogger.getLogger().error(e.getMessage(),e);
            }
        }
    }
}
