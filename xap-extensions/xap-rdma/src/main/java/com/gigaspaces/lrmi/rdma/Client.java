package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.util.DiSNILogger;
import org.apache.log4j.BasicConfigurator;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


public class Client {

    private static String host = "192.168.33.137";
    private static int port = 8888;

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        GSRdmaEndpointFactory factory = new GSRdmaEndpointFactory(new RdmaResourceFactory());

        //we have passed our own endpoint factory to the group, therefore new endpoints will be of type GSRdmaClientEndpoint
        //let's create a new client endpoint
        GSRdmaClientEndpoint endpoint = (GSRdmaClientEndpoint) factory.create();

        //connect to the server
        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress address = new InetSocketAddress(ipAddress, port);

        try {
            endpoint.connect(address, 1000);
            DiSNILogger.getLogger().info("::client channel set up ");

            oneByOneScenario(endpoint);
//            pipelineScenario(endpoint);

            endpoint.close();
            DiSNILogger.getLogger().info("endpoint closed");
            factory.close();
            DiSNILogger.getLogger().info("group closed");

        } catch (Exception e) {
            DiSNILogger.getLogger().info("got exception", e);
        } finally {
            System.exit(0);
        }
    }

    private static void oneByOneScenario(GSRdmaClientEndpoint endpoint) throws InterruptedException, ExecutionException {
        CompletableFuture<RdmaMsg> future = endpoint.getTransport().send(new StringRdmaMsg("i am the client"));
        RdmaMsg respond = future.get();
        DiSNILogger.getLogger().info((String) respond.getPayload());

        CompletableFuture<RdmaMsg> future1 = endpoint.getTransport().send(new StringRdmaMsg("i am connected"));
        respond = future1.get();
        DiSNILogger.getLogger().info((String) respond.getPayload());

    }

    private static void pipelineScenario(GSRdmaClientEndpoint endpoint) throws InterruptedException {
        ArrayList<CompletableFuture<RdmaMsg>> futures = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            CompletableFuture<RdmaMsg> send = endpoint.getTransport()
                    .send(new StringRdmaMsg("i am the client msg number " + i));
            futures.add(send);
            send.whenComplete((s, throwable) -> DiSNILogger.getLogger().info((String) s.getPayload(), throwable));
        }

        for (CompletableFuture<RdmaMsg> future : futures) {
            try {
                RdmaMsg response = future.get();
                DiSNILogger.getLogger().info((String) response.getPayload());
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    public static class StringRdmaMsg extends RdmaMsg {

        private String payload;

        public StringRdmaMsg(String payload) {
            this.payload = payload;
        }

        @Override
        public Object getPayload() {
            return payload;
        }

        @Override
        public void setPayload(Object payload) {
            this.payload = (String) payload;
        }

        @Override
        public void serialize(ByteBuffer byteBuffer) {
            byteBuffer.asCharBuffer().put(payload);
        }
    }

}

