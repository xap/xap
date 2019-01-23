package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.util.DiSNILogger;
import org.apache.log4j.BasicConfigurator;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


public class Client {

    private static String host = "192.168.33.137";
    private static int port = 8888;

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        GSRdmaEndpointFactory factory = new GSRdmaEndpointFactory();

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
        CompletableFuture<String> future = endpoint.getTransport().send("i am the client");
        String respond = future.get();
        DiSNILogger.getLogger().info(respond);

        CompletableFuture<String> future1 = endpoint.getTransport().send("i am connected");
        respond = future1.get();
        DiSNILogger.getLogger().info(respond);

    }

    private static void pipelineScenario(GSRdmaClientEndpoint endpoint) throws InterruptedException {
        ArrayList<CompletableFuture<String>> futures = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            CompletableFuture<String> send = endpoint.getTransport().send("i am the client msg number " + i);
            futures.add(send);
            send.whenComplete((s, throwable) -> DiSNILogger.getLogger().info(s, throwable));
        }

        for (CompletableFuture<String> future : futures) {
            try {
                String response = future.get();
                DiSNILogger.getLogger().info(response);
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

}

