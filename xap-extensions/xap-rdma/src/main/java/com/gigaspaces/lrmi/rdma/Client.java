package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.util.DiSNILogger;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class Client {

    private static String host = "192.168.72.60";
    private static int port = 8888;
    private static Logger logger = DiSNILogger.getLogger();

    static {
        if (host.length() == 0) {
            try {
                host = InetAddress.getLocalHost().getHostAddress();
                System.out.println("Binding to " + host);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);

        GSRdmaEndpointFactory factory = new GSRdmaEndpointFactory(new RdmaResourceFactory(), ClientTransport::readResponse);

        //we have passed our own endpoint factory to the group, therefore new endpoints will be of type GSRdmaClientEndpoint
        //let's create a new client endpoint
        GSRdmaClientEndpoint endpoint = (GSRdmaClientEndpoint) factory.create();

        //connect to the server
        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress address = new InetSocketAddress(ipAddress, port);

        try {
            endpoint.connect(address, 1000);

            logger.info("::client channel set up ");

            oneByOneScenario(endpoint);

            System.out.println("Completed!");
            endpoint.close();
            logger.info("endpoint closed");
            factory.close();
            logger.info("group closed");

        } catch (Throwable e) {
            logger.info("got exception", e);
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }

    private static void oneByOneScenario(GSRdmaClientEndpoint endpoint) throws InterruptedException, ExecutionException, TimeoutException {
        for (int i = 0; i < 50000; i++) {
            CompletableFuture<String> future = endpoint.getTransport().send("i am the client");
            try {
                future.get(5000, TimeUnit.MILLISECONDS);
            }catch (Throwable e){
                System.out.println("exception at i = "+i);
                e.printStackTrace();
                throw e;
            }
//            logger.info(future.get());
        }

    }

    private static void pipelineScenario(GSRdmaClientEndpoint endpoint) throws InterruptedException {
        ArrayList<CompletableFuture<String>> futures = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            CompletableFuture<String> send = endpoint.getTransport().send("i am the client msg number " + i);
            futures.add(send);
            send.whenComplete((s, throwable) -> logger.info(s, throwable));
        }

        for (CompletableFuture<String> future : futures) {
            try {
                String response = future.get();
                logger.info(response);
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }


}

