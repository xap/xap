package com.gigaspaces.lrmi.rdma;/*
 * DiSNI: Direct Storage and Networking Interface
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016-2018, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import com.ibm.disni.util.DiSNILogger;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;


public class Client {

    private final GSRdmaEndpointFactory factory;
    private final InetSocketAddress address;
    private final GSRdmaClientEndpoint endpoint;
    private String host = "192.168.33.137";
    private int port = 8888;

    public Client() throws IOException {
        factory = new GSRdmaEndpointFactory();

        //we have passed our own endpoint factory to the group, therefore new endpoints will be of type GSRdmaClientEndpoint
        //let's create a new client endpoint
        endpoint = factory.create();

        //connect to the server
        InetAddress ipAddress = InetAddress.getByName(host);
        address = new InetSocketAddress(ipAddress, port);
    }


    public void run() throws Exception {
        try {
            connect(1000);
            DiSNILogger.getLogger().info("SimpleClient::client channel set up ");

            CompletableFuture<String> future = send("i am the client");
            String respond = future.get();
            DiSNILogger.getLogger().info(respond);

            CompletableFuture<String> future1 = send("i am connected");
            respond = future1.get();
            DiSNILogger.getLogger().info(respond);

            close();
        } catch (Exception e) {
            DiSNILogger.getLogger().info("got exception", e);
        } finally {
            System.exit(0);
        }
    }

    private <T extends Serializable> CompletableFuture<T> send(Serializable msg) {
        return endpoint.getTransport().send(new RdmaMsg(msg)).thenApply(rdmaMsg -> (T) rdmaMsg.getPayload());
    }

    private void connect(int timeout) throws Exception {
        endpoint.connect(address, timeout);

    }

    private void close() throws IOException, InterruptedException {
        //close everything
        endpoint.close();
        DiSNILogger.getLogger().info("endpoint closed");
        factory.close();
        DiSNILogger.getLogger().info("group closed");
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Client simpleClient = new Client();
        simpleClient.run();
    }

}

