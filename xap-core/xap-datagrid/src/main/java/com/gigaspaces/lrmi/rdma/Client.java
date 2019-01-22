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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;


public class Client {

    private static String host = "192.168.33.137";
    private static int port = 8888;

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        GSRdmaEndpointFactory factory = new GSRdmaEndpointFactory();

        //we have passed our own endpoint factory to the group, therefore new endpoints will be of type GSRdmaClientEndpoint
        //let's create a new client endpoint
        GSRdmaClientEndpoint endpoint = factory.create();

        //connect to the server
        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress address = new InetSocketAddress(ipAddress, port);

        try {
            endpoint.connect(address, 1000);
            DiSNILogger.getLogger().info("::client channel set up ");

            CompletableFuture<String> future = endpoint.getTransport().send("i am the client");
            String respond = future.get();
            DiSNILogger.getLogger().info(respond);

            CompletableFuture<String> future1 = endpoint.getTransport().send("i am connected");
            respond = future1.get();
            DiSNILogger.getLogger().info(respond);

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

}

