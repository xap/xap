package com.gigaspaces.rdma;/*
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

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;


public class Client implements RdmaEndpointFactory<Client.CustomClientEndpoint> {
    private final InetSocketAddress address;
    private final CustomClientEndpoint endpoint;
    RdmaActiveEndpointGroup<CustomClientEndpoint> endpointGroup;
    private String host = "192.168.33.137";
    private int port = 8888;

    public Client() throws IOException {
        //create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the endpoint.dispatchCqEvent() method.
        endpointGroup = new RdmaActiveEndpointGroup<CustomClientEndpoint>(1000, false, 128, 4, 128);
        endpointGroup.init(this);
        //we have passed our own endpoint factory to the group, therefore new endpoints will be of type CustomClientEndpoint
        //let's create a new client endpoint
        endpoint = endpointGroup.createEndpoint();

        //connect to the server
        InetAddress ipAddress = InetAddress.getByName(host);
        address = new InetSocketAddress(ipAddress, port);
    }

    public Client.CustomClientEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
        return new CustomClientEndpoint(endpointGroup, idPriv, serverSide);
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
        endpointGroup.close();
        DiSNILogger.getLogger().info("group closed");
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Client simpleClient = new Client();
        simpleClient.run();
    }

    public static class CustomClientEndpoint extends RdmaActiveEndpoint {


        private ClientTransport transport;

        public CustomClientEndpoint(RdmaActiveEndpointGroup<CustomClientEndpoint> endpointGroup,
                                    RdmaCmId idPriv, boolean serverSide) throws IOException {
            super(endpointGroup, idPriv, serverSide);

        }

        //important: we override the init method to prepare some buffers (memory registration, post recv, etc).
        //This guarantees that at least one recv operation will be posted at the moment this endpoint is connected.
        public void init() throws IOException {
            super.init();

            transport = new ClientTransport(this);

        }


        public void dispatchCqEvent(IbvWC wc) throws IOException {
            DiSNILogger.getLogger().info("CLIENT: op code = " + IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) + ", id = " + wc.getWr_id());
            getTransport().onCompletionEvent(wc);
        }


        public ClientTransport getTransport() {
            return transport;
        }

    }

}

