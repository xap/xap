/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gigaspaces.metrics.influxdb;

import com.gigaspaces.metrics.UdpConnection;

import java.io.IOException;
import java.net.SocketException;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 10.2.1
 */
public class InfluxDBUdpDispatcher extends InfluxDBDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(InfluxDBUdpDispatcher.class.getName());
    private final UdpConnection connection;

    public InfluxDBUdpDispatcher(InfluxDBReporterFactory factory) {
        try {
            this.connection = new UdpConnection(factory.getHost(), factory.getPort(), Charset.forName("utf-8"));
        } catch (SocketException e) {
            throw new RuntimeException("Failed to create InfluxDBUdpDispatcher", e);
        }
            logger.info("InfluxDBUdpDispatcher created [host=" + factory.getHost() +
                    ", port=" + factory.getPort() + "]");
    }

    public UdpConnection getConnection() {
        return connection;
    }

    @Override
    protected void doSend(String content) throws IOException {
        connection.send(content);
    }

    @Override
    public void close() {
        connection.close();
        super.close();
    }
}
