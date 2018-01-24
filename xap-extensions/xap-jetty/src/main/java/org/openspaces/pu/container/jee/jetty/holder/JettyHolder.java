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


package org.openspaces.pu.container.jee.jetty.holder;

import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.NetworkConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.MultiException;

import java.util.Collection;

/**
 * A generic holder that holds a Jetty server and controls its lifecycle. Note, make not to call
 * start and stop on {@link Server}.
 *
 * @author kimchy
 */
public abstract class JettyHolder {

    /**
     * Returns the jetty server.
     */
    public abstract Server getServer();

    /**
     * Returns <code>true</code> if this is a single instance.
     */
    public abstract boolean isSingleInstance();

    /**
     * Open Jetty ports.
     */
    public void openConnectors() throws Exception {
        Server server = getServer();
        Connector[] connectors = server.getConnectors();
        for (Connector c : connectors) {
            if (c instanceof NetworkConnector)
                ((NetworkConnector) c).open();
        }

    }

    /**
     * Closes Jetty ports.
     */
    public void closeConnectors() throws Exception {
        Server server = getServer();
        Connector[] connectors = server.getConnectors();
        MultiException ex = new MultiException();
        for (Connector c : connectors) {
            try {
                if (c instanceof NetworkConnector)
                    ((NetworkConnector) c).close();
            } catch (Exception e) {
                ex.add(e);
            }
        }
        ex.ifExceptionThrowMulti();

    }

    /**
     * Start Jetty. Note, if this fails, make sure to call {@link #stop()}
     */
    public void start() throws Exception {
        Server server = getServer();
        server.start();
    }

    /**
     * Stops Jetty.
     */
    public void stop() throws Exception {
        Server server = getServer();
        server.stop();
        server.destroy();
    }

    public void updateConfidentialPort(int port, int newPort) {

        Server server = getServer();
        for (Connector connector : server.getConnectors()) {
            HttpConfiguration config = getHttpConfig(connector);
            if (config != null && config.getSecurePort() == port) {
                // if the confidential port of one connectors points to this connector that we are changing its port
                // then update it as well
                config.setSecurePort(newPort);
            }
        }
    }

    public static int getConfidentialPort(Connector connector) {
        //return connector.getConfidentialPort();
        HttpConfiguration config = getHttpConfig(connector);
        return config != null ? config.getSecurePort() : -1;
    }

    public static HttpConfiguration getHttpConfig(Connector connector) {
        HttpConnectionFactory connectionFactory = connector.getConnectionFactory(HttpConnectionFactory.class);
        return connectionFactory != null ? connectionFactory.getHttpConfiguration() : null;
    }

    public static int getSslPort(Connector connector) {
        HttpConfiguration config = getHttpConfig(connector);
        return config != null ? config.getSecurePort() : -1;
    }

    public static int getSslConnectionPort(Connector connector) {
        int sslPort = 0;
        Collection<ConnectionFactory> connectionFactories = connector.getConnectionFactories();
        for( ConnectionFactory connectionFactory : connectionFactories ){
            //only in the case of SslConnectionFactory usage retrieve secure port
            if( connectionFactory instanceof SslConnectionFactory){
                sslPort = getSslPort( connector );
                break;
            }
        }

        return sslPort;
    }
}
