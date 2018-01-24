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


package org.openspaces.pu.container.jee.jetty;

import com.gigaspaces.start.SystemInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.NetworkConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.webapp.WebAppContext;
import org.jini.rio.boot.BootUtil;
import org.openspaces.pu.container.CannotCloseContainerException;
import org.openspaces.pu.container.jee.JeeServiceDetails;
import org.openspaces.pu.container.jee.JeeType;
import org.openspaces.pu.container.jee.jetty.holder.JettyHolder;
import org.openspaces.pu.container.jee.jetty.support.FreePortGenerator;
import org.openspaces.pu.service.ServiceDetails;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.context.ContextLoader;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * The actual container simply holding the jetty web application context, the application context,
 * and the {@link org.openspaces.pu.container.jee.jetty.holder.JettyHolder}. They are used when
 * closing this container.
 *
 * @author kimchy
 */
public class JettyProcessingUnitContainer extends org.openspaces.pu.container.jee.JeeProcessingUnitContainer {

    private static final Log logger = LogFactory.getLog(JettyProcessingUnitContainer.class);

    private final ApplicationContext applicationContext;

    private ApplicationContext webApplicationContext;

    private final WebAppContext webAppContext;

    private ContextHandlerCollection contextHandlerCollection;

    private final JettyHolder jettyHolder;

    private final List<FreePortGenerator.PortHandle> portHandels;

    public JettyProcessingUnitContainer(ApplicationContext applicationContext, WebAppContext webAppContext,
                                        ContextHandlerCollection contextHandlerCollection, JettyHolder jettyHolder,
                                        List<FreePortGenerator.PortHandle> portHandels) {
        this.applicationContext = applicationContext;
        this.webAppContext = webAppContext;
        this.contextHandlerCollection = contextHandlerCollection;
        this.jettyHolder = jettyHolder;
        this.webApplicationContext = ContextLoader.getCurrentWebApplicationContext();
        if (webApplicationContext == null) {
            webApplicationContext = applicationContext;
        }
        this.portHandels = portHandels;
    }

    /**
     * Returns the spring application context this processing unit container wraps.
     */
    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public ServiceDetails[] getServicesDetails() {
        return new ServiceDetails[]{getJeeDetails()};
    }

    public JeeServiceDetails getJeeDetails() {
        String hostAddress = null;
        int port = -1;
        int sslPort = -1;
        int sslConnectionPort = -1;

        Connector connector = jettyHolder.getServer().getConnectors()[0];
        if (connector instanceof NetworkConnector) {
            NetworkConnector networkConnector = (NetworkConnector) connector;
            port = networkConnector.getPort();
            sslPort = JettyHolder.getConfidentialPort(connector);
            sslConnectionPort = JettyHolder.getSslConnectionPort(connector);
            String host = networkConnector.getHost();
            if (host == null)
                host = SystemInfo.singleton().network().getHostId();
            InetSocketAddress addr = host == null ? new InetSocketAddress(port) : new InetSocketAddress(host, port);
            hostAddress = addr.getAddress().getHostAddress();

        }
        JeeServiceDetails details = new JeeServiceDetails(hostAddress, port, sslPort,
                webAppContext.getContextPath(),
                jettyHolder.isSingleInstance(),
                "jetty",
                JeeType.JETTY,
                sslConnectionPort);
        return details;
    }

    /**
     * Closes the processing unit container by destroying the web application and the Spring
     * application context.
     */
    @Override
    public void close() throws CannotCloseContainerException {

        for (FreePortGenerator.PortHandle portHandle : portHandels) {
            portHandle.release();
        }
        portHandels.clear();

        if (webAppContext.isRunning()) {
            try {
                webAppContext.stop();
                webAppContext.destroy();
            } catch (Exception e) {
                logger.warn("Failed to stop/destroy web context", e);
            } finally {
                webAppContext.setClassLoader(null);
            }

            if (contextHandlerCollection != null) {
                contextHandlerCollection.removeHandler(webAppContext);
            }
        }

        // close the application context anyhow (it might be closed by the webapp context, but it
        // might not if it is not a pure Spring application).
        ConfigurableApplicationContext confAppContext = (ConfigurableApplicationContext) applicationContext;
        confAppContext.close();

        try {
            jettyHolder.stop();
        } catch (Exception e) {
            logger.warn("Failed to stop jetty server", e);
        }

        super.close();
    }

}