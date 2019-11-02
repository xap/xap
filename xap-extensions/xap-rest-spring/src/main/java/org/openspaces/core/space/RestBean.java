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

package org.openspaces.core.space;

import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.start.SystemInfo;
import com.j_spaces.core.IJSpace;
import net.jini.core.discovery.LookupLocator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tomcat.InstanceManager;
import org.apache.tomcat.SimpleInstanceManager;
import org.eclipse.jetty.annotations.ServletContainerInitializersStarter;
import org.eclipse.jetty.apache.jsp.JettyJasperInitializer;
import org.eclipse.jetty.jsp.JettyJspServlet;
import org.eclipse.jetty.plus.annotation.ContainerInitializer;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.openspaces.pu.container.CannotCreateContainerException;
import org.openspaces.pu.container.jee.JeeServiceDetails;
import org.openspaces.pu.container.jee.JeeType;
import org.openspaces.pu.container.jee.stats.RequestStatisticsFilter;
import org.openspaces.pu.service.ServiceDetails;
import org.openspaces.pu.service.ServiceDetailsProvider;
import org.openspaces.pu.service.ServiceMonitors;
import org.openspaces.pu.service.ServiceMonitorsProvider;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import javax.servlet.DispatcherType;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;

/**
 * @author yohana
 * @since 10.1.0
 */
public class RestBean implements InitializingBean, ClusterInfoAware, DisposableBean, ServiceDetailsProvider, ServiceMonitorsProvider {
    protected Log logger = LogFactory.getLog(getClass());

    private Server server;

    private GigaSpace gigaspace;

    private String spaceName;

    private String groups;

    private String locators;

    private String port;

    private int jettyPort;

    private FilterHolder filterHolder;

    private ClusterInfo clusterInfo;

    private WebAppContext webAppContext;

    private boolean jettyStarted = false;

    private Properties properties;

    @Override
    public void destroy() {
        if (jettyStarted) {
            logger.info("Stopping rest service");
            try {
                webAppContext.stop();
            } catch (Exception e) {
                logger.error("Unable to stop web context", e);
            }
            try {
                server.stop();
            } catch (Exception e) {
                logger.error("Unable to stop rest service", e);
            }

            server.destroy();
        }
    }

    public void setPort(String port) {
        this.port = port;
    }

    public void setGigaSpace(GigaSpace gigaSpace) {
        this.gigaspace = gigaSpace;
    }

    public void setSpaceName(String spaceName) {
        this.spaceName = spaceName;
    }

    public void setGroups(String groups) {
        this.groups = groups;
    }

    public void setLocators(String locators) {
        this.locators = locators;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        int runningNumber = clusterInfo.getRunningNumber();
        try {
            jettyPort = Integer.valueOf(this.port);
        } catch (NumberFormatException e) {
            throw new CannotCreateContainerException("Port should be number");
        }
        jettyPort += runningNumber;
        server = new Server(jettyPort);
        server.setStopAtShutdown(true);
        server.setStopTimeout(1000);

        String ispaceName, igroups, ilocators;

        if (gigaspace == null && spaceName == null) {
            throw new CannotCreateContainerException("Either giga-space or space-name attribute should be specified.");
        }
        if (gigaspace != null && spaceName != null) {
            throw new CannotCreateContainerException("Either giga-space or space-name attribute can be specified but not both.");
        }

        if (spaceName != null) {
            ispaceName = spaceName;
            //TODO validate groups and locators ?
            igroups = (groups == null ? null : groups);
            ilocators = (locators == null ? null : locators);
        } else {
            IJSpace space = gigaspace.getSpace();
            ispaceName = space.getName();
            String[] lookupgroups = space.getFinderURL().getLookupGroups();
            if (lookupgroups == null || lookupgroups.length == 0) {
                igroups = null;
            } else {
                igroups = StringUtils.join(lookupgroups, ",", 0, lookupgroups.length);
            }

            LookupLocator[] lookuplocators = space.getFinderURL().getLookupLocators();
            if (lookuplocators == null || lookuplocators.length == 0) {
                ilocators = null;
            } else {
                ilocators = "";
                for (int i = 0; i < lookuplocators.length; i++) {
                    ilocators += lookuplocators[i].getHost() + ":" + lookuplocators[i].getPort();
                    if (i != (lookuplocators.length - 1)) {
                        ilocators += ",";
                    }
                }
            }
        }

        logger.info("Starting REST service on port [" + jettyPort + "]");
        webAppContext = new WebAppContext();
        filterHolder = new FilterHolder(RequestStatisticsFilter.class);
        webAppContext.addFilter(filterHolder, "/*", EnumSet.of(DispatcherType.INCLUDE, DispatcherType.REQUEST));
        webAppContext.setContextPath("/");
        webAppContext.setWar(SystemInfo.singleton().locations().libOptional("rest").resolve("xap-rest.war").toString());
        webAppContext.setInitParameter("port", port);
        webAppContext.setInitParameter("spaceName", ispaceName);
        if (igroups != null && !igroups.equalsIgnoreCase("null")) {
            logger.debug("Applying groups " + igroups);
            webAppContext.setInitParameter("lookupGroups", igroups);
        }

        if (ilocators != null && !ilocators.equalsIgnoreCase("null")) {
            logger.debug("Applying locators " + ilocators);
            webAppContext.setInitParameter("lookupLocators", ilocators);
        }

        if (properties != null && properties.getProperty("datetime_format") != null) {
            webAppContext.setInitParameter("datetime_format", properties.getProperty("datetime_format"));
        }

        webAppContext.setCopyWebDir(false);
        webAppContext.setParentLoaderPriority(true);

        webAppContext.setAttribute("org.eclipse.jetty.server.webapp.ContainerIncludeJarPattern",
                ".*/[^/]*servlet-api-[^/]*\\.jar$|.*/javax.servlet.jsp.jstl-.*\\.jar$|.*/.*taglibs*\\.jar$");

        webAppContext.setAttribute("org.eclipse.jetty.containerInitializers", jspInitializers());

        webAppContext.setAttribute(InstanceManager.class.getName(), new SimpleInstanceManager());

        webAppContext.addBean(new ServletContainerInitializersStarter(webAppContext), true);

        webAppContext.addServlet(jspServletHolder(), "*.jsp");


        server.setHandler(webAppContext);
        try {
            server.start();
            jettyStarted = true;
            logger.info("REST service is started");
        } catch (Exception e) {
            logger.error("Unable to start rest service on port [" + jettyPort + "]", e);
            throw new CannotCreateContainerException("Unable to start rest server on port [" + jettyPort + "]", e);
        }
    }

    @Override
    public ServiceDetails[] getServicesDetails() {
        final String host = SystemInfo.singleton().network().getHostId();
        JeeServiceDetails details = new JeeServiceDetails(host, jettyPort, 0, "/", false, "jetty", JeeType.CUSTOM, 0);
        return new ServiceDetails[]{details};
    }

    @Override
    public ServiceMonitors[] getServicesMonitors() {
        if (jettyStarted && filterHolder != null) {
            RequestStatisticsFilter filter = ((RequestStatisticsFilter) filterHolder.getFilter());
            if (filter == null) {
                logger.debug("Unable to find a running Filter");
                return new ServiceMonitors[0];
            } else {
                return filter.getServicesMonitors();
            }
        } else {
            return new ServiceMonitors[0];
        }
    }

    @Override
    public void setClusterInfo(ClusterInfo clusterInfo) {
        this.clusterInfo = clusterInfo;
    }

    /**
     * Create JSP Servlet (must be named "jsp")
     */
    private ServletHolder jspServletHolder()
    {
        ServletHolder holderJsp = new ServletHolder("jsp", JettyJspServlet.class);
        holderJsp.setInitOrder(0);
        holderJsp.setInitParameter("logVerbosityLevel", "DEBUG");
        holderJsp.setInitParameter("fork", "false");
        holderJsp.setInitParameter("xpoweredBy", "false");
        holderJsp.setInitParameter("compilerTargetVM", "1.7");
        holderJsp.setInitParameter("compilerSourceVM", "1.7");
        holderJsp.setInitParameter("keepgenerated", "true");
        return holderJsp;
    }

    /**
     * Ensure the jsp engine is initialized correctly
     */
    private List<ContainerInitializer> jspInitializers()
    {
        JettyJasperInitializer sci = new JettyJasperInitializer();
        ContainerInitializer initializer = new ContainerInitializer(sci, null);
        List<ContainerInitializer> initializers = new ArrayList<ContainerInitializer>();
        initializers.add(initializer);
        return initializers;
    }
}
