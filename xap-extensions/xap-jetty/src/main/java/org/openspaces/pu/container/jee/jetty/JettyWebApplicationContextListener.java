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

import com.j_spaces.core.IJSpace;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.session.DefaultSessionIdManager;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.component.LifeCycle;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.properties.BeanLevelProperties;
import org.openspaces.core.space.UrlSpaceConfigurer;
import org.openspaces.pu.container.jee.JeeProcessingUnitContainerProvider;
import org.openspaces.pu.container.jee.jetty.session.SpaceSessionDataStore;
import org.springframework.context.ApplicationContext;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.function.Consumer;

/**
 * An jetty specific {@link javax.servlet.ServletContextListener} that is automatically loaded by
 * the {@link org.openspaces.pu.container.jee.context.BootstrapWebApplicationContextListener}. <p/>
 * <p>Support specific GigaSpace based session storge when using the
 * <code>jetty.sessions.spaceUrl</code> parameter within the (web) processing unit properties. It is
 * handled here since we want to setup the session support under the web application class loader
 * and not under the class loader that starts up jetty.
 *
 * @author kimchy
 */
public class JettyWebApplicationContextListener implements ServletContextListener {

    private static final Log logger = LogFactory.getLog(JettyWebApplicationContextListener.class);

    /**
     * A deploy property that controls if Jetty will store the session on the Space. Just by
     * specifying the url it will automatically enable it.
     */
    public static final String JETTY_SESSIONS_URL = "jetty.sessions.spaceUrl";

    /**
     * The lease of the SessionData that is written to the
     * Space. Set in <b>seconds</b> and defaults to FOREVER.
     */
    public static final String JETTY_SESSIONS_LEASE = "jetty.sessions.lease";

    public void contextInitialized(ServletContextEvent servletContextEvent) {
        final ServletContext servletContext = servletContextEvent.getServletContext();
        // a hack to get the jetty context
        final ServletContextHandler jettyContext = (ServletContextHandler) ((ContextHandler.Context) servletContext).getContextHandler();
        final SessionHandler sessionHandler = jettyContext.getSessionHandler();
        BeanLevelProperties beanLevelProperties = (BeanLevelProperties) servletContext.getAttribute(JeeProcessingUnitContainerProvider.BEAN_LEVEL_PROPERTIES_CONTEXT);
        ClusterInfo clusterInfo = (ClusterInfo) servletContext.getAttribute(JeeProcessingUnitContainerProvider.CLUSTER_INFO_CONTEXT);
        if (beanLevelProperties != null) {
            replaceWorkerName(sessionHandler, clusterInfo.getUniqueName().replace('.', '_'));

            // automatically enable GigaSpaces Session Manager when passing the relevant property
            String sessionsSpaceUrl = beanLevelProperties.getContextProperties().getProperty(JETTY_SESSIONS_URL);
            if (sessionsSpaceUrl != null) {
                logger.info("Jetty GigaSpaces Session Data Store support using space url [" + sessionsSpaceUrl + "]");
                GigaSpace space = getSpace(sessionsSpaceUrl, servletContext, clusterInfo);
                initDataSessionStore(sessionsSpaceUrl, sessionHandler, space, beanLevelProperties);
            }
        }
    }

    private GigaSpace getSpace(String sessionsSpaceUrl, ServletContext servletContext, ClusterInfo clusterInfo) {
        GigaSpace space;
        if (sessionsSpaceUrl.startsWith("bean://")) {
            ApplicationContext applicationContext = (ApplicationContext) servletContext.getAttribute(JeeProcessingUnitContainerProvider.APPLICATION_CONTEXT_CONTEXT);
            if (applicationContext == null) {
                throw new IllegalStateException("Failed to find servlet context bound application context");
            }
            Object bean = applicationContext.getBean(sessionsSpaceUrl.substring("bean://".length()));
            if (bean instanceof GigaSpace) {
                space = (GigaSpace) bean;
            } else if (bean instanceof IJSpace) {
                space = new GigaSpaceConfigurer((IJSpace) bean).create();
            } else {
                throw new IllegalArgumentException("Bean [" + bean + "] is not of either GigaSpace type or IJSpace type");
            }
        } else {
            space = new GigaSpaceConfigurer(new UrlSpaceConfigurer(sessionsSpaceUrl).clusterInfo(clusterInfo)).create();
        }
        return space;
    }

    private void initDataSessionStore(String sessionsSpaceUrl, SessionHandler sessionHandler, GigaSpace space,
                                      BeanLevelProperties beanLevelProperties) {
        final SpaceSessionDataStore spaceSessionDataStore = new SpaceSessionDataStore(space);

        String lease = beanLevelProperties.getContextProperties().getProperty(JETTY_SESSIONS_LEASE);
        if (lease != null) {
            spaceSessionDataStore.setLease(Long.parseLong(lease));
            if (logger.isDebugEnabled()) {
                logger.debug("Setting lease to [" + lease + "] milliseconds");
            }
        }

        restart(sessionHandler.getSessionCache(), sc -> sc.setSessionDataStore(spaceSessionDataStore), "Replacing default session data store");
    }

    private void replaceWorkerName(SessionHandler sessionHandler, String newWorkerName) {
        // if we have a default session id manager, set its worker name automatically...
        if (sessionHandler.getSessionIdManager() instanceof DefaultSessionIdManager) {
            DefaultSessionIdManager sessionIdManager = (DefaultSessionIdManager) sessionHandler.getSessionIdManager();
            String currWorkerName = sessionIdManager.getWorkerName();
            if (currWorkerName == null || currWorkerName.equals("node0")) {
                if (logger.isInfoEnabled()) {
                    logger.info("Modifying worker name from [" + currWorkerName + "] to [" + newWorkerName + "]");
                }
                restart(sessionIdManager, sim -> sessionIdManager.setWorkerName(newWorkerName), "Modify worker name");
            }
        }
    }

    public void contextDestroyed(ServletContextEvent servletContextEvent) {
    }

    private static <T extends LifeCycle> void restart(T instance, Consumer<T> action, String desc) {
        if (logger.isInfoEnabled()) {
            logger.info("Stopping " + instance.toString() + " - " + desc);
        }
        try {
            instance.stop();
        } catch (Exception e) {
            throw new RuntimeException("Failed to stop " + instance.toString() + " " + desc, e);
        }

        action.accept(instance);

        if (logger.isInfoEnabled()) {
            logger.info("Restarting " + instance.toString() + " - " + desc);
        }
        try {
            instance.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to restart " + instance.toString() + " " + desc, e);
        }
    }
}
