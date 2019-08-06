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


package org.openspaces.pu.container.jee.context;

import com.gigaspaces.internal.dump.InternalDumpProcessor;
import com.gigaspaces.internal.utils.ClassLoaderUtils;
import com.gigaspaces.start.SystemBoot;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jini.rio.boot.SharedServiceData;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.MemberAliveIndicator;
import org.openspaces.core.cluster.ProcessingUnitUndeployingListener;
import org.openspaces.core.properties.BeanLevelProperties;
import org.openspaces.pu.container.ProcessingUnitContainerConfig;
import org.openspaces.pu.container.jee.JeeProcessingUnitContainerProvider;
import org.openspaces.pu.container.jee.stats.RequestStatisticsFilter;
import org.openspaces.pu.container.spi.ApplicationContextProcessingUnitContainerProvider;
import org.openspaces.pu.container.support.ResourceApplicationContext;
import org.openspaces.pu.service.ServiceDetails;
import org.openspaces.pu.service.ServiceDetailsProvider;
import org.openspaces.pu.service.ServiceMonitors;
import org.openspaces.pu.service.ServiceMonitorsProvider;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Bootstap servlet context listener allowing to get the {@link org.openspaces.core.cluster.ClusterInfo},
 * {@link org.openspaces.core.properties.BeanLevelProperties}, and handle an optional pu.xml file
 * within META-INF/spring by loading it. <p/> <p>The different web containers support in OpenSpaces
 * will use {@link #prepareForBoot(java.io.File, org.openspaces.core.cluster.ClusterInfo,
 * org.openspaces.core.properties.BeanLevelProperties)} before the web application is started. It
 * will basically marshall the ClusterInfo and BeanLevelProperties so they can be read when the
 * {@link #contextInitialized(javax.servlet.ServletContextEvent)} is called. It will also change the
 * web.xml in order to add this class as a context listener. <p/> <p>During context initialization,
 * the marshalled ClusterInfo and BeanLevelProperties can be read and put in the servlet context
 * (allowing us to support any web container). <p/> <p>If there is a pu.xml file, it will be started
 * as well, with the application context itself set in the servlet context (can then be used by the
 * {@link org.openspaces.pu.container.jee.context.ProcessingUnitContextLoaderListener} as well as
 * all its beans read and set in the servlet context under their respective names.
 *
 * @author kimchy
 */
public class BootstrapWebApplicationContextListener implements ServletContextListener {

    private static final Log logger = LogFactory.getLog(BootstrapWebApplicationContextListener.class);

    private static final String BOOTSTRAP_CONTEXT_KEY = BootstrapWebApplicationContextListener.class.getName() + ".bootstraped";

    private static final String MARSHALLED_STORE = "/WEB-INF/gsstore";
    private static final String MARSHALLED_CLUSTER_INFO = MARSHALLED_STORE + "/cluster-info";
    private static final String MARSHALLED_BEAN_LEVEL_PROPERTIES = MARSHALLED_STORE + "/bean-level-properties";

    private volatile ServletContextListener jeeContainerContextListener;

    public void contextInitialized(ServletContextEvent servletContextEvent) {
        ServletContext servletContext = servletContextEvent.getServletContext();

        Boolean bootstraped = (Boolean) servletContext.getAttribute(BOOTSTRAP_CONTEXT_KEY);
        if (bootstraped != null && bootstraped) {
            logger.debug("Already performed bootstrap, ignoring");
            return;
        }
        servletContext.setAttribute(BOOTSTRAP_CONTEXT_KEY, true);

        logger.info("Booting OpenSpaces Web Application Support");
        if (logger.isDebugEnabled())
            logger.debug(ClassLoaderUtils.getCurrentClassPathString("Web Class Loader"));
        final ProcessingUnitContainerConfig config = new ProcessingUnitContainerConfig();

        InputStream is = servletContext.getResourceAsStream(MARSHALLED_CLUSTER_INFO);
        if (is != null) {
            try {
                config.setClusterInfo((ClusterInfo) objectFromByteBuffer(FileCopyUtils.copyToByteArray(is)));
                servletContext.setAttribute(JeeProcessingUnitContainerProvider.CLUSTER_INFO_CONTEXT, config.getClusterInfo());
            } catch (Exception e) {
                logger.warn("Failed to read cluster info from " + MARSHALLED_CLUSTER_INFO, e);
            }
        } else {
            logger.debug("No cluster info found at " + MARSHALLED_CLUSTER_INFO);
        }
        is = servletContext.getResourceAsStream(MARSHALLED_BEAN_LEVEL_PROPERTIES);
        if (is != null) {
            try {
                config.setBeanLevelProperties((BeanLevelProperties) objectFromByteBuffer(FileCopyUtils.copyToByteArray(is)));
                servletContext.setAttribute(JeeProcessingUnitContainerProvider.BEAN_LEVEL_PROPERTIES_CONTEXT, config.getBeanLevelProperties());
            } catch (Exception e) {
                logger.warn("Failed to read bean level properties from " + MARSHALLED_BEAN_LEVEL_PROPERTIES, e);
            }
        } else {
            logger.debug("No bean level properties found at " + MARSHALLED_BEAN_LEVEL_PROPERTIES);
        }

        Resource resource = null;
        String realPath = servletContext.getRealPath("/META-INF/spring/pu.xml");
        if (realPath != null) {
            resource = new FileSystemResource(realPath);
        }
        if (resource != null && resource.exists()) {
            logger.debug("Loading [" + resource + "]");
            // create the Spring application context
            final ResourceApplicationContext applicationContext = new ResourceApplicationContext(new Resource[]{resource},
                    null, config);
            // "start" the application context
            applicationContext.refresh();

            servletContext.setAttribute(JeeProcessingUnitContainerProvider.APPLICATION_CONTEXT_CONTEXT, applicationContext);
            String[] beanNames = applicationContext.getBeanDefinitionNames();
            for (String beanName : beanNames) {
                if (applicationContext.getType(beanName) != null)
                    servletContext.setAttribute(beanName, applicationContext.getBean(beanName));
            }

            if (config.getClusterInfo() != null && SystemBoot.isRunningWithinGSC()) {
                final String key = config.getClusterInfo().getUniqueName();

                SharedServiceData.addServiceDetails(key, new Callable() {
                    public Object call() throws Exception {
                        ArrayList<ServiceDetails> serviceDetails = new ArrayList<ServiceDetails>();
                        Map map = applicationContext.getBeansOfType(ServiceDetailsProvider.class);
                        for (Iterator it = map.values().iterator(); it.hasNext(); ) {
                            ServiceDetails[] details = ((ServiceDetailsProvider) it.next()).getServicesDetails();
                            if (details != null) {
                                for (ServiceDetails detail : details) {
                                    serviceDetails.add(detail);
                                }
                            }
                        }
                        return serviceDetails.toArray(new Object[serviceDetails.size()]);
                    }
                });

                SharedServiceData.addServiceMonitors(key, new Callable() {
                    public Object call() throws Exception {
                        ArrayList<ServiceMonitors> serviceMonitors = new ArrayList<ServiceMonitors>();
                        Map map = applicationContext.getBeansOfType(ServiceMonitorsProvider.class);
                        for (Iterator it = map.values().iterator(); it.hasNext(); ) {
                            ServiceMonitors[] monitors = ((ServiceMonitorsProvider) it.next()).getServicesMonitors();
                            if (monitors != null) {
                                for (ServiceMonitors monitor : monitors) {
                                    serviceMonitors.add(monitor);
                                }
                            }
                        }
                        return serviceMonitors.toArray(new Object[serviceMonitors.size()]);
                    }
                });

                Map map = applicationContext.getBeansOfType(MemberAliveIndicator.class);
                for (Iterator it = map.values().iterator(); it.hasNext(); ) {
                    final MemberAliveIndicator memberAliveIndicator = (MemberAliveIndicator) it.next();
                    if (memberAliveIndicator.isMemberAliveEnabled()) {
                        SharedServiceData.addMemberAliveIndicator(key, new Callable<Boolean>() {
                            public Boolean call() throws Exception {
                                return memberAliveIndicator.isAlive();
                            }
                        });
                    }
                }

                map = applicationContext.getBeansOfType(ProcessingUnitUndeployingListener.class);
                for (Iterator it = map.values().iterator(); it.hasNext(); ) {
                    final ProcessingUnitUndeployingListener listener = (ProcessingUnitUndeployingListener) it.next();
                    SharedServiceData.addUndeployingEventListener(key, new Callable() {
                        public Object call() throws Exception {
                            listener.processingUnitUndeploying();
                            return null;
                        }
                    });
                }

                map = applicationContext.getBeansOfType(InternalDumpProcessor.class);
                for (Iterator it = map.values().iterator(); it.hasNext(); ) {
                    SharedServiceData.addDumpProcessors(key, it.next());
                }
            }
        } else {
            logger.debug("No [" + ApplicationContextProcessingUnitContainerProvider.DEFAULT_PU_CONTEXT_LOCATION + "] to load");
        }

        // load jee specific context listener
        if (config.getBeanLevelProperties() != null) {
            String jeeContainer = JeeProcessingUnitContainerProvider.getJeeContainer(config.getBeanLevelProperties());
            String className = "org.openspaces.pu.container.jee." + jeeContainer + "." + StringUtils.capitalize(jeeContainer) + "WebApplicationContextListener";
            Class clazz = null;
            try {
                clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
            } catch (ClassNotFoundException e) {
                // no class, ignore
            }
            if (clazz != null) {
                try {
                    jeeContainerContextListener = (ServletContextListener) clazz.newInstance();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create JEE specific context listener [" + clazz.getName() + "]", e);
                }
                jeeContainerContextListener.contextInitialized(servletContextEvent);
            }
        }

        // set the class loader used so the service bean can use it
        if (config.getClusterInfo() != null && SystemBoot.isRunningWithinGSC()) {
            SharedServiceData.putWebAppClassLoader(config.getClusterInfo().getUniqueName(), Thread.currentThread().getContextClassLoader());
        }
    }

    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        if (jeeContainerContextListener != null) {
            jeeContainerContextListener.contextDestroyed(servletContextEvent);
        }
        ConfigurableApplicationContext applicationContext = (ConfigurableApplicationContext) servletContextEvent.getServletContext().getAttribute(JeeProcessingUnitContainerProvider.APPLICATION_CONTEXT_CONTEXT);
        if (applicationContext != null && applicationContext.isActive()) {
            applicationContext.close();
        }
    }

    /**
     * Changes the web.xml to include the bootstrap context listener and the request statistics
     * filter
     */
    public static void prepareForBoot(File warPath, ClusterInfo clusterInfo, BeanLevelProperties beanLevelProperties) throws Exception {
        File gsStore = new File(warPath, MARSHALLED_STORE);
        gsStore.mkdirs();
        if (clusterInfo != null) {
            FileCopyUtils.copy(objectToByteBuffer(clusterInfo), new File(warPath, MARSHALLED_CLUSTER_INFO));
        }
        if (beanLevelProperties != null) {
            FileCopyUtils.copy(objectToByteBuffer(beanLevelProperties), new File(warPath, MARSHALLED_BEAN_LEVEL_PROPERTIES));
        }

        new File(warPath, "/WEB-INF/web.xml").renameTo(new File(warPath, "/WEB-INF/web.xml.orig"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(warPath, "/WEB-INF/web.xml.orig"))));
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(warPath, "/WEB-INF/web.xml"), false))));
        String line;
        while ((line = reader.readLine()) != null) {
            line = line.replace("org.springframework.web.context.ContextLoaderListener", "org.openspaces.pu.container.jee.context.ProcessingUnitContextLoaderListener");
            if (line.indexOf("<web-app") != -1) {
                writer.println(line);
                if (line.indexOf('>') == -1) {
                    // the tag is not closed
                    while ((line = reader.readLine()) != null) {
                        if (line.indexOf('>') == -1) {
                            writer.println(line);
                        } else {
                            break;
                        }
                    }
                    writer.println(line);
                }
                // now append our context listener
                writer.println("<!-- GigaSpaces CHANGE START: Boot Listener -->");
                writer.println("<listener>");
                writer.println("    <listener-class>" + BootstrapWebApplicationContextListener.class.getName() + "</listener-class>");
                writer.println("</listener>");
                writer.println("<!-- GigaSpaces CHANGE END: Boot Listener -->");
                writer.println("<!-- GigaSpaces CHANGE START: Request Statistics Listener -->");
                writer.println("<filter>");
                writer.println("    <filter-name>gs-request-statistics</filter-name>");
                writer.println("    <filter-class>" + RequestStatisticsFilter.class.getName() + "</filter-class>");
                if( !Boolean.getBoolean( "com.gs.cancel-async-supported" ) ) {
                    writer.println("<async-supported>true</async-supported>");
                }
                writer.println("</filter>");
                writer.println("<filter-mapping>");
                writer.println("    <filter-name>gs-request-statistics</filter-name>");
                writer.println("    <url-pattern>/*</url-pattern>");
                writer.println("</filter-mapping>");
                writer.println("<!-- GigaSpaces CHANGE END: Request Statistics Listener -->");
            } else {
                writer.println(line);
            }
        }
        writer.close();
        reader.close();
    }

    public static Object objectFromByteBuffer(byte[] buffer) throws Exception {
        if (buffer == null)
            return null;

        ByteArrayInputStream inStream = new ByteArrayInputStream(buffer);
        ObjectInputStream in = new ObjectInputStream(inStream);
        Object retval = in.readObject();
        in.close();

        return retval;
    }

    public static byte[] objectToByteBuffer(Object obj) throws Exception {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(outStream);
        out.writeObject(obj);
        out.flush();
        byte[] result = outStream.toByteArray();
        out.close();
        outStream.close();

        return result;
    }
}
