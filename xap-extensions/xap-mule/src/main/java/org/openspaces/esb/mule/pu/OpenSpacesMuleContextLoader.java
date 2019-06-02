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


package org.openspaces.esb.mule.pu;

import org.mule.api.MuleContext;
import org.mule.api.context.MuleContextFactory;
import org.mule.config.ConfigResource;
import org.mule.config.spring.GSSpringXmlConfigurationBuilder;
import org.mule.config.spring.OptionalObjectsController;
import org.mule.context.DefaultMuleContextFactory;
import org.openspaces.pu.service.ServiceDetails;
import org.openspaces.pu.service.ServiceDetailsProvider;
import org.openspaces.pu.service.ServiceMonitors;
import org.openspaces.pu.service.ServiceMonitorsProvider;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.support.AbstractApplicationContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * <code> OpenSpacesMuleContextLoader</code> used for loading Mule configuration that refrenced from
 * PU configuration file.
 *
 * <p>It sets the PU appliction context as the parent of Mule appliction context, giving it the
 * ability to access beans that declerd in the PU appliction context.
 *
 * @author yitzhaki
 */
public class OpenSpacesMuleContextLoader implements ApplicationContextAware, InitializingBean, DisposableBean, ApplicationListener,
        ServiceDetailsProvider, ServiceMonitorsProvider {

    private static final String DEFAULT_LOCATION = "META-INF/spring/mule.xml";

    private String location;

    private ApplicationContext applicationContext;

    private MuleContextFactory muleContextFactory;

    private AbstractApplicationContext muleApplicationContext;

    private MuleContext muleContext;

    private volatile boolean contextCreated = false;

    public OpenSpacesMuleContextLoader() {
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }


    public void afterPropertiesSet() throws Exception {
        if (this.location == null) {
            this.location = DEFAULT_LOCATION;
        }
    }

    public void onApplicationEvent(ApplicationEvent event) {
        if (event instanceof ContextRefreshedEvent) {
            if (!contextCreated) {
                contextCreated = true;
                try {
                    muleContextFactory = new DefaultMuleContextFactory();
                    GSSpringXmlConfigurationBuilder muleXmlConfigurationBuilder = new GSSpringXmlConfigurationBuilder(location) {

                        @Override
                        protected ApplicationContext doCreateApplicationContext(MuleContext muleContext, ConfigResource[] configResources, OptionalObjectsController optionalObjectsController) {
                            AbstractApplicationContext context = (AbstractApplicationContext) super.doCreateApplicationContext(muleContext, configResources, optionalObjectsController);
                            context.setParent(applicationContext);
                            muleApplicationContext = context;
                            return context;
                        }
                    };
                    muleXmlConfigurationBuilder.setParentContext(this.applicationContext);
                    muleContext = muleContextFactory.createMuleContext(muleXmlConfigurationBuilder);
                    muleContext.start();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to start mule [" + location + "]", e);
                }
            }
        }
    }

    public void destroy() throws Exception {
        if (muleContext != null) {
            try {
                // set the parent to null, so the close context event won't be fired to the parent as well...
                muleApplicationContext.setParent(null);
                muleContext.dispose();
            } finally {
                muleContext = null;
                muleApplicationContext = null;

                //Haven't found a route for this in new api
                //MuleServer.setMuleContext(null);
            }
        }
    }

    public ServiceDetails[] getServicesDetails() {
        ArrayList<Object> serviceDetails = new ArrayList<Object>();
        if (muleApplicationContext != null) {
            Map map = muleApplicationContext.getBeansOfType(ServiceDetailsProvider.class);
            for (Iterator it = map.values().iterator(); it.hasNext(); ) {
                ServiceDetails[] details = ((ServiceDetailsProvider) it.next()).getServicesDetails();
                if (details != null) {
                    for (ServiceDetails detail : details) {
                        serviceDetails.add(detail);
                    }
                }
            }
        }
        return serviceDetails.toArray(new ServiceDetails[serviceDetails.size()]);
    }

    public ServiceMonitors[] getServicesMonitors() {
        ArrayList<ServiceMonitors> serviceMonitors = new ArrayList<ServiceMonitors>();
        if (muleApplicationContext != null) {
            Map map = muleApplicationContext.getBeansOfType(ServiceMonitorsProvider.class);
            for (Iterator it = map.values().iterator(); it.hasNext(); ) {
                ServiceMonitors[] monitors = ((ServiceMonitorsProvider) it.next()).getServicesMonitors();
                if (monitors != null) {
                    for (ServiceMonitors monitor : monitors) {
                        serviceMonitors.add(monitor);
                    }
                }
            }
        }
        return serviceMonitors.toArray(new ServiceMonitors[serviceMonitors.size()]);
    }
}
