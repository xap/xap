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


package org.openspaces.core.jini;

import com.j_spaces.core.jini.SharedDiscoveryManagement;

import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.LookupDiscovery;
import net.jini.discovery.LookupGroups;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.entry.Name;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.config.AbstractFactoryBean;

import java.lang.reflect.InvocationTargetException;

/**
 * JiniServiceFactoryBean for Jini environments. The class is made up from various samples found on
 * jini.org customized in a Spring specific way. The search will be executed using the provided
 * ServiceTemplate or, if it is null, one will be created using the serviceClass and serviceName. If
 * the lookup operation times out (30 seconds by default), a null service will be returned. For most
 * cases the serviceClass and serviceNames are enough and hide the jini details from the client.
 *
 * <p>The factoryBean can be configured to do a lookup each time before returning the object type by
 * setting the "singleton" property to false.
 *
 * <p>The service factory can be configured to return a smart proxy that will try and perform
 * another lookup in case of an invocation exception (see {@link #setSmartProxy(boolean)}. The retry
 * count can be controlled using {@link #setRetryCountOnFailure(int)}.
 *
 * @author kimchy
 */
public class JiniServiceFactoryBean extends AbstractFactoryBean implements MethodInterceptor {

    private static final Log logger = LogFactory.getLog(JiniServiceFactoryBean.class);

    private ServiceTemplate template;

    // utility properties
    private Class<?> serviceClass;
    private String serviceName;

    private String[] groups = LookupGroups.all();

    private String[] locators = null;

    // 30 secs
    private long timeout = 30 * 1000;
    // used to pass out information from inner classes

    private volatile Object actualService;

    private final Object actualServiceMonitor = new Object();

    private boolean smartProxy = false;

    private int retryCountOnFailure = 3;

    @Override
    public Class<?> getObjectType() {
        // try to discover the class type if possible to make it work with autowiring
        if (actualService == null) {
            // no template - look at serviceClass
            if (template == null) {
                return (serviceClass == null ? null : serviceClass);
            }

            // look at the template and
            // return the first class from the template (if there is one)
            if (template.serviceTypes != null && template.serviceTypes.length > 0) {
                return template.serviceTypes[0];
            }
        }
        if (actualService == null) {
            throw new IllegalArgumentException("Failed to identify factory class type");
        }
        return actualService.getClass();
    }

    /**
     * Creates an instance of the service. Performs a lookup (using {@link #lookupService()} and if
     * smart proxy is used, will wrap the returned service with a proxy that performs lookups in
     * case of failures.
     */
    @Override
    protected Object createInstance() throws Exception {
        synchronized (actualServiceMonitor) {
            actualService = lookupService();
        }
        if (!smartProxy) {
            return actualService;
        }
        ProxyFactory proxyFactory = new ProxyFactory(actualService);
        proxyFactory.addAdvice(this);
        return proxyFactory.getProxy();
    }

    /**
     * When using smart proxy, wraps the invocation of a service method and in case of failure will
     * try and perform another lookup for the service.
     */
    public Object invoke(MethodInvocation methodInvocation) throws Throwable {
        int retries = retryCountOnFailure;
        while (true) {
            try {
                if (actualService == null) {
                    synchronized (actualServiceMonitor) {
                        if (actualService == null) {
                            actualService = lookupService();
                        }
                    }
                }
                return methodInvocation.getMethod().invoke(actualService, methodInvocation.getArguments());
            } catch (InvocationTargetException e) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Failed to execute [" + methodInvocation.getMethod().getName() + "] on [" + actualService + "]", e);
                }
                synchronized (actualServiceMonitor) {
                    actualService = null;
                }
                // we have an invocation exception, if we got to the reties
                // throw it, if not, lookup another service and try it
                if (--retries == 0) {
                    throw e.getTargetException();
                }
            }
        }
    }

    /**
     * A helper method to lookup the service.
     */
    protected Object lookupService() throws Exception {
        Object service = null;

        ServiceTemplate templ;
        if (template == null) {
            Class<?>[] types = (serviceClass == null ? null : new Class[]{serviceClass});
            Entry[] entry = (serviceName == null ? null : new Entry[]{new Name(serviceName)});

            templ = new ServiceTemplate(null, types, entry);
        } else {
            templ = template;
        }

        LookupLocator[] lookupLocators = null;
        if (locators != null) {
            lookupLocators = new LookupLocator[locators.length];
            for (int i = 0; i < locators.length; i++) {
                String locator = locators[i];
                if (!locator.startsWith("jini://")) {
                    locator = "jini://" + locator;
                }
                lookupLocators[i] = new LookupLocator(locator);
            }
        }
        ServiceDiscoveryManager serviceDiscovery = null;
        try {
            serviceDiscovery = SharedDiscoveryManagement.getBackwardsServiceDiscoveryManager(groups, lookupLocators, null);
            ServiceItem returnObject = serviceDiscovery.lookup(templ, null, timeout);
            if (returnObject != null) {
                service = returnObject.service;
            }
        } finally {
            if (serviceDiscovery != null) {
                try {
                    serviceDiscovery.terminate();
                } catch (Exception e) {
                    logger.warn("Failed to terminate service discovery, ignoring", e);
                }
            }
        }
        return service;
    }

    /**
     * Sets if this proxy will be a smart proxy. When this value is set to <code>true</code> the
     * service found will be wrapped with a smart proxy that will detect failures and try to lookup
     * the service again in such cases. Defaults to <code>false</code>.
     */
    public void setSmartProxy(boolean smartProxy) {
        this.smartProxy = smartProxy;
    }

    /**
     * Sets the number of successive method invocation lookup retry count in case of a failure.
     * Defaults to 3.
     */
    public void setRetryCountOnFailure(int retryCountOnFailure) {
        this.retryCountOnFailure = retryCountOnFailure;
    }

    /**
     * Returns the groups.
     */
    public String[] getGroups() {
        return groups;
    }

    /**
     * The groups to set
     */
    public void setGroups(String[] groups) {
        this.groups = groups;
    }

    /**
     * Returns the locators.
     */
    public String[] getLocators() {
        return locators;
    }

    /**
     * Sets the locators.
     */
    public void setLocators(String[] locators) {
        this.locators = locators;
    }

    /**
     * @return Returns the serviceClass.
     */
    public Class<?> getServiceClass() {
        return serviceClass;
    }

    /**
     * @param serviceClass The serviceClass to set.
     */
    public void setServiceClass(Class<?> serviceClass) {
        this.serviceClass = serviceClass;
    }

    /**
     * @return Returns the serviceName.
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * @param serviceName The serviceName to set.
     */
    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    /**
     * @return Returns the template.
     */
    public ServiceTemplate getTemplate() {
        return template;
    }

    /**
     * @param template The template to set.
     */
    public void setTemplate(ServiceTemplate template) {
        this.template = template;
    }

    /**
     * The timeout to wait looking up the service
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * The timeout to wait looking up the service
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

}
