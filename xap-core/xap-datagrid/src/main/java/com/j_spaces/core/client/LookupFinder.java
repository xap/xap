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

package com.j_spaces.core.client;

import com.gigaspaces.internal.remoting.routing.clustered.LookupType;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.jini.SharedDiscoveryManagement;
import com.j_spaces.core.jini.SharedDiscoveryManagement.SharedServiceDiscoveryManager;
import com.j_spaces.core.service.ServiceConfigLoader;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.DiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.entry.Name;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * LookupFinder is a generalized concept of the SpaceFinder utility. It simplifies the process of
 * service lookup and acquiring a proxy of this service. The lookup performed on three criterions:
 * service name, service class and service attributes. Any one of the above criterion can be null.
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @deprecated Since 8.0 - This class is reserved for internal usage only.
 */
@com.gigaspaces.api.InternalApi
public class LookupFinder {
    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_LOOKUPFINDER);

    public static final long DEFAULT_TIMEOUT = 5 * 1000;
    public final static long DEFAULT_INTERVAL_TIMEOUT = 100;
    public final static long SINGLE_LOOKUP_TIMEOUT = 1;

    /**
     * If this system property is set to "true", {@link #close()} will actually perform the close
     * operation, otherwise, it will not perform anything. To force close, please call {@link
     * #forceClose()}.
     */
    public static final String ACTUAL_CLOSE_SYSTEM_PROPERTY = "com.gigaspaces.lookupfinder.actualclose";

    private final static long REPEATS_BASED_LUS_LOOKUP_TIMEOUT = 5 * 1000;

    private static final Set<SharedServiceDiscoveryManager> sdms = new HashSet<SharedServiceDiscoveryManager>();

    private static Configuration serviceConfig;

    static {
        try {
            serviceConfig = ServiceConfigLoader.getConfiguration();
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Closes the lookup finder if the {@link #ACTUAL_CLOSE_SYSTEM_PROPERTY} is set to "true". IF it
     * is not set, will default to "true".
     */
    public static void close() {
        if (System.getProperty(ACTUAL_CLOSE_SYSTEM_PROPERTY, "true").equals("true")) {
            forceClose();
        }
    }

    /**
     * Forces close regardless of the {@link #ACTUAL_CLOSE_SYSTEM_PROPERTY} system proepty.
     */
    public static void forceClose() {
        synchronized (sdms) {
            for (SharedServiceDiscoveryManager sdm : sdms)
                sdm.terminate();
            sdms.clear();
        }
    }

    public static Object find(LookupRequest request)
            throws FinderException {
        final String serviceName = request.getServiceName();
        final Class serviceClass = request.getServiceClass();
        final ServiceID serviceID = getServiceIDFromRequest(request);
        final Entry[] serviceAttributes = appendNameIfNeeded(request.getServiceAttributes(), serviceName);
        sortLookupAttributes(serviceAttributes);
        if (serviceName == null && serviceClass == null && serviceAttributes == null)
            throw new IllegalArgumentException("All parameters can not be null.");

        final long timeout = request.getTimeout() <= 0 ? DEFAULT_TIMEOUT : request.getTimeout();
        final LookupLocator[] lookupLocators = request.getLookupLocators();
        final String[] lookupGroups = request.getLookupGroups();
        final Class[] serviceClasses = serviceClass != null ? new Class[]{serviceClass} : null;
        final ServiceTemplate template = new ServiceTemplate(serviceID, serviceClasses, serviceAttributes);

        SharedServiceDiscoveryManager sdm = null;
        Object result = null;
        try {
            Exception exception = null;
            try {
                sdm = SharedDiscoveryManagement.getBackwardsServiceDiscoveryManager(lookupGroups, lookupLocators, null);
                result = find(template, sdm, timeout, request);
            } catch (Exception ex) {
                exception = ex;
            }

            if (result != null) {
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.fine(generateReport(result, serviceName, serviceID, serviceClass, serviceAttributes,
                            lookupLocators, lookupGroups, timeout, sdm));
                }

                return result;
            }

            String report = generateReport(null, serviceName, serviceID, serviceClass, serviceAttributes,
                    lookupLocators, lookupGroups, timeout, sdm);
            if (_logger.isLoggable(Level.FINE))
                _logger.fine(report);

            throw new FinderException(report, exception);
        } finally {
            if (sdm != null) {
                synchronized (sdms) {
                    final boolean terminateSDM =
                            request.getLookupType() == LookupType.TimeBasedLastIteration ||
                                    request.getLookupType() == LookupType.RepeatsBased;

                    // if finder exception was thrown call the terminate method
                    // that will decrease the reference count for this SDM
                    // when all the references are removed the SDM will be
                    // closed including its thread pools
                    if (result == null && terminateSDM) {
                        sdm.terminate();
                    } else {
                        boolean isNew = sdms.add(sdm);
                        if (!isNew)
                            sdm.terminate();
                    }
                }
            }
        }
    }

    private static ServiceID getServiceIDFromRequest(LookupRequest request) {
        if (request.getSpaceUuid() == null) {
            return null;
        }
        UUID uuid = UUID.fromString(request.getSpaceUuid());
        return new ServiceID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    private static Object find(ServiceTemplate template, SharedServiceDiscoveryManager sdm, long timeout, LookupRequest request)
            throws InterruptedException {

        final long interval = request.getLookupInterval();
        final long sdmCreationTime = SystemTime.timeMillis();

        // GS-10631
        // TODO add ability to block on lookup discovery manager until
        // discovery of all lookup servers has been attempted at least once
        if (timeout == SINGLE_LOOKUP_TIMEOUT && request.getLookupType() == LookupType.RepeatsBased &&
                sdm.getDiscoveryManager().getRegistrars().length == 0) {
            waitForLookupService(sdm);
        }

        final long deadline = SystemTime.timeMillis() + timeout;
        boolean firstTime = true;

        while (true) {
            if (firstTime && _logger.isLoggable(Level.FINE) && sdm.getDiscoveryManager().getRegistrars().length > 0) {
                _logger.fine("Initial LUS found in [" + (SystemTime.timeMillis() - sdmCreationTime) + "ms]");
                firstTime = false;
            }
            final long lookupTimeStart = _logger.isLoggable(Level.FINE) ? SystemTime.timeMillis() : 0;
            final ServiceItem foundService = sdm.lookup(template, null);
            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("Service Lookup took [" + (SystemTime.timeMillis() - lookupTimeStart) + "ms]");
            }
            final Object result = foundService == null ? null : foundService.getService();
            if (result != null)
                return result;

            if (SystemTime.timeMillis() > deadline) {
                return null;
            }
            Thread.sleep(interval);
        }
    }


    private static void waitForLookupService(ServiceDiscoveryManager sdm) throws InterruptedException {
        final CountDownLatch discoveredLatch = new CountDownLatch(1);
        DiscoveryListener listener = new DiscoveryListener() {
            public void discovered(DiscoveryEvent e) {
                discoveredLatch.countDown();
            }

            public void discarded(DiscoveryEvent e) {
            }
        };
        sdm.getDiscoveryManager().addDiscoveryListener(listener);
        try {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine("1ms (special value) passed, waiting at most [" +
                        REPEATS_BASED_LUS_LOOKUP_TIMEOUT + "ms] to discover at least one lookup service");

            discoveredLatch.await(REPEATS_BASED_LUS_LOOKUP_TIMEOUT, TimeUnit.MILLISECONDS);
            if (_logger.isLoggable(Level.FINE) &&
                    sdm.getDiscoveryManager().getRegistrars().length == 0)
                _logger.fine("No LUS found after wait");
        } finally {
            sdm.getDiscoveryManager().removeDiscoveryListener(listener);
        }
    }

    static private String generateReport(Object foundService, String serviceName, ServiceID serviceID,
                                         Class serviceClass, Entry[] serviceAttributes, LookupLocator[] lookupLocators,
                                         String[] lookupGroups, long timeout, ServiceDiscoveryManager sdm) {
        StringBuilder sb;
        if (foundService != null)
            sb = new StringBuilder("LookupFinder found service using the following service attributes: \n");
        else
            sb = new StringBuilder("LookupFinder failed to find service using the following service attributes: \n");

        if (serviceAttributes != null && serviceAttributes.length > 0) {
            for (int i = 0; i < serviceAttributes.length; i++)
                sb.append("\n\t Service attributes: [").append(serviceAttributes[i]).append(']');
        }
        sb.append("\n\t Lookup timeout: [").append(timeout).append(']');
        if (serviceClass != null)
            sb.append("\n\t Class: " + serviceClass.getName());
        if (serviceName != null)
            sb.append("\n\t Service name: [").append(serviceName).append(']');
        if (serviceID != null)
            sb.append("\n\t Service Id: [").append(serviceID).append(']');
        if (lookupGroups != null)
            sb.append("\n\t Jini Lookup Groups: ").append(Arrays.asList(lookupGroups));
        if (lookupLocators != null)
            sb.append("\n\t Jini Lookup Locators: ").append(Arrays.asList(lookupLocators));
        if (foundService != null)
            sb.append("\n\t Found Jini service: ").append((foundService).toString());
        if (sdm != null)
            sb.append("\n\t Number of Lookup Services: ").append(sdm.getDiscoveryManager().getRegistrars().length);

        sb.append('\n');
        return sb.toString();
    }

    private static Entry[] appendNameIfNeeded(Entry[] attributes, String serviceName) {

        if (serviceName == null)
            return attributes;

        final Entry nameAttribute = new Name(serviceName);
        if (attributes == null || attributes.length == 0)
            return new Entry[]{nameAttribute};

        Entry[] result = new Entry[attributes.length + 1];
        for (int i = 0; i < attributes.length; i++)
            result[i] = attributes[i];
        result[attributes.length] = nameAttribute;
        return result;
    }

    public static void sortLookupAttributes(Entry[] attributes) {
        if (attributes != null && attributes.length > 1)
            Arrays.sort(attributes, ATTRIBUTES_COMPARATOR);
    }

    private static Comparator<Entry> ATTRIBUTES_COMPARATOR = new Comparator<Entry>() {
        @Override
        public int compare(Entry o1, Entry o2) {
            return o1.getClass().getSimpleName().compareTo(o2.getClass().getSimpleName());
        }
    };
}
