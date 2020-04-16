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

package com.gigaspaces.internal.service;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.ILRMIService;
import com.j_spaces.core.service.CountdownModifyAttributesLatchFactory;
import com.j_spaces.core.service.ServiceConfigLoader;
import com.sun.jini.start.LifeCycle;
import com.sun.jini.start.ServiceProxyAccessor;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;
import net.jini.discovery.DiscoveryListener;
import net.jini.export.Exporter;
import net.jini.id.Uuid;
import net.jini.id.UuidFactory;
import net.jini.lookup.JoinManager;

import java.rmi.Remote;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for GigaSpaces Services that supports registration in Jini Lookup Service.
 *
 * @author Niv Ingberg
 * @since 8.0.3
 */
public abstract class AbstractGigaSpacesService implements Remote, ServiceProxyAccessor, ILRMIService {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_SERVICE);

    /**
     * Wait for 60 seconds for any attributes change
     */
    public static final long SERVICE_ATTRIBUTE_MODIFY_AWAIT = 10000;

    private final Uuid _uuid;
    private final LifeCycle _lifeCycle;
    private volatile ServiceRegistrator _serviceRegistrator;
    private final List<Entry> _lookupAttributes;

    protected AbstractGigaSpacesService(LifeCycle lifeCycle) {
        this(lifeCycle, UuidFactory.generate());

    }

    protected AbstractGigaSpacesService(LifeCycle lifeCycle, Uuid serviceUuid) {
        this._lifeCycle = lifeCycle;
        this._uuid = serviceUuid;
        this._lookupAttributes = new ArrayList<Entry>();
    }

    public Uuid getUuid() {
        return _uuid;
    }

    public LifeCycle getLifeCycle() {
        return _lifeCycle;
    }

    @Override
    public abstract String getServiceName();

    public JoinManager getJoinManager() {
        return _serviceRegistrator == null ? null : _serviceRegistrator.getJoinManager();
    }

    /**
     * Add attributes to the collection of attributes the JoinManager utility maintains. If the
     * service is not advertised, the new attribute will be added to the local collection of
     * attributes. NOTE: This method is not thread safe - it should no be called concurrently with
     * register/unregister.
     *
     * @param attributes - Array of Entry attributes
     */
    public void addLookupAttributes(Entry[] attributes) {
        try {
            addLookupAttributes(attributes, SERVICE_ATTRIBUTE_MODIFY_AWAIT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Add the lookup attributes with a specified timeout to wait for the request to complete.
     *
     * @return true if attributes were added, false otherwise.
     * @throws InterruptedException if this request was interrupted while waiting for completion
     * @since 12.1.1
     */
    public void addLookupAttributes(Entry[] attributes, long timeoutInMillis) throws InterruptedException {
        if (attributes == null) {
            return;
        }
        boolean advertised;
        JoinManager joinManager = getJoinManager();
        if (joinManager == null) {
            Collections.addAll(_lookupAttributes, attributes);
            advertised = false;
        } else {
            JoinManager.ModifyAttributesLatch latch = joinManager.addAttributes(attributes, false, CountdownModifyAttributesLatchFactory.INSTANCE);
            advertised = latch.await(timeoutInMillis);
        }
        if (!advertised) {
            _logger.warn("active attribute has not yet been advertised");
        }
    }

    /**
     * Modify the current attribute sets. The resulting set will be used for all future joins.  The
     * same modifications are also made to all currently-joined lookup services. NOTE: This method
     * is not thread safe - it should no be called concurrently with register/unregister.
     *
     * @param attrSetTemplates the templates for matching attribute sets
     * @param attrSets         the modifications to make to matching sets
     */
    public void modifyLookupAttributes(Entry[] attrSetTemplates, Entry[] attrSets) {
        if (getJoinManager() != null) {
            JoinManager.ModifyAttributesLatch latch = getJoinManager().modifyAttributes(attrSetTemplates, attrSets,
                    false, CountdownModifyAttributesLatchFactory.INSTANCE);
            try {
                latch.await(SERVICE_ATTRIBUTE_MODIFY_AWAIT);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void registerLookupService(String[] lookupGroups, LookupLocator[] lookupLocators,
                                      DiscoveryListener listener, boolean useSharedLeaseRenewalManager)
            throws ServiceRegistrationException {
        if (_serviceRegistrator != null)
            return;

        AppendInitialLookupAttributes(_lookupAttributes);
        Entry[] serviceAttributes = _lookupAttributes.toArray(new Entry[_lookupAttributes.size()]);

        _serviceRegistrator = new ServiceRegistrator(lookupGroups, lookupLocators, listener, useSharedLeaseRenewalManager,
                this, serviceAttributes);

        _serviceRegistrator.register();
    }

    public void unregisterFromLookupService() {
        if (_serviceRegistrator != null) {
            _serviceRegistrator.unregister();
            _serviceRegistrator = null;
        }
    }

    protected String getServiceTypeDescription() {
        return this.getClass().getName();
    }

    protected void AppendInitialLookupAttributes(List<Entry> lookupAttributes) throws ServiceRegistrationException {
    }

    /**
     * Returns an instance of Exporter. Loads Jini configuration (services.config by default) and
     * creates the relevant exporter (which is GenericExporter by default).
     */
    protected static Exporter getExporter() {
        Exporter exporter = null;

        //TODO check if we can have a singleton Exporter instance
        // why cant we have SAME instance for all calls?
        // and why do we need 2 instances, one for admin API calls and another for the space/space container?
        try {
            exporter = ServiceConfigLoader.getExporter();
        }
        /** in case jini throws:
         * java.security.AccessControlException: access denied (java.lang.RuntimePermission createSecurityManager)
         * we do nothing and the user of the m_configuration needs to check if its not null.
         * The security exception might happen while running in the context of other container e.g.
         * embedded space inside Application server without granting implicit policy permissions.
         */ catch (java.security.AccessControlException e) {
            if (_logger.isWarnEnabled())
                _logger.warn("Failed to create exporter.", e);
        } catch (SecurityException e) {
            if (_logger.isWarnEnabled())
                _logger.warn("Failed to create exporter.", e);
        } catch (ExceptionInInitializerError e) {
            if (_logger.isWarnEnabled())
                _logger.warn("Failed to create exporter.", e);
        } catch (Throwable e) {
            if (_logger.isErrorEnabled())
                _logger.error("Failed to create exporter.", e);
        }

        return exporter;
    }
}
