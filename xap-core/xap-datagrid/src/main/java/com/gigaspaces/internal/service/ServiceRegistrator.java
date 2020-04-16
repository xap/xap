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
import com.j_spaces.core.jini.SharedDiscoveryManagement;
import com.j_spaces.core.jini.SharedLeaseRenewalManager;
import com.j_spaces.core.service.ServiceConfigLoader;
import com.j_spaces.kernel.JSpaceUtilities;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.discovery.DiscoveryListener;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.JoinManager;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 8.0.3
 */
@com.gigaspaces.api.InternalApi
public class ServiceRegistrator {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_SERVICE);

    private final String[] _lookupGroups;
    private final LookupLocator[] _lookupLocators;
    private final DiscoveryListener _discoveryListener;
    private final boolean _useSharedLeaseRenewalManager;
    private final AbstractGigaSpacesService _service;
    private final Entry[] _serviceAttributes;

    private volatile boolean _isRegistered;
    private LookupDiscoveryManager _discoveryManager;
    private LeaseRenewalManager _leaseRenewalManager;
    private JoinManager _joinManager;

    public ServiceRegistrator(String[] groups, LookupLocator[] locators,
                              DiscoveryListener listener, boolean useSharedLeaseRenewalManager,
                              AbstractGigaSpacesService service, Entry[] serviceAttributes) {
        this._lookupGroups = groups;
        this._lookupLocators = locators;
        this._discoveryListener = listener;
        this._useSharedLeaseRenewalManager = useSharedLeaseRenewalManager;
        this._service = service;
        this._serviceAttributes = serviceAttributes;
    }

    public synchronized JoinManager getJoinManager() {
        return _joinManager;
    }

    public boolean isRegistered() {
        return _isRegistered;
    }

    public synchronized void register() throws ServiceRegistrationException {
        if (_isRegistered)
            return;

        try {
            _discoveryManager = SharedDiscoveryManagement.getLookupDiscoveryManager(
                    _lookupGroups, _lookupLocators, _discoveryListener);

            Object serviceProxy = _service.getServiceProxy();
            ServiceID serviceId = new ServiceID(
                    _service.getUuid().getMostSignificantBits(),
                    _service.getUuid().getLeastSignificantBits());

            boolean useAdvancedConfig = !_useSharedLeaseRenewalManager;
            Configuration config = getConfiguration(useAdvancedConfig);
            _leaseRenewalManager = _useSharedLeaseRenewalManager ? SharedLeaseRenewalManager.getLeaseRenewalManager() : new LeaseRenewalManager(config);

            _joinManager = new JoinManager(serviceProxy, _serviceAttributes, serviceId,
                    _discoveryManager, _leaseRenewalManager, config);

            _isRegistered = true;
            if (_logger.isDebugEnabled()) {
                String groupsDesc = _lookupGroups == null ? "[ALL]" : Arrays.asList(_lookupGroups).toString();
                String locatorsSuffix = _lookupLocators == null ? "" : "\n[ and Unicast Locators " + Arrays.asList(_lookupLocators) + "  ]";

                String message = "GigaSpaces Service (Jini Lookup Service): \n"
                        + "[ " + _service.getServiceTypeDescription() + " <" + _service.getServiceName() + "> member of " + groupsDesc + " jini lookup groups  ]\n"
                        + "[ Was advertised with serviceID <" + serviceId + "> ]" + locatorsSuffix;

                _logger.debug(message);
            }
        } catch (Throwable e) {
            unregister();
            if (_logger.isErrorEnabled())
                _logger.error("Failed to register service: " + e.getMessage(), e);
            throw new ServiceRegistrationException("Failed to register service", e);
        }
    }

    public synchronized void unregister() {
        if (_joinManager != null) {
            _joinManager.terminate();
            _joinManager = null;
        }

        if (_leaseRenewalManager != null) {
            if (_useSharedLeaseRenewalManager)
                SharedLeaseRenewalManager.releaseLeaseRenewalManaer();
            else
                _leaseRenewalManager.terminate();
            _leaseRenewalManager = null;
        }

        if (_discoveryManager != null) {
            _discoveryManager.terminate();
            _discoveryManager = null;
        }

        _isRegistered = false;
    }

    private Configuration getConfiguration(boolean isAdvanced)
            throws ConfigurationException {
        JSpaceUtilities.setXMLImplSystemProps();
        Configuration config = null;

        try {
            config = isAdvanced ? ServiceConfigLoader.getAdvancedSpaceConfig() : ServiceConfigLoader.getConfiguration();
        }
        /** in case jini throws:
         * java.security.AccessControlException: access denied (java.lang.RuntimePermission createSecurityManager)
         * we do nothing and the user of the m_configuration needs to check if its not null.
         * The security exception might happen while running in the context of other container e.g.
         * embedded space inside Application server without granting implicit policy permissions.
         */ catch (java.security.AccessControlException e) {
            if (_logger.isWarnEnabled())
                _logger.warn("Failed to initialize Jini Configuration object", e);
        } catch (SecurityException e) {
            if (_logger.isWarnEnabled())
                _logger.warn("Failed to initialize Jini Configuration object", e);
        } catch (ExceptionInInitializerError e) {
            if (_logger.isWarnEnabled())
                _logger.warn("Failed to initialize Jini Configuration object", e);
        } catch (Throwable e) {
            if (_logger.isErrorEnabled())
                _logger.error("Failed to initialize Jini Configuration object", e);
        }

        if (config != null && isAdvanced) {
            Long renewalDuration = ((Long) config.getEntry("net.jini.lookup.JoinManager", "maxLeaseDuration", long.class));
            Long roundTripTime = ((Long) config.getEntry("net.jini.lease.LeaseRenewalManager", "roundTripTime", long.class));
            if (roundTripTime != null && renewalDuration != null)
                _logger.debug("The Space JoinManager leasing configuration values are: roundTripTime:" + roundTripTime + " maxLeaseDuration:" + renewalDuration);
        }
        return config;
    }
}
