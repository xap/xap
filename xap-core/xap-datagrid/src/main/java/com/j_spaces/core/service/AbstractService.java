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

package com.j_spaces.core.service;

import com.gigaspaces.internal.service.AbstractGigaSpacesService;
import com.gigaspaces.internal.service.ServiceRegistrationException;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.start.SystemInfo;
import com.j_spaces.core.LookupManager;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.log.JProperties;
import com.sun.jini.start.LifeCycle;

import net.jini.core.discovery.LookupLocator;
import net.jini.discovery.DiscoveryListener;
import net.jini.export.Exporter;

import java.rmi.RemoteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.j_spaces.core.Constants.LookupManager.LOOKUP_GROUP_PROP;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_UNICAST_ENABLED_DEFAULT;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_UNICAST_ENABLED_PROP;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_UNICAST_URL_DEFAULT;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_UNICAST_URL_PROP;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
public abstract class AbstractService extends AbstractGigaSpacesService implements Service {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_ADMIN);

    protected Exporter m_adminExporter;
    protected ServiceAdminImpl m_adminImpl;

    public AbstractService(LifeCycle lifeCycle) {
        super(lifeCycle);

        m_adminExporter = ServiceConfigLoader.getExporter();
    }

    public void registerLookupService(DiscoveryListener listener, boolean useSharedLeaseRenewalManager)
            throws ServiceRegistrationException {
        if (getJoinManager() != null)
            return;

        String containerName = getContainerName();
        // Load lookup groups:
        String[] lookupGroups = JSpaceUtilities.parseLookupGroups(JProperties.getContainerProperty(containerName,
                LOOKUP_GROUP_PROP, SystemInfo.singleton().lookup().defaultGroups()));

        // Load lookup locators (check unicast discovery flag):
        LookupLocator[] lookupLocators = null;
        if (JProperties.getContainerProperty(containerName, LOOKUP_UNICAST_ENABLED_PROP, LOOKUP_UNICAST_ENABLED_DEFAULT).equalsIgnoreCase("true")) {
            String unicastHosts = JProperties.getContainerProperty(containerName, LOOKUP_UNICAST_URL_PROP, LOOKUP_UNICAST_URL_DEFAULT);
            lookupLocators = LookupManager.buildLookupLocators(unicastHosts);
        }

        registerLookupService(lookupGroups, lookupLocators, listener, useSharedLeaseRenewalManager);
    }

    public abstract String getContainerName();

    public abstract void shutdown() throws RemoteException;

    /**
     * Return service admin proxy
     */
    public synchronized Object getAdmin() {
        try {
            if (m_adminImpl == null)
                m_adminImpl = new ServiceAdminImpl(this, m_adminExporter);
        } catch (Exception ex) {
            if (_logger.isErrorEnabled())
                _logger.error(ex.getMessage(), ex);
        }

        return (m_adminImpl != null ? m_adminImpl.getProxy() : null);
    }
}
