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

import com.gigaspaces.time.SystemTime;

import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;
import net.jini.discovery.DiscoveryGroupManagement;
import net.jini.discovery.DiscoveryLocatorManagement;
import net.jini.export.Exporter;
import net.jini.lookup.JoinManager;

import java.io.IOException;
import java.rmi.RemoteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

@com.gigaspaces.api.InternalApi
public class ServiceAdminImpl
        implements ServiceAdmin {
    /**
     * Maximum delay for unexport attempts
     */
    protected static final long MAX_UNEXPORT_DELAY = 1000 * 60; // 1 minute

    protected final RemoteException joinManagerNullException =
            new RemoteException("JoinManager is null. Please verify that Jini LUS is enabled.");

    /**
     * The service instance
     */
    protected AbstractService m_service;
    /**
     * The Exporter for the ServiceAdmin
     */
    protected Exporter m_exporter;
    /**
     * The remote reference to myself
     */
    protected ServiceAdmin m_thisRemoteRef;
    /**
     * The ServiceAdminProxy
     */
    protected ServiceAdminProxy m_adminProxy;

    /**
     * Preparer for received lookup locators
     */
    //protected ProxyPreparer m_locatorPreparer = new BasicProxyPreparer();

    //logger
    private static Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_ADMIN);

    /**
     * Constructs a <code>ServiceAdminImpl</code>.
     */
    public ServiceAdminImpl(AbstractService service, Exporter exporter)
            throws RemoteException {
        m_service = service;
        m_exporter = exporter;
        m_thisRemoteRef = (ServiceAdmin) exporter.export(this);
    }

    /**
     * Get the object to communicate with the ServiceAdminImpl
     */
    public ServiceAdmin getProxy() {
        if (m_adminProxy == null)
            m_adminProxy = ServiceAdminProxy.getInstance(m_thisRemoteRef, m_service.getUuid());

        return m_adminProxy;
    }

    /**
     * Unexport the ServiceAdmin
     */
    public void unexport(boolean force) {
        if (m_thisRemoteRef != null)
            try {
                if (force)
                    m_exporter.unexport(true);
                else {
                    long endTime = SystemTime.timeMillis() + MAX_UNEXPORT_DELAY;
                    boolean unexported = false;

                    /** first try unexporting politely */
                    while (!unexported && (SystemTime.timeMillis() < endTime)) {
                        unexported = m_exporter.unexport(false);
                        if (!unexported)
                            Thread.yield();
                    }

                    /** if still not unexported, forcibly unexport */
                    if (!unexported)
                        m_exporter.unexport(true);
                }
            } catch (Exception ex) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(ex.getMessage(), ex);
                }
            }
    }


    /**
     * Throw the <code>RemoteException</code> in case of JoinManager is null.
     */
    protected void validateJoinManager(JoinManager mgr) throws RemoteException {
        if (mgr == null) {
            throw joinManagerNullException;
        }
    }

    //**************
    // DestroyAdmin
    //**************
    public void destroy() throws RemoteException {
        m_service.shutdown();
    }

    //***********
    // JoinAdmin
    //***********
    public Entry[] getLookupAttributes()
            throws RemoteException {
        JoinManager mgr = m_service.getJoinManager();
        validateJoinManager(mgr);
        return mgr.getAttributes();
    }

    public void addLookupAttributes(Entry[] attrSets)
            throws RemoteException {
        JoinManager mgr = m_service.getJoinManager();
        validateJoinManager(mgr);
        JoinManager.ModifyAttributesLatch latch = mgr.addAttributes(attrSets, false, CountdownModifyAttributesLatchFactory.INSTANCE);
        try {
            latch.await(AbstractService.SERVICE_ATTRIBUTE_MODIFY_AWAIT);
        } catch (InterruptedException e) {
            throw new RemoteException("Failed to wait for modify attributes");
        }
    }

    public void modifyLookupAttributes(Entry[] attrSetTemplates, Entry[] attrSets)
            throws RemoteException {
        JoinManager mgr = m_service.getJoinManager();
        validateJoinManager(mgr);
        JoinManager.ModifyAttributesLatch latch = mgr.modifyAttributes(attrSetTemplates, attrSets, false, CountdownModifyAttributesLatchFactory.INSTANCE);
        try {
            latch.await(AbstractService.SERVICE_ATTRIBUTE_MODIFY_AWAIT);
        } catch (InterruptedException e) {
            throw new RemoteException("Failed to wait for modify attributes");
        }
    }

    public String[] getLookupGroups()
            throws RemoteException {
        JoinManager mgr = m_service.getJoinManager();
        validateJoinManager(mgr);

        DiscoveryGroupManagement dgm = (DiscoveryGroupManagement) mgr.getDiscoveryManager();
        return dgm.getGroups();
    }

    public void addLookupGroups(String[] groups)
            throws RemoteException {
        JoinManager mgr = m_service.getJoinManager();
        validateJoinManager(mgr);

        DiscoveryGroupManagement dgm = (DiscoveryGroupManagement) mgr.getDiscoveryManager();
        try {
            dgm.addGroups(groups);
        } catch (IOException ex) {
            throw new RemoteException(ex.toString());
        }
    }

    public void removeLookupGroups(String[] groups)
            throws RemoteException {
        JoinManager mgr = m_service.getJoinManager();
        validateJoinManager(mgr);

        DiscoveryGroupManagement dgm = (DiscoveryGroupManagement) mgr.getDiscoveryManager();
        dgm.removeGroups(groups);
    }

    public void setLookupGroups(String[] groups)
            throws RemoteException {
        JoinManager mgr = m_service.getJoinManager();
        validateJoinManager(mgr);

        DiscoveryGroupManagement dgm = (DiscoveryGroupManagement) mgr.getDiscoveryManager();
        try {
            dgm.setGroups(groups);
        } catch (IOException e) {
            throw new RemoteException(e.toString());
        }
    }

    public LookupLocator[] getLookupLocators()
            throws RemoteException {
        JoinManager mgr = m_service.getJoinManager();
        validateJoinManager(mgr);

        DiscoveryLocatorManagement dlm = (DiscoveryLocatorManagement) mgr.getDiscoveryManager();
        return dlm.getLocators();
    }

    public void addLookupLocators(LookupLocator[] locators)
            throws RemoteException {
        JoinManager mgr = m_service.getJoinManager();
        validateJoinManager(mgr);

        DiscoveryLocatorManagement dlm = (DiscoveryLocatorManagement) mgr.getDiscoveryManager();
        dlm.addLocators(locators);
    }

    public void removeLookupLocators(LookupLocator[] locators)
            throws RemoteException {
        JoinManager mgr = m_service.getJoinManager();
        validateJoinManager(mgr);

        DiscoveryLocatorManagement dlm = (DiscoveryLocatorManagement) mgr.getDiscoveryManager();
        dlm.removeLocators(locators);
    }

    public void setLookupLocators(LookupLocator[] locators)
            throws RemoteException {
        JoinManager mgr = m_service.getJoinManager();
        validateJoinManager(mgr);

        //locators = prepareLocators(locators, m_locatorPreparer, false);
        DiscoveryLocatorManagement dlm = (DiscoveryLocatorManagement) mgr.getDiscoveryManager();
        dlm.setLocators(locators);
    }

}
