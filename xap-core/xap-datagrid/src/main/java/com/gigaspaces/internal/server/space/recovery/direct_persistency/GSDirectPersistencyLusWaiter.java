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

//
package com.gigaspaces.internal.server.space.recovery.direct_persistency;

import com.gigaspaces.cluster.activeelection.core.ActiveElectionState;
import com.gigaspaces.internal.naming.INamingService;
import com.gigaspaces.internal.naming.LookupNamingService;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.service.Service;
import com.j_spaces.lookup.entry.ClusterGroup;
import com.j_spaces.lookup.entry.ClusterName;
import com.sun.jini.lookup.entry.LookupAttributes;

import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.lookup.JoinManager;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import org.slf4j.Logger;

//import com.gigaspaces.cluster.activeelection.core.ActiveElectionState.State;

/**
 * Created by yechielf on 05/03/2015.
 */
@com.gigaspaces.api.InternalApi
public class GSDirectPersistencyLusWaiter extends Thread {


    /**
     * the extension of service-template. For internal usage only
     */
    final private static class ElectionEntry
            extends ServiceTemplate
            implements Cloneable {
        private static final long serialVersionUID = 1L;

        ActiveElectionState _actState;

        /**
         * not <code>null</code> if the manage service implements {@link net.jini.id.ReferentUuid}
         * interface
         */
        ServiceID _serviceID;

        /**
         * A service object
         */
        transient Object service;

        private ElectionEntry( /*Object service,*/ ServiceTemplate srvTemplate) {
            /* constructor to make compiler happy :) */
            super(null, null, null);

            this.service = service;

/*            if ( service instanceof ReferentUuid )
            {
                long mostSignBits = ((ReferentUuid)service).getReferentUuid().getMostSignificantBits();
                long leastSignBits = ((ReferentUuid)service).getReferentUuid().getLeastSignificantBits();
                _serviceID = new ServiceID( mostSignBits, leastSignBits );
            }
*/
            /* keep the reference so that we can change the desired state on demand */
            _actState = new ActiveElectionState();

            /* Attribute set templates to match, or <tt>null</tt>. */
            this.attributeSetTemplates = LookupAttributes.add(srvTemplate.attributeSetTemplates, new Entry[]{_actState});

            /* Service types to match */
            this.serviceTypes = srvTemplate.serviceTypes;
        }// constructor

        /**
         * clone the ElectionEntry so that don't override on direct reference of
         * ActiveElectionState
         */
        @Override
        public ElectionEntry clone() {
            try {
                ElectionEntry cloneEntry = (ElectionEntry) super.clone();
                cloneEntry._actState = new ActiveElectionState();
                cloneEntry.attributeSetTemplates = attributeSetTemplates.clone();

                for (int i = 0; i < cloneEntry.attributeSetTemplates.length; i++) {
                    if (cloneEntry.attributeSetTemplates[i] instanceof ActiveElectionState) {
                        cloneEntry.attributeSetTemplates[i] = cloneEntry._actState;
                        break;
                    }
                }// for

                return cloneEntry;
            } catch (CloneNotSupportedException e) {
                // this shouldn't happen, since we are Cloneable
                throw new InternalError();
            }
        }

        private Object getService() {
            return service;
        }

        /**
         * set the desired state for this service template
         */
        private void setState(ActiveElectionState.State state) {
            _actState.setState(state);
        }

        /**
         * @return the serviceID of manage service, null <code>null</code> if service implements
         * {@link net.jini.id.ReferentUuid} interface
         **/
        private ServiceID getServiceID() {
            return _serviceID;
        }
    }


    //TBD Throw this POS after 10.1
    private static final long PERIODIC_SLEEP_TIME = 5000;


    private volatile boolean _closed;
    private final Logger _logger;
    final private INamingService _namingService;
    final private ElectionEntry _electTemplate;
    final private SpaceImpl _spaceImpl;
    final private Object _terminationMonitor;
    private volatile DirectPersistencyRecoveryException _ex;

    public GSDirectPersistencyLusWaiter(SpaceImpl spaceImpl,
                                        ClusterPolicy clusterPolicy,
                                        Logger logger, JoinManager joinManager,
                                        Object terminationMonitor
    )
            throws RemoteException {
        super("GSDirectPersistencyLusWaiter");
        this.setDaemon(true);
        _logger = logger;
        _spaceImpl = spaceImpl;
        _terminationMonitor = terminationMonitor;
            /*if ( service == null )
                throw new NullPointerException("service can not be null.");
            */
        ServiceTemplate participantSrvTemplate = buildServiceTemplate(clusterPolicy);
        if (participantSrvTemplate.attributeSetTemplates == null &&
                participantSrvTemplate.serviceTypes == null)
            throw new IllegalArgumentException("ServiceTemplate can not be initialized with attributeSetTemplates=null and serviceTypes=null");

            /*if ( !( service instanceof ReferentUuid ) )
            {
                if( _logger.isLoggable( Level.WARNING ) )
                    _logger.warning( service + " is not implements [" + ReferentUuid.class.getName() + "] interface. " +
                            "			  In this case the service must provide the consistent equals() implementation." );
            }
            */
        _namingService = new LookupNamingService((LookupDiscoveryManager) joinManager.getDiscoveryManager(), joinManager.getLeaseRenewalManager());
        _electTemplate = new ElectionEntry( /*service,*/ participantSrvTemplate);

    }


    private boolean isPrimaryActive() throws InterruptedException {
        List<ServiceItem> res = lookup(Integer.MAX_VALUE);
        if (res == null || res.isEmpty()) {
            if (_logger.isInfoEnabled()) {
                _logger.info(_spaceImpl.getEngine().getFullSpaceName() + " waiting for another space to become primary since its storage is  " +
                        "inconsistent, found none so far ");
            }
            return false;
        }
        if (_logger.isInfoEnabled()) {
            _logger.info(_spaceImpl.getEngine().getFullSpaceName() + " waited for another space to become primary since its storage is  " +
                    "inconsistent and found " + ((IJSpace) (res.get(0).getService())).getName());
        }
        return true;


    }


    private List<ServiceItem> lookup(int maxMatches) throws InterruptedException {

        _electTemplate.setState(ActiveElectionState.State.ACTIVE);
        ServiceItem[] foundSrv = _namingService.lookup(_electTemplate, maxMatches, null /*filter*/);

        if (foundSrv == null) {
            if (_logger.isTraceEnabled()) {
                _logger.trace(toString() + " lookup service not found while querying for state: " + _electTemplate._actState.getState());
            }
            return null;
        }

        /*
         * The same service may be registered on multiple naming services.
         * Removes the duplicates ServiceItems by ServiceID.
         */
        List<ServiceItem> matchedSrv = trimServices(foundSrv);

        if (_logger.isTraceEnabled()) {
            int duplicates = foundSrv.length - matchedSrv.size();
            _logger.trace(toString() + " found: [" + matchedSrv.size() + "] matches for " +
                    " serviceTemplate: [" + _electTemplate + "]; matched services: " + matchedSrv +
                    (duplicates > 0 ? " duplicates: [" + duplicates + "]" : ""));
        }


        return matchedSrv;
    }

    static List<ServiceItem> trimServices(ServiceItem[] srvSet) {
        TreeSet<ServiceItem> treeSet = new TreeSet<ServiceItem>(new Comparator<ServiceItem>() {
            @Override
            public int compare(ServiceItem o1, ServiceItem o2) {
                ServiceID srvID_1 = o1.serviceID;
                ServiceID srvID_2 = o2.serviceID;

                // The ordering is intentionally set up so that the UUIDs
                // can simply be numerically compared as two numbers
                return (srvID_1.getMostSignificantBits() < srvID_2.getMostSignificantBits() ? -1 :
                        (srvID_1.getMostSignificantBits() > srvID_2.getMostSignificantBits() ? 1 :
                                (srvID_1.getLeastSignificantBits() < srvID_2.getLeastSignificantBits() ? -1 :
                                        (srvID_1.getLeastSignificantBits() > srvID_2.getLeastSignificantBits() ? 1 : 0))));
            }
        });


        if (srvSet != null) {
            Collections.addAll(treeSet, srvSet);
        }

        List<ServiceItem> trimSet = new ArrayList<ServiceItem>();
        trimSet.addAll(treeSet);

        return trimSet;
    }

    private ServiceTemplate buildServiceTemplate(ClusterPolicy clusterPolicy) {
        Entry clusterName = new ClusterName(clusterPolicy.m_ClusterName);
        ClusterGroup clusterGroup = new ClusterGroup(clusterPolicy.m_FailOverPolicy.getElectionGroupName(),
                null, /* repl */
                null/* load-balance */);

        com.j_spaces.lookup.entry.State state = new com.j_spaces.lookup.entry.State();
        state.setElectable(Boolean.TRUE);

        Entry[] spaceTemplAttr = new Entry[]{clusterName, clusterGroup, state};
        Class[] serviceTypes = new Class[]{Service.class};

        return new ServiceTemplate(null, serviceTypes, spaceTemplAttr);
    }


    public void waitForAnotherPrimary(long maxWaitTime) {
        long ttl = maxWaitTime;
        synchronized (_terminationMonitor) {
            if (_ex != null)
                throw _ex;
            if (_closed)
                return; //already finished
            long t1 = System.currentTimeMillis();
            long t2 = t1 + maxWaitTime;
            while (true) {
                try {
                    _terminationMonitor.wait(ttl);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    _ex = new DirectPersistencyRecoveryException("space " + _spaceImpl.getEngine().getFullSpaceName() + " got exception while waiting for other space to become primary", e);
                    break;
                }
                if (_closed)
                    break;
                long cur = System.currentTimeMillis();
                if (cur < t2) {
                    ttl = t2 - cur;
                } else {
                    _ex = new DirectPersistencyRecoveryException("space " + _spaceImpl.getEngine().getFullSpaceName() + " time to wait for another primary expired");
                    try {
                        close();
                    } catch (InterruptedException e) {
                    }
                    break;
                }
            }
            if (_ex != null) {
                _logger.error("space " + _spaceImpl.getEngine().getFullSpaceName() + " got exception while waiting for other space to become primary " + _ex);
                throw _ex;
            }
        }
    }


    @Override
    public void run() {
        try {
            while (!_closed) {
                try {

                    if (isPrimaryActive())
                        break;
                    Thread.sleep(PERIODIC_SLEEP_TIME);

                } catch (Exception ex) {
                    _logger.error("space " + _spaceImpl.getEngine().getFullSpaceName() + " got exception while waiting for other space to become primary " + ex);
                    DirectPersistencyRecoveryException e = _ex = new DirectPersistencyRecoveryException("space " + _spaceImpl.getEngine().getFullSpaceName() + " got exception while waiting for other space to become primary", ex);
                    if (_ex == null)
                        _ex = e;
                    throw e;
                }

            }
        } finally {
            _namingService.terminate();
            synchronized (_terminationMonitor) {
                _closed = true;
                _terminationMonitor.notifyAll();
            }

        }

    }

    public void close() throws InterruptedException {
        synchronized (_terminationMonitor) {
            if (_closed)
                return;
            _closed = true;
            interrupt();
            _terminationMonitor.wait();
        }
    }


}
