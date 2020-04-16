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

package com.gigaspaces.cluster.activeelection;

import com.gigaspaces.cluster.activeelection.core.ActiveElectionConfig;
import com.gigaspaces.cluster.activeelection.core.ActiveElectionEvent;
import com.gigaspaces.cluster.activeelection.core.ActiveElectionException;
import com.gigaspaces.cluster.activeelection.core.ActiveElectionManager;
import com.gigaspaces.cluster.activeelection.core.ActiveElectionState;
import com.gigaspaces.cluster.activeelection.core.IActiveElectionDecisionFilter;
import com.gigaspaces.cluster.activeelection.core.IActiveElectionListener;
import com.gigaspaces.cluster.activeelection.core.SplitBrainRecoveryHolder;
import com.gigaspaces.cluster.activeelection.core.SplitBrainRecoveryPolicy;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.naming.INamingService;
import com.gigaspaces.internal.naming.LookupNamingService;
import com.gigaspaces.internal.service.ServiceItemUtils;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.service.Service;
import com.j_spaces.lookup.entry.ClusterGroup;
import com.j_spaces.lookup.entry.ClusterName;
import com.j_spaces.lookup.entry.HostName;
import com.j_spaces.lookup.entry.State;

import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.LookupDiscoveryManager;

import java.rmi.RemoteException;



/**
 * Provides runtime primary/backup selection in cluster topologies.
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class LusBasedSelectorHandler extends LeaderSelectorHandler implements IActiveElectionListener {
    private ActiveElectionManager _electManager;
    private INamingService _namingService;

    /**
     * keep the primary space item
     */
    volatile private ServiceItem _primarySrvItem;

    /**
     * indication whether the functionality class was terminated
     */
    private boolean _isTerminated = false;

    private SpaceProxyImpl _securedProxy;

    public LusBasedSelectorHandler(SpaceProxyImpl securedProxy) {
        _securedProxy = securedProxy;
    }

    public void initialize(LeaderSelectorHandlerConfig config) throws Exception {
        super.initialize(config);

        /* extract ActiveElection args */
        _namingService = new LookupNamingService((LookupDiscoveryManager) config.getSpace().getJoinManager().getDiscoveryManager(),
                config.getSpace().getJoinManager().getLeaseRenewalManager());
        ServiceTemplate spaceTemplItem = buildServiceTemplate(config.getSpace().getClusterPolicy());
        IActiveElectionDecisionFilter electFilter = new PriorityElectionFilter(config.getSpace().getServiceName(),
                config.getSpace().getClusterPolicy().m_FailOverPolicy);
        ActiveElectionConfig _electConfig = config.getSpace().getClusterPolicy().m_FailOverPolicy.getActiveElectionConfig();


        SplitBrainRecoveryHolder splitBrainRecoveryHolder = new SplitBrainRecoveryHolder(SplitBrainRecoveryPolicy.getEnum(
                config.getSpace().getConfig().getProperty("space-config.split-brain.recovery-policy.after-detection", "discard-least-consistent")),
                config.getSpace().getQuiesceHandler()
        );

        try {
            _electManager = new ActiveElectionManager(config.getSpace().getNodeName(), _securedProxy, spaceTemplItem,
                    _namingService, this, electFilter, _electConfig, config.getSpace().getClusterFailureDetector(),
                    splitBrainRecoveryHolder, config.getSpace().getRecoveryIndicator());
        } catch (ActiveElectionException e) {
            /* explicitly set only the message, to avoid exception wrapping */
            throw new RemoteException(e.getMessage());
        }

    }//<init>


    /**
     * @return the initialized {@link ServiceTemplate} service template
     */
    private ServiceTemplate buildServiceTemplate(ClusterPolicy clusterPolicy) {
        Entry clusterName = new ClusterName(clusterPolicy.m_ClusterName);
        ClusterGroup clusterGroup = new ClusterGroup(clusterPolicy.m_FailOverPolicy.getElectionGroupName(),
                null, /* repl */
                null/* load-balance */);

        State state = new State();
        state.setElectable(Boolean.TRUE);

        Entry[] spaceTemplAttr = new Entry[]{clusterName, clusterGroup, state};
        Class[] serviceTypes = new Class[]{Service.class};

        return new ServiceTemplate(null, serviceTypes, spaceTemplAttr);
    }

    /**
     * @return <code>true</code>if the Space is primary, otherwise the space in backup mode.
     */
    public boolean isPrimary() {
        return _spaceMode == SpaceMode.PRIMARY;
    }


    /**
     * set primary space item
     */
    private void setPrimarySpaceItem(ServiceItem primarySrvItem) {
        _primarySrvItem = primarySrvItem;
    }

    /**
     * @return the reference to the primary space
     */
    public String getPrimaryMemberName() {
        return ServiceItemUtils.getSpaceMemberName(_primarySrvItem);
    }

    /**
     * start the primary election manager
     */
    public void select() throws RemoteException {
        try {
            if (_spaceMode == SpaceMode.NONE) {
                _electManager.elect();
            } else {
                _electManager.reelect();
            }
        } catch (ActiveElectionException e) {
            throw new RemoteException(_spaceMember + " failed to elect Primary space", e);
        } catch (InterruptedException ex) {
        }
    }


    /**
     * @see IActiveElectionListener#onActive(ActiveElectionEvent)
     **/
    @Override
    public void onActive(ActiveElectionEvent theEvent) {
        ServiceItem activeServiceItem = theEvent.getActiveServiceItem();

        if (_logger.isTraceEnabled())
            _logger.trace("PrimarySpaceSelector --> onActive: " + activeServiceItem);


 	  /* if true the active was changed */
        ServiceItem primarySrvItem = _primarySrvItem;
        if (primarySrvItem != null && !primarySrvItem.serviceID.equals(activeServiceItem.serviceID)) {
            StringBuilder logBuf = new StringBuilder();
            String failurePrimaryHost = HostName.getHostNameFrom(primarySrvItem.attributeSets);
            Object service = primarySrvItem.getService();
            logBuf.append("Space instance identified primary [").append(service).append("] failure");
            if (failurePrimaryHost != null)
                logBuf.append(" on [").append(failurePrimaryHost).append("] machine");

            _logger.info(logBuf.toString());
        }// if

        if (_electManager.getState() == ActiveElectionState.State.ACTIVE) {
            _logger.info("Space instance has been elected as Primary");
            moveToPrimary();
        } else {
            if (_logger.isInfoEnabled()) {
                StringBuilder logBuf = new StringBuilder();

                String primaryHost = HostName.getHostNameFrom(activeServiceItem.attributeSets);

                Object service = activeServiceItem.getService();
                logBuf.append("Election resolved: Space instance [").append(service).append("]");
                if (primaryHost != null)
                    logBuf.append(" on [").append(primaryHost).append("] machine");

                logBuf.append(" has been elected to be Primary");

                _logger.info(logBuf.toString());

                if (getSpaceMode() == SpaceMode.BACKUP)
                    _logger.info("Space instance remains Backup");
                else
                    _logger.info("Space instance is Backup");
            }

            setPrimarySpaceItem(activeServiceItem);
            moveToBackup();

        }//if onBackup
    }// onActive()


    /**
     * @see IActiveElectionListener#onSplitBrain(ActiveElectionEvent)
     **/
    @Override
    public void onSplitBrain(ActiveElectionEvent theEvent) {
        // this method is called after split brain
        //on a space which during split brain was backup
        if (_electManager.getState() == ActiveElectionState.State.ACTIVE) {
            onSplitBrainActive(theEvent);
        } else {
            ServiceItem activeServiceItem = theEvent.getActiveServiceItem();
            //check if after split brain resolution the backup primary was changed
            if (_primarySrvItem != null && !_primarySrvItem.serviceID.equals(activeServiceItem.serviceID)) {
                onSplitBrainBackup(theEvent);
            } else {
                StringBuilder logBuf = new StringBuilder();
                String primaryHost = HostName.getHostNameFrom(activeServiceItem.attributeSets);
                Object service = activeServiceItem.getService();
                logBuf.append("Split-Brain resolved: Space instance [").append(service).append("] ");
                if (primaryHost != null)
                    logBuf.append("on [").append(primaryHost).append("] ");

                logBuf.append("has been elected to be Primary");

                _logger.info(logBuf.toString());
                _logger.info("Space instance remains backup");

            }
        }
    }

    @Override
    public void onSplitBrainActive(ActiveElectionEvent theEvent) {
        /* reset the previous primary */
        setPrimarySpaceItem(null);
    }

    @Override
    public void onExtraBackup(ActiveElectionEvent theEvent) {

        ServiceItem activeServiceItem = theEvent.getActiveServiceItem();

        //verify that no election took place before moving to unusable
        if (isPrimary() || (activeServiceItem.serviceID.equals(_space.getUuid()))) {
            _logger.info("Election preceded extra-backup resolution - this Space instance: " + activeServiceItem.getService() + " remains a primary");
            return;
        }

        if (_logger.isInfoEnabled()) {
            _logger.info("Extra-backup resolved: primary Space instance [" + activeServiceItem + "] on host ["
                    + HostName.getHostNameFrom(activeServiceItem.attributeSets) + "] is replicating to a different backup");

            _logger.info("Space instance will discard of all data and become inaccessible");

        }

        setPrimarySpaceItem(null); //reset the previous primary
        setLastError(new ActiveElectionException("Space instance [" + _spaceMember + "] is unusable after network reconnection. It will be redeployed if necessary by the Service Grid"));
        moveToUnusable();
    }

    @Override
    public void onSplitBrainBackup(ActiveElectionEvent theEvent) {
        ServiceItem activeServiceItem = theEvent.getActiveServiceItem();

        if (_logger.isInfoEnabled()) {
            StringBuilder logBuf = new StringBuilder();
            String primaryHost = HostName.getHostNameFrom(activeServiceItem.attributeSets);
            Object service = activeServiceItem.getService();
            logBuf.append("Split-Brain resolved: Space instance [").append(service).append("] ");
            if (primaryHost != null)
                logBuf.append("on [").append(primaryHost).append("] ");

            logBuf.append("has been elected to be Primary");

            _logger.info(logBuf.toString());
            _logger.info("Space instance will discard of all data and recover from the primary [" + service + "]");

        }// if

        setPrimarySpaceItem(activeServiceItem);
        setLastError(new ActiveElectionException("Space [" + _spaceMember + "] is unusable after split brain. It will be redeployed if necessary by the Service Grid"));
        moveToUnusable();
    }


    /**
     * terminate the PrimarySpaceSelector
     */
    synchronized public void terminate() {
        /* is already terminated */
        if (_isTerminated)
            return;

        _isTerminated = true;


        if (_electManager != null)
            _electManager.terminate();

        _namingService.terminate();

        _primarySpaceModeListeners.clear();
    }

    /**
     * Forcefully make this space to be primary
     */
    public void forceMoveToPrimary() throws RemoteException {
        _electManager.forceMoveToPrimary();
    }


    public Throwable getLastError() {
        return _lastError;
    }

    public void setLastError(Throwable lastError) {
        this._lastError = lastError;
    }
}
