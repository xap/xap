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

package com.gigaspaces.internal.server.space.recovery.group;

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.cluster.node.replica.ISpaceCopyReplicaState;
import com.gigaspaces.internal.cluster.node.replica.ISpaceCopyResult;
import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeReplicaState;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.j_spaces.core.UnknownTypesException;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.SpaceFinder;
import com.j_spaces.core.client.SpaceURL;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * {@link LoadBalancingGroupRecovery} defines how to recover in load balancing group
 *
 * @author anna
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class LoadBalancingGroupRecovery extends RecoveryGroup {
    /**
     * @param space
     */
    public LoadBalancingGroupRecovery(SpaceImpl space) {
        super(space);

    }


    @Override
    public ISpaceSynchronizeReplicaState recover(boolean transientOnly, boolean memoryOnly) {

        if (!_space.getClusterPolicy().m_NotifyRecovery)
            return null;

        List<String> excludeTargets = getExcludeTargets();

        List<SpaceURL> recTargets = _space.getClusterPolicy().m_LoadBalancingPolicy.getRecoverableTargets(_space.getClusterPolicy().m_ClusterName,
                _space.getServiceName(), excludeTargets);

        for (SpaceURL remoteSpaceURL : recTargets) {
            try {
                ISpaceCopyReplicaState spaceCopyReplica = _space.getEngine()
                        .spaceBroadcastNotifyTemplatesReplica(remoteSpaceURL /* remoteMember */,
                                1,
                                true);
                ISpaceCopyResult result = spaceCopyReplica.getCopyResult();

                if (result.isSuccessful()) {
                    if (_logger.isInfoEnabled()) {
                        _logger.info("Space [" + _space.getServiceName()
                                + "] recovered notify templates from ["
                                + remoteSpaceURL + "]");
                    }
                    if(_space.pullBroadcastTables()) {
                        if (_logger.isInfoEnabled()) {
                            _logger.info("Space [" + _space.getServiceName()
                                    + "] recovered broadcast table entries");
                        }
                        return new CopyOnlySynchronizeSpaceReplicate(spaceCopyReplica);
                    }
                    else{
                        //TODO
                    }
                } else
                    _space.getEngine().rollbackCopyReplica(result);
            } catch (InterruptedException e) {
                //Restore interrupted state
                Thread.currentThread().interrupt();
                break;
            }
        }// for
        return null;
    }


    private IDirectSpaceProxy findTargetProxy(SpaceURL remoteSpaceURL)
            throws FinderException {
        IDirectSpaceProxy abstractSpaceProxy;
        /** get remote space object */
        long findTimeout = 0;

        /** get find timeout from replication or fail-over policy */
        if (_space.getEngine().getClusterPolicy().m_ReplicationPolicy != null)
            findTimeout = _space.getEngine().getClusterPolicy().m_ReplicationPolicy.m_SpaceFinderTimeout;
        else if (_space.getEngine().getClusterPolicy().m_FailOverPolicy != null)
            findTimeout = _space.getEngine().getClusterPolicy().m_FailOverPolicy.spaceFinderTimeout;

        remoteSpaceURL.setProperty(SpaceURL.TIMEOUT, String.valueOf(findTimeout));
        abstractSpaceProxy = (IDirectSpaceProxy) SpaceFinder.find(remoteSpaceURL);
        return abstractSpaceProxy;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.core.RecoveryGroup#getExcludeTargets()
     */
    public List<String> getExcludeTargets() {
        // Exclude the failover and replication members
        List<String> excludeTargets = new LinkedList<String>();

        if (_space.getClusterPolicy().m_FailOverPolicy != null)
            excludeTargets.addAll(_space.getClusterPolicy().m_FailOverPolicy.getElectionGroupMembers());

        if (_space.getClusterPolicy().m_ReplicationPolicy != null)
            excludeTargets.addAll(_space.getClusterPolicy().m_ReplicationPolicy.m_ReplicationGroupMembersNames);

        return excludeTargets;
    }


}
