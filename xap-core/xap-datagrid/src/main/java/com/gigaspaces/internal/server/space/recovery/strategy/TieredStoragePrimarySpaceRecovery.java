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

package com.gigaspaces.internal.server.space.recovery.strategy;

import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeReplicaState;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.recovery.group.CompositeRecoveryGroup;
import com.gigaspaces.internal.server.space.recovery.group.FailoverGroupRecovery;
import com.gigaspaces.internal.server.space.recovery.group.LoadBalancingGroupRecovery;
import com.gigaspaces.internal.server.space.recovery.group.ReplicationGroupRecovery;
import com.j_spaces.core.cluster.ClusterPolicy;


/**
 * @author yael
 * @since 16.0
 */
@com.gigaspaces.api.InternalApi
public class TieredStoragePrimarySpaceRecovery
        implements SpaceRecoverStrategy {


    private final CompositeRecoveryGroup _recoveryGroup;
    private final SpaceImpl _space;


    public TieredStoragePrimarySpaceRecovery(SpaceImpl space, SpaceMode spaceMode) {
        _space = space;

        _recoveryGroup = new CompositeRecoveryGroup(_space);

        ClusterPolicy clusterPolicy = _space.getClusterPolicy();
        if (clusterPolicy == null)
            return;

        if(!spaceMode.equals(SpaceMode.PRIMARY)){
            if (clusterPolicy.m_FailOverPolicy != null)
                _recoveryGroup.add(new FailoverGroupRecovery(_space));
        }

        if (clusterPolicy.m_ReplicationPolicy != null)
            _recoveryGroup.add(new ReplicationGroupRecovery(_space));

        if (clusterPolicy.m_LoadBalancingPolicy != null)
            _recoveryGroup.add(new LoadBalancingGroupRecovery(_space));

    }

    /* (non-Javadoc)
     * @see com.j_spaces.core.SpaceRecoverStrategy#recover()
     */
    public ISpaceSynchronizeReplicaState recover() throws Exception {
        recoverFromDB();
        return recoverFromOtherSpace();
    }

    /* (non-Javadoc)
     * @see com.j_spaces.core.SpaceRecoverStrategy#recoverFromDB()
     */
    public void recoverFromDB() throws Exception {
        _space.initAndRecoverFromDataStorage(true);

    }

    /**
     * Recover data for primary space. 1. try to recover in replication group . 2. try to recover
     * notify templates in LB group
     */
    public ISpaceSynchronizeReplicaState recoverFromOtherSpace() throws Exception {
        // check if all the entries should be recovered,or only the transient ones 
        // if the engine didn't load any data - recover everything from another space
        // if space was started and data was loaded from database,
        // only transient entries should be copied from the target space because all persistent entries
        // were be retrieved from the DB.
        boolean transientOnly = !_space.getEngine()
                .isColdStart();

        return _recoveryGroup.recover(transientOnly, transientOnly);

    }

}