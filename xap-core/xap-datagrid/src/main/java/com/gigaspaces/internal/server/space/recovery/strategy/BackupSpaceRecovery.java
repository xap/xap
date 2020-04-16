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

/**
 *
 */
package com.gigaspaces.internal.server.space.recovery.strategy;

import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeReplicaState;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.StorageConsistencyModes;
import com.gigaspaces.internal.server.space.recovery.group.FailoverGroupRecovery;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.exception.internal.InternalInactiveSpaceException;

import java.rmi.RemoteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BackupSpaceRecovery implements SpaceRecoverStrategy {
    //logger
    static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_COMMON);

    protected final SpaceImpl _space;

    private FailoverGroupRecovery _failoverGroup;

    /**
     * @param recoveryManager
     */
    public BackupSpaceRecovery(SpaceImpl space) {
        _space = space;

        ClusterPolicy clusterPolicy = _space.getClusterPolicy();
        if (clusterPolicy == null)
            return;

        if (clusterPolicy.m_FailOverPolicy != null)
            _failoverGroup = new FailoverGroupRecovery(_space);


    }

    /* (non-Javadoc)
     * @see com.j_spaces.core.SpaceRecoverStrategy#recover()
     */
    public ISpaceSynchronizeReplicaState recover()
            throws Exception {
        recoverFromDB();

        boolean recovered = false;
        int directPersistencyRecoverRetryCount = 0;
        ISpaceSynchronizeReplicaState replicaState = null;
        while (!recovered) {
            try {
                replicaState = recoverFromOtherSpace();
                recovered = true;
            } catch (UnavailablePrimarySpaceException e) {
                if (_space.getDirectPersistencyRecoveryHelper() != null) {
                    _space.getDirectPersistencyRecoveryHelper().handleDirectPersistencyRecoverFailure(++directPersistencyRecoverRetryCount);
                }
                if (_logger.isWarnEnabled()) {
                    _logger.warn("Primary space is unavailable.", e);
                }
                if (_space.getEngine().isFailOverDuringRecovery()) {
                    if (_logger.isWarnEnabled())
                        _logger.warn("Primary space failure detected during the recovery process of this space instance - recovery will be aborted");
                    throw e;
                }
                Thread.sleep(5000);
            }
        }
        return replicaState;
    }

    /**
     * Recover all the data from primary space
     */
    protected ISpaceSynchronizeReplicaState recoverAllFromPrimary() throws Exception {
        return recoverFromPrimary(false, false);
    }

    /**
     * Recover transient data from primary space
     */
    protected ISpaceSynchronizeReplicaState recoverTransientFromPrimary() throws Exception {
        return recoverFromPrimary(true, true);
    }

    protected ISpaceSynchronizeReplicaState recoverFromPrimaryMemoryOnly() throws Exception {
        return recoverFromPrimary(false, true);
    }

    /**
     * Recover only from primary in failover group. if primary election is used, only primaries are
     * considered valid targets.
     *
     * @param memoryOnly TODO
     */
    protected ISpaceSynchronizeReplicaState recoverFromPrimary(boolean transientOnly, boolean memoryOnly) throws Exception {
        if (_failoverGroup == null)
            return null;

        ISpaceSynchronizeReplicaState recoveryStatus = _failoverGroup.recover(transientOnly, memoryOnly);


        if (recoveryStatus != null && recoveryStatus.getCopyResult().isFailed()) {
            Exception causeException = recoveryStatus.getCopyResult().getFailureReason();

            // check if no recovery was performed
            if ((causeException instanceof RemoteException || causeException instanceof FinderException || causeException instanceof InternalInactiveSpaceException)
                    && recoveryStatus.getCopyResult().isEmpty())
                throw new UnavailablePrimarySpaceException(causeException);

            throw causeException;
        }


        //if relevant mark the backup as consistent  all data is recovered
        if (_space.getEngine().getSpaceImpl().getDirectPersistencyRecoveryHelper() != null) {
            if (_logger.isInfoEnabled()) {
                _logger.info("[" + _space.getEngine().getFullSpaceName() + "] setting storage state of backup to Consistent after recovery from primary");
            }
            _space.getEngine().getSpaceImpl().getDirectPersistencyRecoveryHelper().setStorageState(StorageConsistencyModes.Consistent);
        }
        return recoveryStatus;
    }


    /* (non-Javadoc)
     * @see com.j_spaces.core.SpaceRecoverStrategy#recoverFromDB()
     */
    public abstract void recoverFromDB() throws Exception;

    /* (non-Javadoc)
     * @see com.j_spaces.core.SpaceRecoverStrategy#recoverFromPrimary()
     */
    public abstract ISpaceSynchronizeReplicaState recoverFromOtherSpace() throws Exception;
}
