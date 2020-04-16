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

import com.gigaspaces.internal.cluster.node.replica.ISpaceCopyResult;
import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeReplicaState;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.server.blobstore.BlobStoreException;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.SpaceURL;

import java.rmi.RemoteException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Recovery super class
 *
 * @author anna
 * @since 6.0
 */
public abstract class RecoveryGroup {
    //logger
    static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_COMMON);

    protected final SpaceImpl _space;

    /**
     * @param space
     */
    public RecoveryGroup(SpaceImpl space) {
        _space = space;
    }

    public abstract ISpaceSynchronizeReplicaState recover(boolean transientOnly, boolean memoryOnly) throws Exception;

    /**
     * Perform memory recovery to this space- try each member of the recovery group.
     *
     * @param transientOnly if set to true only transient entries will be recovered
     * @param memoryOnly    TODO
     * @return SpaceCopyStatus This object contains status info of copied operation.
     */
    public ISpaceSynchronizeReplicaState recover(List<SpaceURL> recTargets,
                                                 boolean transientOnly, boolean memoryOnly) {
        // check is this space supports memory recovery
        if (!_space.getEngine().isMemoryRecoveryEnabled())
            return null;

        /* use every replication peer to try and perform memory recovery*/
        int recoveryChunkSize = _space.getClusterPolicy().m_ReplicationPolicy.getRecoveryChunkSize();
        ISpaceSynchronizeReplicaState recoveryStatus = null;

        for (SpaceURL remoteSpaceURL : recTargets) {

            recoveryStatus = recoverFromSpace(remoteSpaceURL,
                    transientOnly,
                    memoryOnly,
                    recoveryChunkSize);

            if (recoveryStatus.getCopyResult().isSuccessful())
                break;// Found recoverable member - break

            // if recovery failed to recover - stop recovery
            // if failed because no recovery member was found - try another member
            if (!(recoveryStatus.getCopyResult().getFailureReason() instanceof FinderException || recoveryStatus.getCopyResult().getFailureReason() instanceof RemoteException))
                return recoveryStatus;

        }// for

        return recoveryStatus;
    }

    /**
     * Try to recover from given space
     */
    private ISpaceSynchronizeReplicaState recoverFromSpace(SpaceURL remoteSpaceURL,
                                                           boolean transientOnly, boolean memoryOnly, int recoveryChunkSize) {

        if (_logger.isInfoEnabled()) {
            _logger.info("Space [" + _space.getServiceName()
                    + "] trying to perform recovery from [" + remoteSpaceURL
                    + "]. RecoveryChunkSize=" + recoveryChunkSize);
        }

        /** starting to perform memory-recovery */
        ISpaceSynchronizeReplicaState spaceSynchronizeReplica = _space.getEngine().spaceSynchronizeReplica(remoteSpaceURL,
                recoveryChunkSize, transientOnly, memoryOnly);

        /** clean source space if the source space recovered some data where the target space failed */
        ISpaceCopyResult copyResult = spaceSynchronizeReplica.getCopyResult();
        if (copyResult.isFailed() && !copyResult.isEmpty()) {
            // Do not try to clean and start a new engine in blobstore case GS-11844
            if (_space.getEngine().getCacheManager().isBlobStoreCachePolicy()) {
                Exception failureReason = copyResult.getFailureReason();

                String err = ("Space [" + _space.getServiceName()
                        + "] failed to perform recovery from [" + remoteSpaceURL
                        + "] reason=" + (failureReason != null ? failureReason.getCause() : "[]") + " space is disabled since it cannot restart in BlobStore cache-policy");
                _logger.error(err);
                try {
                    _space.shutdown();
                } catch (RemoteException e) {
                }
                throw new BlobStoreException(err);
            }
            _space.internalClean(true /*isInternalUse*/, false /*isRestart*/, false/*isWarmInit*/);
        }
        return spaceSynchronizeReplica;

    }

}
