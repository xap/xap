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

package com.gigaspaces.internal.server.space.replication;

import com.gigaspaces.cluster.replication.IncomingReplicationOutOfSyncException;
import com.gigaspaces.internal.cluster.node.AbstractReplicationNodeStateListener;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogMemberState;
import com.gigaspaces.internal.cluster.node.impl.groups.BrokenReplicationTopologyException;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.utils.StringUtils;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.exception.internal.InternalInactiveSpaceException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationNodeStateListener extends AbstractReplicationNodeStateListener {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_ENGINE);

    private final SpaceEngine _spaceEngine;
    private final String _fullSpaceName;
    private final ClusterPolicy _clusterPolicy;
    private final SpaceImpl _spaceImpl;

    public ReplicationNodeStateListener(SpaceEngine spaceEngine) {
        this._spaceEngine = spaceEngine;
        this._fullSpaceName = spaceEngine.getFullSpaceName();
        this._clusterPolicy = spaceEngine.getClusterPolicy();
        this._spaceImpl = spaceEngine.getSpaceImpl();
    }

    @Override
    public boolean onTargetChannelOutOfSync(String groupName, String channelSourceLookupName, IncomingReplicationOutOfSyncException outOfSyncReason) {
        String message = "Space [" + _fullSpaceName + "] received out of sync indication from replication group [" + groupName + "] source member [" + channelSourceLookupName + "]";
        //Only backup is allowed to recover, other should ignore
        if (_spaceImpl.isPrimary()) {
            message += StringUtils.NEW_LINE + "The current space is an active space, it will resynchronize with the source and may lose packets";
            if (_logger.isErrorEnabled())
                _logger.error(message);
            return true;
        }

        if (_clusterPolicy != null) {
            switch (_clusterPolicy.getReplicationPolicy().getOnMissingPackets()) {
                //If set to ignore we can resync, otherwise we can't
                case IGNORE: {
                    message += StringUtils.NEW_LINE + "The current space is an inactive space (backup), it is set to ignore missing packets, it will resynchronize with the source and may lose packets";
                    if (_logger.isErrorEnabled())
                        _logger.error(message);
                    return true;
                }
                case RECOVER: {
                    message += StringUtils.NEW_LINE + "The current space is an inactive space (backup), it will be restarted and perform full recovery from its primary space";
                    if (_logger.isWarnEnabled())
                        _logger.warn(message);
                    _spaceEngine.setReplicationUnhealthyReason(outOfSyncReason);
                    return false;
                }
            }
        }

        _spaceEngine.setReplicationUnhealthyReason(outOfSyncReason);
        return false;
    }

    @Override
    public void onTargetChannelBacklogDropped(String groupName, String channelSourceLookupName, IBacklogMemberState memberState) {
        if (_logger.isWarnEnabled())
            _logger.warn("Space [" + _fullSpaceName + "] received backlog dropped notification from replication group [" + groupName + "] source member [" + channelSourceLookupName + "]"
                    + StringUtils.NEW_LINE + "Current space will be restarted and perform full recovery from its primary space");
        _spaceEngine.setReplicationUnhealthyReason(new IncomingReplicationOutOfSyncException("Replication is out of sync, backlog was dropped by source, replication state " + memberState.toLogMessage()));
    }

    @Override
    public void onSourceBrokenReplicationTopology(String groupName, String channelTargetMemberName, BrokenReplicationTopologyException error) {
    }

    @Override
    public void onSourceChannelActivated(String groupName, String memberName) {
    }

    @Override
    public void onTargetChannelCreationValidation(String groupName, String sourceMemberName, Object sourceUniqueId) {
        //Regular space has no custom validation logic
    }

    @Override
    public void onNewReplicaRequest(String groupName, String memberName, boolean isSynchronizeRequest) {
        //Only primary can be recovered from - otherwise throw an exception
        //this can happen right before backup turns into primary
        if (isSynchronizeRequest && !_spaceImpl.isPrimary()) {
            String message = "Space [" + _fullSpaceName + "] received new replica request from replication group [" + groupName + "] , source member [" + memberName + "]";
            message += StringUtils.NEW_LINE + "The current space is not an active space. New replica request was denied.";
            if (_logger.isWarnEnabled())
                _logger.warn(message);

            throw new InternalInactiveSpaceException(message);
        }
    }
}
