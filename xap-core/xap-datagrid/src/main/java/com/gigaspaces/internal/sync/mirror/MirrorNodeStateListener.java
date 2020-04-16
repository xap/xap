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

package com.gigaspaces.internal.sync.mirror;

import com.gigaspaces.cluster.replication.IncomingReplicationOutOfSyncException;
import com.gigaspaces.internal.cluster.node.AbstractReplicationNodeStateListener;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogMemberState;
import com.gigaspaces.internal.cluster.node.impl.groups.BrokenReplicationTopologyException;
import com.gigaspaces.internal.cluster.node.impl.groups.ReplicationSourceAlreadyAttachedException;
import com.gigaspaces.internal.utils.StringUtils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class MirrorNodeStateListener extends AbstractReplicationNodeStateListener {

    private static final Logger _mirrorLogger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_MIRROR_REPLICATION);

    private final String _fullSpaceName;
    private final Object _lock;
    private final boolean _stronglyTypedSourceClustername;
    private String _firstAttachedClusterName;

    public MirrorNodeStateListener(String fullSpaceName, MirrorConfig mirrorConfig) {
        this._fullSpaceName = fullSpaceName;
        this._lock = new Object();
        this._stronglyTypedSourceClustername = StringUtils.hasLength(mirrorConfig.getClusterName());
    }

    @Override
    public void onTargetChannelBacklogDropped(String groupName, String channelSourceLooString, IBacklogMemberState memberState) {
        if (_mirrorLogger.isWarnEnabled())
            _mirrorLogger.warn("Mirror service [" + _fullSpaceName + "] received channel backlog dropped message which is not allowed");
    }

    @Override
    public void onSourceBrokenReplicationTopology(String groupName, String channelTargetMemberName, BrokenReplicationTopologyException error) {
        if (_mirrorLogger.isDebugEnabled())
            _mirrorLogger.debug("Mirror service [" + _fullSpaceName + "] received source channel broken replication topology message which is not allowed");
    }

    @Override
    public void onSourceChannelActivated(String groupName, String memberName) {
        if (_mirrorLogger.isDebugEnabled())
            _mirrorLogger.debug("Mirror service [" + _fullSpaceName + "] received source channel activated message which is not allowed");
    }

    @Override
    public void onNewReplicaRequest(String groupName, String memberName, boolean isSynchronizeRequest) {
        String errorMessage = "Mirror service [" + _fullSpaceName + "] received new replica request from replication group [" + groupName + "] source member [" + memberName + "],"
                + StringUtils.NEW_LINE + " The operation is not supported.";
        if (_mirrorLogger.isErrorEnabled())
            _mirrorLogger.error(errorMessage);

        throw new UnsupportedOperationException(errorMessage);
    }

    @Override
    public boolean onTargetChannelOutOfSync(String groupName, String channelSourceLookupName, IncomingReplicationOutOfSyncException outOfSyncReason) {
        //Mirror will tolerate out of sync since there's nothing that can be done in order to fix that state
        if (_mirrorLogger.isErrorEnabled())
            _mirrorLogger.error("Mirror service [" + _fullSpaceName + "] received out of sync indication from replication group [" + groupName + "] source member [" + channelSourceLookupName + "],"
                    + StringUtils.NEW_LINE + "it will resynchronize with the source space and may lose packets", outOfSyncReason);
        return true;
    }

    @Override
    public void onTargetChannelCreationValidation(String groupName, String sourceMemberName, Object sourceUniqueId) {
        //No validation required, the mirror has a strongly typed source cluster name
        if (_stronglyTypedSourceClustername)
            return;

        synchronized (_lock) {
            if (_firstAttachedClusterName == null) {
                _firstAttachedClusterName = extractClusterName(sourceMemberName);
                if (_mirrorLogger.isDebugEnabled())
                    _mirrorLogger.debug("Mirror service [" + _fullSpaceName + "] first connected cluster is " + _firstAttachedClusterName + ", the mirror will only accept replication from that cluster");
                return;
            }

            String clusterName = extractClusterName(sourceMemberName);
            if (!clusterName.equals(_firstAttachedClusterName))
                throw new ReplicationSourceAlreadyAttachedException("Received incompatible incoming channel creation "
                        + "request from replication group ["
                        + groupName
                        + "] and member name ["
                        + sourceMemberName
                        + "]. "
                        + "The mirror is already attached to cluster "
                        + "["
                        + _firstAttachedClusterName
                        + "] and it cannot create a channel to a member of cluster "
                        + "[" + clusterName + "]");
        }

    }

    private String extractClusterName(String sourceMemberName) {
        int indexOf = sourceMemberName.indexOf("_container");
        return sourceMemberName.substring(0, indexOf);
    }
}
