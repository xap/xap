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

package com.gigaspaces.internal.cluster.node.impl.config;

import com.gigaspaces.cluster.replication.ConsistencyLevel;
import com.gigaspaces.cluster.replication.ReplicationFilterManager;
import com.gigaspaces.cluster.replication.sync.SyncReplPolicy;
import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogConfig.LimitReachedPolicy;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationOutFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.ISpaceCopyReplicaInFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.ISpaceCopyReplicaOutFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.ReplicationInFilterAdapter;
import com.gigaspaces.internal.cluster.node.impl.filters.ReplicationOutFilterAdapter;
import com.j_spaces.core.cluster.*;

import java.util.List;


@com.gigaspaces.api.InternalApi
public class ReplicationSettingsAdapter
        implements ISpaceReplicationSettings, ISyncReplicationSettings {

    private final ReplicationPolicy _policy;
    private final ReplicationFilterManager _filterManager;

    public ReplicationSettingsAdapter(ReplicationPolicy policy,
                                      ReplicationFilterManager filterManager) {
        _policy = policy;
        _filterManager = filterManager;
    }

    public String getSpaceMemberName() {
        return _policy.m_OwnMemberName;
    }

    public List<String> getGroupMemberNames() {
        return _policy.m_ReplicationGroupMembersNames;
    }

    public long getIdleDelay() {
        return _policy.m_ReplicationIntervalMillis;
    }

    public int getOperationsReplicationThreshold() {
        return _policy.m_ReplicationChunkSize;
    }

    public LimitReachedPolicy getLimitReachedPolicy() {
        LimitReachedPolicy limitReachedPolicy;
        if (_policy.getOnRedoLogCapacityExceeded() == RedoLogCapacityExceededPolicy.BLOCK_OPERATIONS) {
            limitReachedPolicy = LimitReachedPolicy.BLOCK_NEW;
        } else {
            if (_policy.getOnMissingPackets() == MissingPacketsPolicy.RECOVER)
                limitReachedPolicy = LimitReachedPolicy.DROP_UNTIL_RESYNC;
            else
                limitReachedPolicy = LimitReachedPolicy.DROP_OLDEST;
        }
        return limitReachedPolicy;

    }

    public long getMaxRedoLogCapacity() {
        return _policy.getMaxRedoLogCapacity();
    }

    public long getMaxRedoLogCapacityDuringRecovery() {
        return _policy.getMaxRedoLogRecoveryCapacity();
    }

    public long getMaxRedoLogMemoryCapacity() {
        return _policy.getMaxRedoLogMemoryCapacity();
    }

    @Override
    public RedoLogCompaction getRedoLogCompaction() {
        return _policy.getRedoLogCompaction();
    }

    @Override
    public boolean isNetworkCompressionEnabled() {
        return _policy.isNetworkCompressionEnabled();
    }

    @Override
    public String getBacklogWeightPolicy() {
        return _policy.getBacklogWeightPolicy();
    }

    public boolean isReliableAsyncRepl() {
        return _policy.isReliableAsyncRepl();
    }

    public boolean isMirrorServiceEnabled() {
        return _policy.isMirrorServiceEnabled();
    }

    public boolean isSync() {
        return _policy.m_IsSyncReplicationEnabled;
    }

    public int getBatchSize() {
        return _policy.m_ReplicationChunkSize;
    }

    public boolean isOneWay() {
        return _policy.isOneWayReplication;
    }

    public IReplicationOutFilter getOutFilter() {
        return _filterManager.getOutputFilter() != null ? new ReplicationOutFilterAdapter(_filterManager)
                : null;
    }

    public IReplicationInFilter getInFilter() {
        return _filterManager.getInputFilter() != null ? new ReplicationInFilterAdapter(_filterManager)
                : null;
    }

    public ISpaceCopyReplicaOutFilter getSpaceCopyOutFilter() {
        return _filterManager.getOutputFilter() != null ? new ReplicationOutFilterAdapter(_filterManager)
                : null;
    }

    public ISpaceCopyReplicaInFilter getSpaceCopyInFilter() {
        return _filterManager.getInputFilter() != null ? new ReplicationInFilterAdapter(_filterManager)
                : null;
    }

    public ISyncReplicationSettings getSyncReplicationSettings() {
        return this;
    }

    public boolean isThrottleWhenInactive() {
        SyncReplPolicy syncReplPolicy = _policy.m_SyncReplPolicy;
        if (syncReplPolicy != null)
            return syncReplPolicy.isThrottleWhenInactive();
        return SyncReplPolicy.DEFAULT_THROTTLE_WHEN_INACTIVE;
    }

    public int getMaxThrottleTPWhenInactive() {
        SyncReplPolicy syncReplPolicy = _policy.m_SyncReplPolicy;
        if (syncReplPolicy != null)
            return syncReplPolicy.getMaxThrottleTPWhenInactive();

        return SyncReplPolicy.DEFAULT_MAX_THROTTLE_TP_WHEN_INACTIVE;
    }

    public int getMinThrottleTPWhenActive() {
        SyncReplPolicy syncReplPolicy = _policy.m_SyncReplPolicy;
        if (syncReplPolicy != null)
            return syncReplPolicy.getMinThrottleTPWhenActive();

        return SyncReplPolicy.DEFAULT_MIN_THROTTLE_TP_WHEN_ACTIVE;
    }

    public ReplicationProcessingType getProcessingType() {
        return _policy.getProcessingType();
    }

    public short getBucketCount() {
        MultiBucketReplicationPolicy multiBucketReplicationPolicy = _policy.getMultiBucketReplicationPolicy();
        if (multiBucketReplicationPolicy != null)
            return multiBucketReplicationPolicy.getBucketCount();

        return MultiBucketReplicationPolicy.DEFAULT_BUCKETS_COUNT;
    }

    public int getBatchParallelFactor() {
        MultiBucketReplicationPolicy multiBucketReplicationPolicy = _policy.getMultiBucketReplicationPolicy();
        if (multiBucketReplicationPolicy != null && multiBucketReplicationPolicy.getBatchParallelFactor() != null)
            return multiBucketReplicationPolicy.getBatchParallelFactor();

        return Runtime.getRuntime().availableProcessors();
    }

    public int getBatchParallelThreshold() {
        MultiBucketReplicationPolicy multiBucketReplicationPolicy = _policy.getMultiBucketReplicationPolicy();
        if (multiBucketReplicationPolicy != null)
            return multiBucketReplicationPolicy.getBucketCount();

        return MultiBucketReplicationPolicy.DEFAULT_BUCKETS_COUNT;
    }

    public long getConsumeTimeout() {
        if (_policy.m_SyncReplPolicy == null)
            return SyncReplPolicy.DEFAULT_TARGET_CONSUME_TIMEOUT;
        return _policy.m_SyncReplPolicy.getTargetConsumeTimeout();
    }

    public SwapBacklogConfig getSwapBacklogSettings() {
        return _policy.getSwapRedologPolicy();
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        if (_policy.m_SyncReplPolicy == null)
            return SyncReplPolicy.DEFAULT_CONSISTENCY_LEVEL;
        return _policy.m_SyncReplPolicy.getConsistencyLevel();
    }
}
