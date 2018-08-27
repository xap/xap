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

import com.gigaspaces.cluster.replication.MirrorServiceConfig;
import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogConfig.LimitReachedPolicy;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationOutFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.ISpaceCopyReplicaInFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.ISpaceCopyReplicaOutFilter;
import com.j_spaces.core.cluster.RedoLogCapacityExceededPolicy;
import com.j_spaces.core.cluster.RedoLogCompaction;
import com.j_spaces.core.cluster.ReplicationProcessingType;
import com.j_spaces.core.cluster.SwapBacklogConfig;

import java.util.Set;


@com.gigaspaces.api.InternalApi
public class MirrorReplicationSettingsAdapter
        implements IMirrorChannelReplicationSettings {

    private final MirrorServiceConfig _policy;

    public MirrorReplicationSettingsAdapter(MirrorServiceConfig policy) {
        _policy = policy;
    }

    public String getMirrorMemberName() {
        return _policy.memberName;
    }


    public long getIdleDelay() {
        return _policy.intervalMillis;
    }

    public int getOperationsReplicationThreshold() {
        return _policy.intervalOpers;
    }

    public LimitReachedPolicy getLimitReachedPolicy() {
        LimitReachedPolicy limitReachedPolicy;
        if (_policy.onRedoLogCapacityExceeded == RedoLogCapacityExceededPolicy.BLOCK_OPERATIONS) {
            limitReachedPolicy = LimitReachedPolicy.BLOCK_NEW;
        } else {
            limitReachedPolicy = LimitReachedPolicy.DROP_OLDEST;
        }
        return limitReachedPolicy;
    }

    public long getMaxRedoLogCapacity() {
        return _policy.maxRedoLogCapacity == null ? -1 : _policy.maxRedoLogCapacity;
    }

    public boolean isReliableAsyncRepl() {
        return true;
    }

    public boolean isMirrorServiceEnabled() {
        return false;
    }

    public boolean isSync() {
        return false;
    }

    public int getBatchSize() {
        return _policy.bulkSize;
    }

    public IReplicationOutFilter getOutFilter() {
        return null;
    }

    public IReplicationInFilter getInFilter() {
        return null;
    }

    public ISpaceCopyReplicaInFilter getSpaceCopyInFilter() {
        return null;
    }

    public ISpaceCopyReplicaOutFilter getSpaceCopyOutFilter() {
        return null;
    }

    public boolean isOneWay() {
        return false;
    }

    public ISyncReplicationSettings getSyncReplicationSettings() {
        return null;
    }

    public ReplicationProcessingType getProcessingType() {
        return null;
    }

    public short getBucketCount() {
        return 0;
    }

    public int getBatchParallelFactor() {
        return 0;
    }

    public int getBatchParallelThreshold() {
        return 0;
    }

    public long getConsumeTimeout() {
        return 0;
    }

    public SwapBacklogConfig getSwapBacklogSettings() {
        return null;
    }

    @Override
    public String getBacklogWeightPolicy() {
        return null;
    }

    @Override
    public RedoLogCompaction getRedoLogCompaction() {
        return null;
    }

    @Override
    public boolean isNetworkCompressionEnabled() {
        return true;
    }

    @Override
    public boolean supportsChange() {
        return _policy.supportedChangeOperations != null;
    }

    @Override
    public Set<String> getSupportedChangeOperations() {
        return _policy.supportedChangeOperations;
    }

}
