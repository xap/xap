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
import com.gigaspaces.cluster.replication.sync.SyncReplPolicy;
import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogConfig.LimitReachedPolicy;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationOutFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.ISpaceCopyReplicaInFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.ISpaceCopyReplicaOutFilter;
import com.j_spaces.core.cluster.RedoLogCompaction;
import com.j_spaces.core.cluster.ReplicationPolicy;
import com.j_spaces.core.cluster.ReplicationProcessingType;
import com.j_spaces.core.cluster.SwapBacklogConfig;


@com.gigaspaces.api.InternalApi
public class ReplicationSettings
        implements IReplicationSettings, ISyncReplicationSettings {

    private int operationsReplicationThreshold = ReplicationPolicy.DEFAULT_REPL_INTERVAL_OPERS;
    private int batchSize = ReplicationPolicy.DEFAULT_REPL_CHUNK_SIZE;
    private long idleDelay = ReplicationPolicy.DEFAULT_REPL_INTERVAL_MILLIS;
    private LimitReachedPolicy limitReachedPolicy = LimitReachedPolicy.BLOCK_NEW;
    private long maxRedoLogCapacity = ReplicationPolicy.DEFAULT_MAX_REDO_LOG_CPACITY;
    private long maxRedoLogCapacityDuringRecovery = ReplicationPolicy.DEFAULT_MAX_REDO_LOG_CPACITY;
    private long maxRedoLogMemoryCapacity = ReplicationPolicy.DEFAULT_MAX_REDO_LOG_CPACITY;
    private boolean isNetworkCompressionEnabled = ReplicationPolicy.DEFAULT_REPL_NETWORK_COMPRESSION;
    private IReplicationInFilter inFilter;
    private IReplicationOutFilter outFilter;
    private ISpaceCopyReplicaInFilter spaceCopyInFilter;
    private ISpaceCopyReplicaOutFilter spaceCopyOutFilter;
    private boolean isOneWay;
    private boolean throttleWhenInactive = SyncReplPolicy.DEFAULT_THROTTLE_WHEN_INACTIVE;
    private int maxThrottleTPWhenInactive = SyncReplPolicy.DEFAULT_MAX_THROTTLE_TP_WHEN_INACTIVE;
    private int minThrottleTPWhenActive = SyncReplPolicy.DEFAULT_MIN_THROTTLE_TP_WHEN_ACTIVE;
    private ReplicationProcessingType processingType = ReplicationPolicy.DEFAULT_PROCESSING_TYPE;
    private short bucketCount = MultiBucketReplicationPolicy.DEFAULT_BUCKETS_COUNT;
    private Integer batchParallelFactor = MultiBucketReplicationPolicy.DEFAULT_BATCH_PARALLEL_FACTOR;
    private int batchParallelThreshold = MultiBucketReplicationPolicy.DEFAULT_BATCH_PARALLEL_THRESHOLD;
    private long consumeTimeout = SyncReplPolicy.DEFAULT_TARGET_CONSUME_TIMEOUT;
    private SwapBacklogConfig swapBacklogSettings = new SwapBacklogConfig();
    private ConsistencyLevel consistencyLevel = SyncReplPolicy.DEFAULT_CONSISTENCY_LEVEL;
    private String backlogWeightPolicy = ReplicationPolicy.DEFAULT_BACKLOG_WEIGHT_POLICY;
    private RedoLogCompaction redoLogCompaction = ReplicationPolicy.DEFAULT_REDO_LOG_COMPACTION;

    public int getOperationsReplicationThreshold() {
        return operationsReplicationThreshold;
    }

    public void setOperationsReplicationThreshold(
            int operationsReplicationThreshold) {
        this.operationsReplicationThreshold = operationsReplicationThreshold;
    }

    public long getIdleDelay() {
        return idleDelay;
    }

    public void setIdleDelay(long replicationInterval) {
        this.idleDelay = replicationInterval;
    }

    public LimitReachedPolicy getLimitReachedPolicy() {
        return limitReachedPolicy;
    }

    public void setLimitReachedPolicy(LimitReachedPolicy limitReachedPloicy) {
        this.limitReachedPolicy = limitReachedPloicy;
    }

    public long getMaxRedoLogCapacity() {
        return maxRedoLogCapacity;
    }

    public void setMaxRedoLogCapacity(long maxRedoLogCapacity) {
        this.maxRedoLogCapacity = maxRedoLogCapacity;
    }

    public long getMaxRedoLogCapacityDuringRecovery() {
        return maxRedoLogCapacityDuringRecovery;
    }

    public void setMaxRedoLogCapacityDuringRecovery(
            long maxRedoLogCapacityDuringRecovery) {
        this.maxRedoLogCapacityDuringRecovery = maxRedoLogCapacityDuringRecovery;
    }

    public long getMaxRedoLogMemoryCapacity() {
        return maxRedoLogMemoryCapacity;
    }

    public void setMaxRedoLogMemoryCapacity(long maxRedoLogMemoryCapacity) {
        this.maxRedoLogMemoryCapacity = maxRedoLogMemoryCapacity;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public boolean isOneWay() {
        return isOneWay;
    }

    public void setOneWay(boolean isOneWay) {
        this.isOneWay = isOneWay;
    }

    public IReplicationInFilter getInFilter() {
        return inFilter;
    }

    public IReplicationOutFilter getOutFilter() {
        return outFilter;
    }

    public ISpaceCopyReplicaInFilter getSpaceCopyInFilter() {
        return spaceCopyInFilter;
    }

    public ISpaceCopyReplicaOutFilter getSpaceCopyOutFilter() {
        return spaceCopyOutFilter;
    }

    public ISyncReplicationSettings getSyncReplicationSettings() {
        return this;
    }

    public boolean isThrottleWhenInactive() {
        return throttleWhenInactive;
    }

    public void setThrottleWhenInactive(boolean throttleWhenInactive) {
        this.throttleWhenInactive = throttleWhenInactive;
    }

    public int getMaxThrottleTPWhenInactive() {
        return maxThrottleTPWhenInactive;
    }

    public void setMaxThrottleTPWhenInactive(int maxThrottleTPWhenInactive) {
        this.maxThrottleTPWhenInactive = maxThrottleTPWhenInactive;
    }

    public int getMinThrottleTPWhenActive() {
        return minThrottleTPWhenActive;
    }

    public void setMinThrottleTPWhenActive(int minThrottleTPWhenActive) {
        this.minThrottleTPWhenActive = minThrottleTPWhenActive;
    }

    public void setProcessindType(ReplicationProcessingType processindType) {
        this.processingType = processindType;
    }

    public ReplicationProcessingType getProcessingType() {
        return processingType;
    }

    public void setBucketCount(short bucketCount) {
        this.bucketCount = bucketCount;
    }

    public short getBucketCount() {
        return bucketCount;
    }

    public void setBatchParallelFactor(int batchParallelFactor) {
        this.batchParallelFactor = batchParallelFactor;
    }

    public int getBatchParallelFactor() {
        if (batchParallelFactor == null)
            return Runtime.getRuntime().availableProcessors();
        return batchParallelFactor.intValue();
    }

    public void setBatchParallelThreshold(int batchParallelThreshold) {
        this.batchParallelThreshold = batchParallelThreshold;
    }

    public int getBatchParallelThreshold() {
        return batchParallelThreshold;
    }

    public long getConsumeTimeout() {
        return consumeTimeout;
    }

    public void setConsumeTimeout(long consumeTimeout) {
        this.consumeTimeout = consumeTimeout;
    }

    public SwapBacklogConfig getSwapBacklogSettings() {
        return swapBacklogSettings;
    }

    @Override
    public String getBacklogWeightPolicy() {
        return backlogWeightPolicy;
    }

    @Override
    public RedoLogCompaction getRedoLogCompaction() {
        return redoLogCompaction;
    }

    @Override
    public boolean isNetworkCompressionEnabled() {
        return isNetworkCompressionEnabled;
    }

    public void setRedoLogCompaction(RedoLogCompaction redoLogCompaction) {
        this.redoLogCompaction = redoLogCompaction;
    }

    public void setBacklogWeightPolicy(String backlogWeightPolicy) {
        this.backlogWeightPolicy = backlogWeightPolicy;
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }
}
