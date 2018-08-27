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

import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogConfig.LimitReachedPolicy;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationOutFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.ISpaceCopyReplicaInFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.ISpaceCopyReplicaOutFilter;
import com.j_spaces.core.cluster.RedoLogCompaction;
import com.j_spaces.core.cluster.ReplicationProcessingType;
import com.j_spaces.core.cluster.SwapBacklogConfig;


public interface IReplicationSettings {

    long getIdleDelay();

    int getOperationsReplicationThreshold();

    int getBatchSize();

    LimitReachedPolicy getLimitReachedPolicy();

    long getMaxRedoLogCapacity();

    IReplicationOutFilter getOutFilter();

    IReplicationInFilter getInFilter();

    ISpaceCopyReplicaInFilter getSpaceCopyInFilter();

    ISpaceCopyReplicaOutFilter getSpaceCopyOutFilter();

    ISyncReplicationSettings getSyncReplicationSettings();

    ReplicationProcessingType getProcessingType();

    short getBucketCount();

    int getBatchParallelFactor();

    int getBatchParallelThreshold();

    long getConsumeTimeout();

    SwapBacklogConfig getSwapBacklogSettings();

    String getBacklogWeightPolicy();

    RedoLogCompaction getRedoLogCompaction();

    boolean isNetworkCompressionEnabled();
}
