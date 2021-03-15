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

package com.gigaspaces.internal.remoting;

import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.lrmi.nio.LRMIMethodTrackingIdProvider;
import com.j_spaces.core.SpaceContext;

import java.util.List;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
public interface RemoteOperationRequest<TResult extends RemoteOperationResult> extends LRMIMethodTrackingIdProvider {
    int getOperationCode();

    TResult createRemoteOperationResult();

    TResult getRemoteOperationResult();

    void setRemoteOperationResult(TResult remoteOperationResult);

    void setRemoteOperationExecutionError(Exception error);

    Object getAsyncFinalResult() throws Exception;

    boolean isBlockingOperation();

    boolean processUnknownTypeException(List<Integer> positions);

    PartitionedClusterExecutionType getPartitionedClusterExecutionType();

    Object getPartitionedClusterRoutingValue(PartitionedClusterRemoteOperationRouter router);

    boolean requiresPartitionedPreciseDistribution();

    int getPreciseDistributionGroupingCode();

    SpaceContext getSpaceContext();

    RemoteOperationRequest<TResult> createCopy(int targetPartitionId);

    boolean processPartitionResult(TResult remoteOperationResult, List<TResult> previousResults, int numOfPartitions);

    boolean isDedicatedPoolRequired();

    boolean isDirectExecutionEnabled();
}
