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

package com.gigaspaces.internal.remoting.routing.partitioned;

import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.router.SpacePartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.exceptions.ChunksMapGenerationException;
import com.gigaspaces.internal.remoting.RemoteOperationFutureListener;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.RemoteOperationResult;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class BroadcastOperationFutureListener<T extends RemoteOperationResult> extends RemoteOperationFutureListener<T> {
    private final RemoteOperationRequest<T> _mainRequest;
    private final PartitionedClusterRemoteOperationRouter _router;
    private final List<T> _previousResults;
    private final int _startPartitionId;

    public BroadcastOperationFutureListener(RemoteOperationRequest<T> mainRequest, AsyncFutureListener<Object> listener, PartitionedClusterRemoteOperationRouter router, boolean getResultOnCompletion) {
        super(router.getLogger(), listener, getResultOnCompletion);
        this._mainRequest = mainRequest;
        this._router = router;
        this._previousResults = new ArrayList<T>();
        this._startPartitionId = router.getNextDistributionPartitionId(mainRequest);
    }

    @Override
    protected boolean onOperationResultArrival(RemoteOperationRequest<T> partitionRequest) {
        // Process incoming partition result:
        T partitionResult = partitionRequest.getRemoteOperationResult();
        boolean continueProcessing = _mainRequest.processPartitionResult(partitionResult, _previousResults, _router.getNumOfPartitions());
        // If there are enough accumulated results, or this is the last possible result, signal completion:
        if (!continueProcessing || _previousResults.size() + 1 >= _router.getNumOfPartitions())
            return true;

        // When the partition request is same as the main, clear the result for safety:
        if (_mainRequest == partitionRequest)
            _mainRequest.setRemoteOperationResult(null);

        _previousResults.add(partitionResult);
        if (_mainRequest.getPartitionedClusterExecutionType() == PartitionedClusterExecutionType.BROADCAST_SEQUENTIAL) {
            // Get next partition to execute:
            final int partitionId = (getStartPartitionId() + _previousResults.size()) % _router.getNumOfPartitions();
            // Process request in next partition asynchronously:
            _router.getPartitionRouter(partitionId).executeAsync(_mainRequest, this);
        }
        return false;
    }

    @Override
    protected Object getResult(RemoteOperationRequest<T> request)
            throws Exception {
        return _mainRequest.getAsyncFinalResult();
    }

    public int getStartPartitionId() {
        return _startPartitionId;
    }
}
