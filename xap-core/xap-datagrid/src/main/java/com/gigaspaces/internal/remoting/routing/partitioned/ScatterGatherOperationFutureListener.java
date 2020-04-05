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
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.IntegerObjectMap;
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
public class ScatterGatherOperationFutureListener<T extends RemoteOperationResult> extends RemoteOperationFutureListener<T> {
    private final IntegerObjectMap<ScatterGatherRemoteOperationRequest<T>> _map;
    private final ScatterGatherRemoteOperationRequest<T> _mainRequest;
    private final PartitionedClusterRemoteOperationRouter _router;
    private final List<ScatterGatherRemoteOperationRequest<T>> _previousResults;
    private int[] _partitionIds;

    public ScatterGatherOperationFutureListener(ScatterGatherRemoteOperationRequest<T> mainRequest, AsyncFutureListener<Object> listener, PartitionedClusterRemoteOperationRouter router, boolean getResultOnCompletion) {
        super(router.getLogger(), listener, getResultOnCompletion);
        this._mainRequest = mainRequest;
        this._router = router;
        this._previousResults = new ArrayList<ScatterGatherRemoteOperationRequest<T>>();
        this._map = CollectionsFactory.getInstance().createIntegerObjectMap();
    }

    public void mapValuesByHashCode(Object[] array, ScatterGatherRemoteOperationRequest<T> request) {
        for (int i = 0; i < array.length; i++)
            mapIndexToPartition(i, getPartitionIdByHashcode(array[i]), request);
    }

    public int getPartitionIdByHashcode(Object value) {
        return PartitionedClusterUtils.getPartitionId(value, _router.getGrainsMap());
    }

    public int getNextDistributionPartitionId() {
        return _router.getNextDistributionPartitionId(_mainRequest);
    }

    public void mapIndexToPartition(int index, int partitionId, ScatterGatherRemoteOperationRequest<T> request) {
        if (partitionId != PartitionedClusterUtils.NO_PARTITION)
            addPartition(partitionId, request).add(index);
        else {
            for (int i = 0; i < _router.getNumOfPartitions(); i++)
                addPartition(i, request).add(index);
        }
    }

    public ScatterGatherPartitionInfo addPartition(int partitionId, ScatterGatherRemoteOperationRequest<T> request) {
        ScatterGatherRemoteOperationRequest<T> partitionRequest = _map.get(partitionId);
        if (partitionRequest == null) {
            partitionRequest = (ScatterGatherRemoteOperationRequest<T>) request.createCopy(partitionId);
            _map.put(partitionId, partitionRequest);
        }

        return partitionRequest.getPartitionInfo();
    }


    public int[] getPartitionIds() {
        if (_partitionIds == null)
            _partitionIds = _map.keys();
        return _partitionIds;
    }

    public int getNumOfPartitions() {
        return _router.getNumOfPartitions();

    }

    public ScatterGatherRemoteOperationRequest<T> getPartitionRequest(int partitionId, ScatterGatherRemoteOperationRequest<T> mainRequest) {
        ScatterGatherRemoteOperationRequest<T> partitionRequest = _map.get(partitionId);
        partitionRequest.loadPartitionData(mainRequest);
        return partitionRequest;
    }

    @Override
    protected boolean onOperationResultArrival(RemoteOperationRequest<T> request) {
        ScatterGatherRemoteOperationRequest<T> partitionRequest = (ScatterGatherRemoteOperationRequest<T>) request;
        // Process incoming partition result:
        boolean continueProcessing = _mainRequest.processPartitionResult(partitionRequest, _previousResults);
        // When the partition request is same as the main, clear the result for safety:
        if (_mainRequest == partitionRequest)
            _mainRequest.setRemoteOperationResult(null);
        // If there are enough accumulated results, or this is the last possible result, signal completion:
        if (!continueProcessing || _previousResults.size() + 1 >= _map.size())
            return true;

        //TODO: should there be some round robin here as well? Is there anyone using this atm? (Eitan)
        _previousResults.add(partitionRequest);
        if (_mainRequest.getPartitionedClusterExecutionType() == PartitionedClusterExecutionType.SCATTER_SEQUENTIAL) {
            // Get next partition to execute:
            int partitionId = getPartitionIds()[_previousResults.size()];
            partitionRequest = getPartitionRequest(partitionId, _mainRequest);
            // Process request in next partition asynchronously:
            _router.getPartitionRouter(partitionId).executeAsync(partitionRequest, this);
        }
        return false;
    }

    @Override
    protected Object getResult(RemoteOperationRequest<T> request)
            throws Exception {
        return _mainRequest.getAsyncFinalResult();
    }
}
