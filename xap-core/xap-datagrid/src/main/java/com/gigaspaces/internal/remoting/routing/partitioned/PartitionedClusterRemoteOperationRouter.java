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
import com.gigaspaces.internal.cluster.PartitionToGrainsMap;
import com.gigaspaces.internal.quiesce.QuiesceTokenProviderImpl;
import com.gigaspaces.internal.remoting.RemoteOperationFutureListener;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.RemoteOperationResult;
import com.gigaspaces.internal.remoting.routing.AbstractRemoteOperationRouter;
import com.gigaspaces.internal.remoting.routing.RemoteOperationRouter;
import com.gigaspaces.internal.remoting.routing.RemoteOperationRouterException;
import com.gigaspaces.internal.remoting.routing.clustered.RemoteOperationsExecutorProxy;
import com.gigaspaces.internal.remoting.routing.clustered.RemoteOperationsExecutorsCluster;
import com.gigaspaces.internal.utils.concurrent.CyclicAtomicInteger;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class PartitionedClusterRemoteOperationRouter extends AbstractRemoteOperationRouter {
    private static final RemoteOperationsExecutorProxy _dummyProxy = new RemoteOperationsExecutorProxy("dummy", null, new QuiesceTokenProviderImpl());

    private final RemoteOperationRouter[] _partitions;
    private final CoordinatorFactory _listenerFactory;
    private final RemoteOperationsExecutorsCluster _partitionedCluster;
    private final CyclicAtomicInteger[] _roundRobinPreciseIndexes;
    private final boolean _broadcastDisabled;
    private PartitionToGrainsMap _grainsMap;
    private int _roundRobinApproxIndex = 0;


    public PartitionedClusterRemoteOperationRouter(String name,
                                                   RemoteOperationRouter[] partitions,
                                                   CoordinatorFactory coordinatorFactory,
                                                   boolean broadcastDisabled,
                                                   int numberOfPerciseRoundRobingOperations,
                                                   RemoteOperationsExecutorsCluster partitionedCluster, PartitionToGrainsMap grainsMap) {
        super(name);
        this._partitions = partitions;
        this._grainsMap = grainsMap;
        this._listenerFactory = coordinatorFactory;
        this._broadcastDisabled = broadcastDisabled;
        this._partitionedCluster = partitionedCluster;
        this._roundRobinPreciseIndexes = new CyclicAtomicInteger[numberOfPerciseRoundRobingOperations];
        for (int i = 0; i < _roundRobinPreciseIndexes.length; i++)
            _roundRobinPreciseIndexes[i] = new CyclicAtomicInteger(getNumOfPartitions() - 1);

        if (_logger.isDebugEnabled())
            _logger.debug("Initialized partitioned cluster router - number of partitions = " + partitions.length);
    }

    public int getNumOfPartitions() {
        return _partitions.length;
    }

    public RemoteOperationRouter[] getPartitions() {
        return _partitions;
    }

    public PartitionToGrainsMap getGrainsMap() {
        return _grainsMap;
    }

    public void setGrainsMap(PartitionToGrainsMap grainsMap){
        this._grainsMap = grainsMap;
    }

    public RemoteOperationRouter getPartitionRouter(int partitionId) {
        return _partitions[partitionId];
    }

    public int getNextPreciseDistributionPartitionId(int groupingCode) {
        return _roundRobinPreciseIndexes[groupingCode].getAndIncrement();
    }

    public int getNextApproxDistributionPartitionId() {
        int currentIndex;
        do {
            currentIndex = _roundRobinApproxIndex++;
            if (currentIndex < 0)
                _roundRobinApproxIndex = 0;
        } while (currentIndex < 0);

        return (currentIndex % getNumOfPartitions());
    }

    @Override
    public <T extends RemoteOperationResult> void execute(RemoteOperationRequest<T> request)
            throws InterruptedException {
        PartitionedClusterExecutionType executionType = request.getPartitionedClusterExecutionType();
        if (_logger.isTraceEnabled())
            _logger.trace("Starting partitioned execution (" + executionType + ") of " + request.toString());

        switch (executionType) {
            case SINGLE:
                executeSingle(request, false);
                break;
            case BROADCAST_SEQUENTIAL:
                assertBroadcastEnabled();
                executeBroadcastSequential(request);
                break;
            case BROADCAST_CONCURRENT:
                assertBroadcastEnabled();
                executeBroadcastConcurrent(request);
                break;
            case SCATTER_SEQUENTIAL:
                executeScatterSequential((ScatterGatherRemoteOperationRequest<T>) request);
                break;
            case SCATTER_CONCURRENT:
                executeScatterConcurrent((ScatterGatherRemoteOperationRequest<T>) request);
                break;
            default:
                throw new IllegalStateException("Unsupported PartitionedClusterExecutionType: " + executionType);
        }
    }

    @Override
    public <T extends RemoteOperationResult> RemoteOperationFutureListener<T> createFutureListener(RemoteOperationRequest<T> request, AsyncFutureListener<Object> listener) {
        PartitionedClusterExecutionType executionType = request.getPartitionedClusterExecutionType();
        switch (executionType) {
            case SINGLE:
                return new RemoteOperationFutureListener<T>(_logger, listener);
            case BROADCAST_SEQUENTIAL:
            case BROADCAST_CONCURRENT:
                return _listenerFactory.createAsyncBroadcastListener(request, listener, this);
            case SCATTER_SEQUENTIAL:
            case SCATTER_CONCURRENT:
                return _listenerFactory.createAsyncScatterGatherListener((ScatterGatherRemoteOperationRequest<T>) request, listener, this);
            default:
                throw new IllegalStateException("Unsupported PartitionedClusterExecutionType: " + executionType);
        }
    }


    @Override
    public <T extends RemoteOperationResult> void executeAsync(RemoteOperationRequest<T> request, RemoteOperationFutureListener<T> futureListener) {
        PartitionedClusterExecutionType executionType = request.getPartitionedClusterExecutionType();

        if (_logger.isTraceEnabled())
            _logger.trace("Starting async partitioned execution (" + executionType + ") of " + request.toString());

        switch (executionType) {
            case SINGLE:
                executeSingleAsync(request, futureListener);
                break;
            case BROADCAST_SEQUENTIAL:
                assertBroadcastEnabled();
                executeBroadcastSequentialAsync(request, (BroadcastOperationFutureListener<T>) futureListener);
                break;
            case BROADCAST_CONCURRENT:
                assertBroadcastEnabled();
                executeBroadcastConcurrentAsync(request, (BroadcastOperationFutureListener<T>) futureListener);
                break;
            case SCATTER_SEQUENTIAL:
                executeScatterSequentialAsync((ScatterGatherRemoteOperationRequest<T>) request, (ScatterGatherOperationFutureListener<T>) futureListener);
                break;
            case SCATTER_CONCURRENT:
                executeScatterConcurrentAsync((ScatterGatherRemoteOperationRequest<T>) request, (ScatterGatherOperationFutureListener<T>) futureListener);
                break;
            default:
                throw new IllegalStateException("Unsupported PartitionedClusterExecutionType: " + executionType);
        }
    }

    @Override
    public void executeOneway(RemoteOperationRequest<?> request)
            throws InterruptedException {
        PartitionedClusterExecutionType executionType = request.getPartitionedClusterExecutionType();
        if (_logger.isTraceEnabled())
            _logger.trace("Starting oneway partitioned execution (" + executionType + ") of " + request.toString());

        switch (executionType) {
            case SINGLE:
                executeSingle(request, true);
                break;
            case SCATTER_CONCURRENT:
                executeScatterConcurrentOneway((ScatterGatherRemoteOperationRequest<?>) request);
                break;
            case BROADCAST_CONCURRENT:
                assertBroadcastEnabled();
                executeBroadcastConcurrentOneway(request);
                break;
            default:
                throw new IllegalStateException("Unsupported PartitionedClusterExecutionType: " + executionType);
        }
    }

    private <T extends RemoteOperationResult> void executeSingle(RemoteOperationRequest<T> request, boolean oneway)
            throws InterruptedException {
        Object routingValue = request.getPartitionedClusterRoutingValue(this);
        int partitionId = PartitionedClusterUtils.getPartitionId(routingValue, _grainsMap);
        if (partitionId == PartitionedClusterUtils.NO_PARTITION) {
            request.setRemoteOperationExecutionError(new RemoteOperationRouterException("Cannot execute operation on partitioned cluster without routing value"));
            return;
        }
        if (oneway)
            this._partitions[partitionId].executeOneway(request);
        else
            this._partitions[partitionId].execute(request);
    }

    private <T extends RemoteOperationResult> void executeSingleAsync(RemoteOperationRequest<T> request, RemoteOperationFutureListener<T> listener) {
        Object routingValue = request.getPartitionedClusterRoutingValue(this);
        int partitionId = PartitionedClusterUtils.getPartitionId(routingValue, _grainsMap);
        if (partitionId == PartitionedClusterUtils.NO_PARTITION) {
            request.setRemoteOperationExecutionError(new RemoteOperationRouterException("Cannot execute operation on partitioned cluster without routing value"));
            if (listener != null)
                listener.onOperationCompletion(request, _dummyProxy);
            return;
        }
        this._partitions[partitionId].executeAsync(request, listener);
    }

    private <T extends RemoteOperationResult> void executeBroadcastSequential(RemoteOperationRequest<T> request)
            throws InterruptedException {
        // Create a listener to monitor concurrent execution:
        final int startIndex = getNextDistributionPartitionId(request);
        List<T> previousResults = new ArrayList<T>();
        for (int i = 0; i < _partitions.length; i++) {
            int index = (i + startIndex) % _partitions.length;
            request.setRemoteOperationResult(null);
            this._partitions[index].execute(request);
            T partitionResult = request.getRemoteOperationResult();
            boolean continueProcessing = request.processPartitionResult(partitionResult, previousResults, _partitions.length);
            if (!continueProcessing)
                break;
            previousResults.add(partitionResult);
        }
    }

    public int getNextDistributionPartitionId(RemoteOperationRequest<?> request) {
        return request.requiresPartitionedPreciseDistribution() ? getNextPreciseDistributionPartitionId(request.getPreciseDistributionGroupingCode()) : getNextApproxDistributionPartitionId();
    }

    private <T extends RemoteOperationResult> void executeBroadcastSequentialAsync(RemoteOperationRequest<T> request, BroadcastOperationFutureListener<T> listener) {
        // Execute request on first partition asynchronously (the coordinator will execute the rest sequentially):
        final int startPartitionId = listener.getStartPartitionId();
        this._partitions[startPartitionId].executeAsync(request, listener);
    }

    private <T extends RemoteOperationResult> void executeBroadcastConcurrent(RemoteOperationRequest<T> request)
            throws InterruptedException {
        BroadcastOperationFutureListener<T> listener = _listenerFactory.createSyncBroadcastListener(request, this);
        executeBroadcastConcurrentAsync(request, listener);
        listener.waitForCompletion();
    }

    private <T extends RemoteOperationResult> void executeBroadcastConcurrentAsync(RemoteOperationRequest<T> request, BroadcastOperationFutureListener<T> listener) {
        // Clone request per partition since request is not multithread-safe:
        // Execute request on each partition asynchronously:
        // TODO: Ask Eitan why the duplicate clone.
        RemoteOperationRequest<T> isolatedCopy = request.createCopy(-1);
        for (int i = 0; i < _partitions.length; i++)
            this._partitions[i].executeAsync(isolatedCopy.createCopy(i), listener);
    }

    private void executeBroadcastConcurrentOneway(
            RemoteOperationRequest<?> request) throws InterruptedException {
        // Clone request per partition since request is not multithread-safe:
        // Execute request on each partition in one way:
        RemoteOperationRequest isolatedCopy = request.createCopy(-1);
        for (int i = 0; i < _partitions.length; i++)
            this._partitions[i].executeOneway(isolatedCopy.createCopy(i));

    }

    private <T extends RemoteOperationResult> void executeScatterSequential(ScatterGatherRemoteOperationRequest<T> request)
            throws InterruptedException {
        ScatterGatherOperationFutureListener<T> listener = _listenerFactory.createSyncScatterGatherListener(request, this);
        request.scatterIndexesToPartitions(listener);

        // Execute each partition request and process its result synchronously:
        List<ScatterGatherRemoteOperationRequest<T>> previousPartitions = new ArrayList<ScatterGatherRemoteOperationRequest<T>>();
        for (int partitionId : listener.getPartitionIds()) {
            // Create request for current partition:
            ScatterGatherRemoteOperationRequest<T> partitionRequest = listener.getPartitionRequest(partitionId, request);
            // Execute partition request on partition:
            this._partitions[partitionId].execute(partitionRequest);
            // Process partition result:
            request.processPartitionResult(partitionRequest, previousPartitions);
            previousPartitions.add(partitionRequest);
        }
    }

    private <T extends RemoteOperationResult> void executeScatterSequentialAsync(ScatterGatherRemoteOperationRequest<T> request, ScatterGatherOperationFutureListener<T> listener) {
        request.scatterIndexesToPartitions(listener);

        // Get first partition id:
        int firstPartitionId = listener.getPartitionIds()[0];
        // Create request for first partition:
        ScatterGatherRemoteOperationRequest<T> partitionRequest = listener.getPartitionRequest(firstPartitionId, request);
        // Execute first request asynchronously:
        this._partitions[firstPartitionId].executeAsync(partitionRequest, listener);
    }

    private <T extends RemoteOperationResult> void executeScatterConcurrent(ScatterGatherRemoteOperationRequest<T> request)
            throws InterruptedException {
        ScatterGatherOperationFutureListener<T> listener = _listenerFactory.createSyncScatterGatherListener(request, this);
        executeScatterConcurrentAsync(request, listener);
        listener.waitForCompletion();
    }

    private <T extends RemoteOperationResult> void executeScatterConcurrentAsync(ScatterGatherRemoteOperationRequest<T> request, ScatterGatherOperationFutureListener<T> listener) {
        request.scatterIndexesToPartitions(listener);

        // Dispatch each request to its respective partition asynchronously:
        for (int partitionId : listener.getPartitionIds()) {
            ScatterGatherRemoteOperationRequest<T> partitionRequest = listener.getPartitionRequest(partitionId, request);
            this._partitions[partitionId].executeAsync(partitionRequest, listener);
        }
    }

    private void executeScatterConcurrentOneway(
            ScatterGatherRemoteOperationRequest<?> request) throws InterruptedException {
        ScatterGatherOperationFutureListener listener = _listenerFactory.createSyncScatterGatherListener(request, this);
        request.scatterIndexesToPartitions(listener);

        // Dispatch each request to its respective partition in one way mode:
        for (int partitionId : listener.getPartitionIds()) {
            ScatterGatherRemoteOperationRequest<?> partitionRequest = listener.getPartitionRequest(partitionId, request);
            this._partitions[partitionId].executeOneway(partitionRequest);
        }

    }

    @Override
    public RemoteOperationsExecutorProxy getAnyAvailableMember() {
        return getAvailableMember(false, _partitionedCluster.getConfig().getActiveServerLookupTimeout());
    }

    @Override
    public RemoteOperationsExecutorProxy getAnyActiveMember() {
        return getAvailableMember(true, _partitionedCluster.getConfig().getActiveServerLookupTimeout());
    }

    private RemoteOperationsExecutorProxy getAvailableMember(boolean activeOnly, long timeout) {
        for (int i = 0; i < getNumOfPartitions(); i++) {
            RemoteOperationsExecutorProxy member = getPartitionRouter(i).getCachedMember();

            // TODO there is stil room for optimization here, as
            // the isAvailable method involed a remote call which might 
            // take a long time in case the remote target is down
            if (RemoteOperationsExecutorProxy.isAvailable(member, activeOnly))
                return member;
        }

        try {
            return _partitionedCluster.getAvailableMember(activeOnly, timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (RemoteOperationRouterException e) {
            return null;
        }
    }

    @Override
    public void getAllAvailableMembers(List<RemoteOperationsExecutorProxy> availableMembers) {
        for (int i = 0; i < getNumOfPartitions(); i++)
            getPartitionRouter(i).getAllAvailableMembers(availableMembers);
    }

    @Override
    public void close() {
        _partitionedCluster.close();

        for (RemoteOperationRouter partition : _partitions) {
            partition.close();
        }
    }

    private void assertBroadcastEnabled() {
        if (_broadcastDisabled)
            throw new IllegalArgumentException("Can not perform broadcast." +
                    " Broadcast is disabled by the cluster configuration property 'cluster-config.groups.group.load-bal-policy.default.broadcast-condition=broadcast-disabled'.");
    }

    public Logger getLogger() {
        return _logger;
    }
}
