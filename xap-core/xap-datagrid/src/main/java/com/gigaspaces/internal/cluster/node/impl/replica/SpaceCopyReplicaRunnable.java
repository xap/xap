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

package com.gigaspaces.internal.cluster.node.impl.replica;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.cluster.node.impl.ReplicationNode;
import com.gigaspaces.internal.cluster.node.impl.filters.ISpaceCopyReplicaInFilter;
import com.gigaspaces.internal.cluster.node.impl.packets.NextReplicaStatePacket;
import com.gigaspaces.internal.cluster.node.impl.packets.ReplicaFetchDataPacket;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.utils.concurrent.AsyncCallable;
import com.gigaspaces.internal.utils.concurrent.CyclicAtomicInteger;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider.CycleResult;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.cluster.IReplicationFilterEntry;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Retrieve space copy replica data, can have multiple instances of this class running concurrently
 * to retrieve the replica data
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceCopyReplicaRunnable
        extends AsyncCallable implements AsyncFutureListener<Collection<ISpaceReplicaData>> {
    protected final static Logger _logger = Logger.getLogger(Constants.LOGGER_REPLICATION_REPLICA);
    private final ReplicationNode _replicationNode;

    private final IReplicationMonitoredConnection _originConnection;
    private final ISpaceReplicaDataConsumer _replicaDataProducer;
    private final Object _replicaRemoteContext;
    private final SpaceReplicaState _state;
    private final ReplicaFetchDataPacket _fetchDataPacket;
    private final ISpaceCopyIntermediateResult _intermediateResult;
    private final ISpaceCopyReplicaInFilter _inFilter;
    private final boolean _isFiltered;
    private final CyclicAtomicInteger _orderProvider;
    private volatile boolean _aborted;
    private volatile long _lastIterationTimeStamp = SystemTime.timeMillis();

    public SpaceCopyReplicaRunnable(ReplicationNode replicationNode, IReplicationMonitoredConnection originConnection,
                                    ISpaceReplicaDataConsumer replicaDataProcessor,
                                    ISpaceCopyReplicaInFilter inFilter, Object replicaRemoteContext,
                                    int fetchBatchSize, SpaceReplicaState state,
                                    CyclicAtomicInteger orderProvider) {
        _replicationNode = replicationNode;
        _originConnection = originConnection;
        _replicaDataProducer = replicaDataProcessor;
        _replicaRemoteContext = replicaRemoteContext;
        _state = state;
        _inFilter = inFilter;
        _orderProvider = orderProvider;
        _isFiltered = (_inFilter != null);
        _fetchDataPacket = new ReplicaFetchDataPacket(replicaRemoteContext,
                fetchBatchSize);
        _intermediateResult = _replicaDataProducer.createEmptyResult();
    }

    public ISpaceCopyIntermediateResult getIntermediateResult() {
        return _intermediateResult;
    }

    public CycleResult call() {
        try {
            if (_aborted)
                throw new ReplicaAbortedException();

            AsyncFuture<Collection<ISpaceReplicaData>> future = _originConnection.dispatchAsync(_fetchDataPacket);
            future.setListener(this);
            return CycleResult.SUSPEND;
        } catch (Throwable e) {
            if (_logger.isLoggable(Level.FINER))
                _logger.log(Level.FINER, _replicationNode.getLogPrefix() + " dispatch request for replica batch has an exception", e);

            if (!(e instanceof Exception))
                e = new ExecutionException(e.getMessage(), e);
            _state.signalCopyStageFailed((Exception) e);
            return CycleResult.TERMINATE;
        }
    }

    public long getLastIterationTimeStamp() {
        return _lastIterationTimeStamp;
    }

    public void abort() {
        _aborted = true;
        getHandler().stop(1, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onResult(AsyncResult<Collection<ISpaceReplicaData>> result) {
        try {
            if (result.getException() != null) {
                if (_logger.isLoggable(Level.FINER))
                    _logger.log(Level.FINER, _replicationNode.getLogPrefix() + " incoming replica batch has an exception", result.getException());
                throw result.getException();
            }


            // current stage is done
            if (result.getResult() == null || result.getResult().isEmpty()) {
                final int arrivalOrder = _orderProvider.getAndIncrement();
                if (arrivalOrder < _orderProvider.getMaxValue())
                    return;

                // The last to arrive will move target to next stage
                // before 9.0.1 the NextReplicaStatePacket's dispatch returned boolean
                // after 9.0.1 it returns CurrentStageInfo object
                boolean isOldVersion = false;
                Object nextStage = _originConnection.dispatch(new NextReplicaStatePacket(_replicaRemoteContext));

                boolean hasMoreStages;
                String stageName = null;
                String nextStageName = null;

                if (nextStage instanceof Boolean) {
                    hasMoreStages = (Boolean) nextStage;
                    isOldVersion = true;
                } else {
                    CurrentStageInfo stageInfo = (CurrentStageInfo) nextStage;
                    stageName = stageInfo.getStageName();
                    hasMoreStages = !stageInfo.isLastStage();
                    nextStageName = stageInfo.getNextStageName();
                }

                if (hasMoreStages) {
                    if (_logger.isLoggable(Level.FINER))
                        _logger.finer(_replicationNode.getLogPrefix() + (isOldVersion ? "" : " completed current stage [" + stageName + "], ")
                                + "moved to the next stage"
                                + (isOldVersion ? "" : " [" + nextStageName + "]") + ".");
                    _state.signalSingleCopyStageDone();
                } else {
                    if (_logger.isLoggable(Level.FINER))
                        _logger.finer(_replicationNode.getLogPrefix() + (isOldVersion ? "" : " completed current stage [" + stageName + "], ") + "all stages completed.");
                    _state.signalEntireCopyStageDoneSucessfully();
                }
            } else {
                if (result.getResult() instanceof SpaceReplicaBatch) {
                    SpaceReplicaBatch batch = (SpaceReplicaBatch) result.getResult();
                    if (batch.isFifoBatch()) {
                        SpaceReplicaFifoBatchesHandler fifoBatchesHandler = _replicationNode.getReplicaHandler().getFifoBatchesHandler();
                        fifoBatchesHandler.handleIncomingBatch(batch, this);
                    } else {
                        // Get replica data
                        processBatch(result.getResult());
                    }
                } else {
                    processBatch(result.getResult());
                }
            }
        } catch (Throwable e) {
            if (!(e instanceof Exception))
                e = new ExecutionException(e.getMessage(), e);
            _state.signalCopyStageFailed((Exception) e);
        }
    }

    protected void processBatch(Collection<ISpaceReplicaData> copiedData) {
        try {
            _lastIterationTimeStamp = SystemTime.timeMillis();
            // Consume data
            if (_replicationNode.getBlobStoreReplicaConsumeHelper() != null && copiedData.size() > 1)
                _replicationNode.getBlobStoreReplicaConsumeHelper().prepareForBulking();
            try {
                for (ISpaceReplicaData data : copiedData) {
                    if (_isFiltered && data.supportsReplicationFilter()) {
                        IReplicationFilterEntry filterEntry = _replicaDataProducer.toFilterEntry(data);
                        _inFilter.filterIn(filterEntry, _originConnection.getFinalEndpointLookupName());
                        if (filterEntry.isDiscarded()) {
                            _intermediateResult.incrementBlockedByFilterEntry();
                            continue;
                        }
                    }
                    _replicaDataProducer.consumeData(data, _intermediateResult, _replicationNode);
                }
            } finally {
                if (_replicationNode.getBlobStoreReplicaConsumeHelper() != null)
                    _replicationNode.getBlobStoreReplicaConsumeHelper().flushBulk();
            }
            // Should keep running
            if (_logger.isLoggable(Level.FINEST))
                _logger.log(Level.FINEST, _replicationNode.getLogPrefix() + "copied replica batch " + copiedData);
            getHandler().resumeNow();
        } catch (Throwable e) {
            if (!(e instanceof Exception))
                e = new ExecutionException(e.getMessage(), e);
            _state.signalCopyStageFailed((Exception) e);
        }
    }

}
