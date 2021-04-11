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
package com.gigaspaces.client.iterator.cursor;

import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.GetBatchForIteratorException;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceIteratorBatchResultsManager {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_GSITERATOR);
    private enum ResultStatus {NORMAL, LAST_BATCH, FAILED, ILLEGAL_BATCH_NUMBER};
    private final Map<Integer, SpaceIteratorBatchResult> _partitionIteratorBatchResults;
    private final SpaceIteratorBatchResultProvider _spaceIteratorBatchResultProvider;
    private final ScheduledExecutorService _scheduler;
    private int _activePartitions;

    public SpaceIteratorBatchResultsManager(ISpaceProxy spaceProxy, int batchSize, int readModifiers, ITemplatePacket queryPacket, long maxInactiveDuration){
        this._partitionIteratorBatchResults = new HashMap<>();
        this._spaceIteratorBatchResultProvider = new SpaceIteratorBatchResultProvider(spaceProxy, batchSize, readModifiers, queryPacket, UUID.randomUUID(), maxInactiveDuration);
        this._activePartitions = this._spaceIteratorBatchResultProvider.getInitialNumberOfActivePartitions();
        this._scheduler = Executors.newScheduledThreadPool(1);
        initRenewLeaseTask(maxInactiveDuration/2);
    }

    private void initRenewLeaseTask(long delay) {
        _scheduler.scheduleWithFixedDelay(() -> {
            if(isFinished()) {
                _scheduler.shutdown();
                return;
            }
            _spaceIteratorBatchResultProvider.renewIteratorLease();
        }, delay, delay, TimeUnit.MILLISECONDS);
    }

    public Object[] getNextBatch(long timeout) throws InterruptedException, SpaceIteratorException {
        if(isFinished()){
            tryFinish();
            if (_logger.isDebugEnabled())
                _logger.debug("Space Iterator has finished successfully.");
            return null;
        }
        SpaceIteratorBatchResult spaceIteratorBatchResult = _spaceIteratorBatchResultProvider.consumeBatch(timeout);
        if (spaceIteratorBatchResult == null)
            throw new SpaceIteratorException("Did not find any batch for iterator " + _spaceIteratorBatchResultProvider.getUuid() + " under " + timeout + " milliseconds");
        SpaceIteratorBatchResult previous = _partitionIteratorBatchResults.put(spaceIteratorBatchResult.getPartitionId(), spaceIteratorBatchResult);
        switch (inspectBatchResults(previous, spaceIteratorBatchResult)){
            case ILLEGAL_BATCH_NUMBER:
                return handleIllegalBatchNumber(spaceIteratorBatchResult,timeout);
            case FAILED:
                return handleFailedBatchResult(spaceIteratorBatchResult, timeout);
            case LAST_BATCH:
                return handleLastBatchResult(spaceIteratorBatchResult);
            case NORMAL:
                return handleNormalBatchResult(spaceIteratorBatchResult);
            default:
                throw new SpaceIteratorException("Unknown batch result.");
        }
    }

    private void tryFinish() throws SpaceIteratorException{
        if (anyFinishedExceptionally()) {
            SpaceIteratorException spaceIteratorException = new SpaceIteratorException("Space Iterator finished prematurely with exceptions, not all entries were iterated over.Detailed exceptions:\n");
            for(Map.Entry<Integer, SpaceIteratorBatchResult> entry : _partitionIteratorBatchResults.entrySet()){
                if(entry.getValue().getException() != null)
                    spaceIteratorException.addException(entry.getKey(), entry.getValue().getException());
            }
            throw spaceIteratorException;
        }
    }

    private ResultStatus inspectBatchResults(SpaceIteratorBatchResult previous, SpaceIteratorBatchResult current){
        if(current.isFailed())
            return ResultStatus.FAILED;
        if(isIllegalBatchNumber(previous, current))
            return ResultStatus.ILLEGAL_BATCH_NUMBER;
        if(current.getEntries() != null && current.getEntries().length < _spaceIteratorBatchResultProvider.getBatchSize())
            return ResultStatus.LAST_BATCH;
        return ResultStatus.NORMAL;
    }

    private boolean isIllegalBatchNumber(SpaceIteratorBatchResult previous, SpaceIteratorBatchResult current) {
        if(previous == null || current == null)
            return false;
        if(current.getBatchNumber() == SpaceIteratorBatchResult.NO_BATCH_NUMBER)
            return true;
        return current.getBatchNumber() - previous.getBatchNumber() != 1;
    }

    private Object[] handleIllegalBatchNumber(SpaceIteratorBatchResult spaceIteratorBatchResult, long timeout) throws InterruptedException {
        if (_logger.isWarnEnabled())
            _logger.warn("Space Iterator batch result " + spaceIteratorBatchResult + " batch number is illegal");
        deactivatePartition(spaceIteratorBatchResult);
        return getNextBatch(timeout);
    }

    private Object[] handleFailedBatchResult(SpaceIteratorBatchResult spaceIteratorBatchResult, long timeout) throws InterruptedException {
        if (_logger.isWarnEnabled())
            _logger.warn("Space Iterator batch result failed with exception: " + spaceIteratorBatchResult.getException().getMessage());
        Exception exception = spaceIteratorBatchResult.getException();
        if(!(exception instanceof GetBatchForIteratorException) && exception instanceof RuntimeException)
            throw (RuntimeException) exception;
        deactivatePartition(spaceIteratorBatchResult);
        return getNextBatch(timeout);
    }

    private Object[] handleLastBatchResult(SpaceIteratorBatchResult spaceIteratorBatchResult){
        if (_logger.isDebugEnabled())
            _logger.debug("Space Iterator batch result " + spaceIteratorBatchResult + " has finished");
        deactivatePartition(spaceIteratorBatchResult);
        return spaceIteratorBatchResult.getEntries();
    }

    private Object[] handleNormalBatchResult(SpaceIteratorBatchResult currentSpaceIteratorBatchResult){
        try {
            _spaceIteratorBatchResultProvider.triggerSinglePartitionBatchTask(currentSpaceIteratorBatchResult.getPartitionId(), currentSpaceIteratorBatchResult.getBatchNumber() + 1);
        } catch (RemoteException | TransactionException e) {
            _spaceIteratorBatchResultProvider.addBatchResult(new SpaceIteratorBatchResult(e, currentSpaceIteratorBatchResult.getUuid(), currentSpaceIteratorBatchResult.getPartitionId(), currentSpaceIteratorBatchResult.getBatchNumber() + 1));
        }
        return currentSpaceIteratorBatchResult.getEntries();
    }

    private boolean anyFinishedExceptionally(){
        return _partitionIteratorBatchResults
                .values()
                .stream()
                .anyMatch(SpaceIteratorBatchResult::isFailed);
    }

    private void deactivatePartition(SpaceIteratorBatchResult spaceIteratorBatchResult){
        if(_logger.isDebugEnabled())
            _logger.debug("Deactivating partition " + spaceIteratorBatchResult.getPartitionId());
        _activePartitions--;
    }

    public void close() {
        _partitionIteratorBatchResults.clear();
        _spaceIteratorBatchResultProvider.close();
        _scheduler.shutdown();
    }

    private boolean isFinished(){
        return _activePartitions == 0;
    }
}
