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
package com.gigaspaces.client.iterator.server_based;

import com.gigaspaces.SpaceRuntimeException;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.executors.CloseIteratorDistributedSpaceTask;
import com.gigaspaces.internal.client.spaceproxy.executors.GetBatchForIteratorDistributedSpaceTask;
import com.gigaspaces.internal.client.spaceproxy.executors.RenewIteratorLeaseDistributedSpaceTask;
import com.gigaspaces.internal.client.spaceproxy.executors.SinglePartitionGetBatchForIteratorSpaceTask;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterUtils;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.Constants;
import net.jini.core.transaction.TransactionException;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceIteratorBatchResultProvider implements Serializable {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_GSITERATOR);
    private final BlockingQueue<SpaceIteratorBatchResult> _queue;
    private transient final ISpaceProxy _spaceProxy;
    private final int _batchSize;
    private final int _readModifiers;
    private final long _maxInactiveDuration;
    private final ITemplatePacket _queryPacket;
    private final UUID _uuid;
    private final int _numberOfPartitions;
    private final transient SpaceIteratorBatchResultListener _spaceIteratorBatchResultListener;


    public SpaceIteratorBatchResultProvider(ISpaceProxy spaceProxy, int batchSize, int readModifiers, ITemplatePacket queryPacket, UUID uuid, long maxInactiveDuration){
        this._spaceProxy = spaceProxy;
        this._batchSize = batchSize;
        this._readModifiers = readModifiers;
        this._maxInactiveDuration = maxInactiveDuration;
        this._queryPacket = queryPacket;
        this._uuid = uuid;
        this._numberOfPartitions = _spaceProxy.getDirectProxy().getSpaceClusterInfo().getNumberOfPartitions();
        this._queue = new LinkedBlockingQueue<>(getInitialNumberOfActivePartitions());
        this._spaceIteratorBatchResultListener = new SpaceIteratorBatchResultListener(this);
        initBatchTask();
    }

    private void initBatchTask() {
        try {
            if(_numberOfPartitions == 0){
                if(_logger.isLoggable(Level.FINE))
                    _logger.fine("Initializing space iterator batch task in embedded space.");
                triggerSinglePartitionBatchTask(PartitionedClusterUtils.NO_PARTITION, 0);
                return;
            }
            if(_queryPacket.getRoutingFieldValue() != null){
                if(_logger.isLoggable(Level.FINE))
                    _logger.fine("Initializing space iterator batch task with routing " + _queryPacket.getRoutingFieldValue());
                triggerSinglePartitionBatchTask(PartitionedClusterUtils.getPartitionId(_queryPacket.getRoutingFieldValue(), _numberOfPartitions), 0);
                return;
            }
            if(_logger.isLoggable(Level.FINE))
                _logger.fine("Initializing space iterator batch task in all " + _numberOfPartitions + " partitions");
            triggerBatchTaskInAllPartitions();
        } catch (RemoteException | TransactionException e) {
            throw new SpaceRuntimeException("Failed to initialize iterator", e);
        }
    }

    public void addBatchResult(SpaceIteratorBatchResult spaceIteratorBatchResult){
        _queue.add(spaceIteratorBatchResult);
    }

    public void addAsyncBatchResult(AsyncResult<SpaceIteratorBatchResult> spaceIteratorBatchAsyncResult){
        SpaceIteratorBatchResult spaceIteratorBatchResult = null;
        if(spaceIteratorBatchAsyncResult.getResult() != null) {
            spaceIteratorBatchResult = spaceIteratorBatchAsyncResult.getResult();
        }
        else if(spaceIteratorBatchAsyncResult.getException() != null){
            spaceIteratorBatchResult = new SpaceIteratorBatchResult(spaceIteratorBatchAsyncResult.getException(), _uuid);
        }
        if(spaceIteratorBatchResult != null) {
            addBatchResult(spaceIteratorBatchResult);
            return;
        }
        throw new IllegalStateException("Received async space iterator batch without result or exception");
    }

    public SpaceIteratorBatchResult consumeBatch(long timeout) throws InterruptedException {
        if(_logger.isLoggable(Level.FINE))
            _logger.fine("Waiting for space iterator " + _uuid + " next batch for " + timeout + " milliseconds.");
        return _queue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    public void triggerSinglePartitionBatchTask(int partitionId, int batchNumber) throws RemoteException, TransactionException {
        if(_logger.isLoggable(Level.FINE))
            _logger.fine("Triggering task for space iterator " + _uuid + " in partition " + partitionId + " fetch batchNumber " + batchNumber);
        _spaceProxy.execute(new SinglePartitionGetBatchForIteratorSpaceTask(this, batchNumber), partitionId, null, _spaceIteratorBatchResultListener);
    }

    private void triggerBatchTaskInAllPartitions() throws RemoteException, TransactionException {
        _spaceProxy.execute(new GetBatchForIteratorDistributedSpaceTask(this), null, null, null);
    }

    public void close() {
        if(_logger.isLoggable(Level.FINE))
            _logger.fine("Sending close request to space iterator "  + _uuid);
        try {
            _spaceProxy.execute(new CloseIteratorDistributedSpaceTask(_uuid), null, null, null);
        } catch (RemoteException | TransactionException e) {
            processCloseIteratorFailure(e);
        }
        _queue.clear();
    }

    public int getBatchSize() {
        return _batchSize;
    }

    public int getReadModifiers() {
        return _readModifiers;
    }

    public ITemplatePacket getQueryPacket() {
        return _queryPacket;
    }

    public UUID getUuid() {
        return _uuid;
    }

    public int getNumberOfPartitions() {
        return _numberOfPartitions;
    }

    public long getMaxInactiveDuration() {
        return _maxInactiveDuration;
    }

    public int getInitialNumberOfActivePartitions() {
        if(_numberOfPartitions == 0)
            return 1;
        if(_queryPacket.getRoutingFieldValue() != null)
            return 1;
        return _numberOfPartitions;
    }

    public ISpaceProxy getSpaceProxy() {
        return _spaceProxy;
    }

    private void processCloseIteratorFailure(Exception e) {
        if (_logger.isLoggable(Level.WARNING))
            _logger.log(Level.WARNING, "Failed to close iterator " + _uuid, e);
    }

    public void renewIteratorLease() {
        if(_logger.isLoggable(Level.FINE))
            _logger.fine("Renewing space iterator "  + _uuid + " lease");
        try {
            _spaceProxy.execute(new RenewIteratorLeaseDistributedSpaceTask(_uuid), null, null, null);
        } catch (TransactionException | RemoteException e) {
            processRenewIteratorLeaseFailure(e);
        }
    }

    private void processRenewIteratorLeaseFailure(Exception e) {
        if (_logger.isLoggable(Level.WARNING))
            _logger.log(Level.WARNING, "Failed to renew space iterator " + getUuid() + " lease.", e);
    }
}
