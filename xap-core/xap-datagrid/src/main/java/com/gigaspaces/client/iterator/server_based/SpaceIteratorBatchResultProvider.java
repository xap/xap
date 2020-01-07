package com.gigaspaces.client.iterator.server_based;

import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterUtils;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.Constants;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.TransactionException;

import java.io.Closeable;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
    - Logging
    - Exception Handling
 */
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
    private final ITemplatePacket _queryPacket;
    private final UUID _uuid;
    private final int _numberOfPartitions;

    public SpaceIteratorBatchResultProvider(ISpaceProxy spaceProxy, int batchSize, int readModifiers, ITemplatePacket queryPacket, UUID uuid){
        this._spaceProxy = spaceProxy;
        this._batchSize = batchSize;
        this._readModifiers = readModifiers;
        this._queryPacket = queryPacket;
        this._uuid = uuid;
        this._queue = new LinkedBlockingQueue<>();
        this._numberOfPartitions = _spaceProxy.getDirectProxy().getSpaceClusterInfo().getNumberOfPartitions();
        initBatchTask();
    }

    private void initBatchTask() {
        if(_numberOfPartitions == 0){
            if(_logger.isLoggable(Level.FINE))
                _logger.fine("Initializing space iterator batch task in embedded space.");
            triggerSinglePartitionBatchTask(PartitionedClusterUtils.NO_PARTITION, true);
            return;
        }
        if(_queryPacket.getRoutingFieldValue() != null){
            if(_logger.isLoggable(Level.FINE))
                _logger.fine("Initializing space iterator batch task with routing " + _queryPacket.getRoutingFieldValue());
            triggerSinglePartitionBatchTask(PartitionedClusterUtils.getPartitionId(_queryPacket.getRoutingFieldValue(), _numberOfPartitions), true);
            return;
        }
        try {
            if(_logger.isLoggable(Level.FINE))
                _logger.fine("Initializing space iterator batch task in all " + _numberOfPartitions + " partitions");
            triggerBatchTaskInAllPartitions();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (TransactionException e) {
            e.printStackTrace();
        }
    }

    /*
    Add result on arrival. Add it to queue if successful and set it in map
     */
    public void addBatchResult(SpaceIteratorBatchResult spaceIteratorBatchResult){
        if(spaceIteratorBatchResult == null)
            return;
        _queue.add(spaceIteratorBatchResult);
    }

    public SpaceIteratorBatchResult consumeBatch(long timeout) throws InterruptedException {
        if(_logger.isLoggable(Level.FINE))
            _logger.fine("Waiting for space iterator " + _uuid + " next batch for " + timeout + " milliseconds.");
        return _queue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    public void triggerSinglePartitionBatchTask(int partitionId, boolean firstTime) {
        try {
            _spaceProxy.execute(new SinglePartitionGetBatchForIteratorSpaceTask(this, firstTime), partitionId, null, new SpaceIteratorBatchResultListener(this));
        } catch (TransactionException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
    /*
    On init, task is triggered to start iterator in all servers
     */
    private void triggerBatchTaskInAllPartitions() throws RemoteException, TransactionException {
        _spaceProxy.execute(new GetBatchForIteratorDistributedSpaceTask(this, true), null, null, null);
    }

    public void close() {
        if(_logger.isLoggable(Level.FINE))
            _logger.fine("Sending close request to space iterator "  + _uuid);
        try {
            _spaceProxy.closeSpaceIterator(_uuid);
        } catch (RemoteException e) {
            processCloseIteratorFailure(e);
        } catch (InterruptedException e) {
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

    public int getInitialNumberOfActivePartitions() {
        if(_numberOfPartitions == 0)
            return 1;
        if(_queryPacket.getRoutingFieldValue() != null)
            return 1;
        return _numberOfPartitions;
    }

    private void processCloseIteratorFailure(Exception e) {
        if (_logger.isLoggable(Level.SEVERE))
            _logger.log(Level.SEVERE, "Failed to close iterator " + _uuid + " in server", e);
    }
}
