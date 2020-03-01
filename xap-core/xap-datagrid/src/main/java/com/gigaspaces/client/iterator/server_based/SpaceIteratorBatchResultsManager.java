package com.gigaspaces.client.iterator.server_based;

import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.GetBatchForIteratorException;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceIteratorBatchResultsManager {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_GSITERATOR);
    private enum ResultStatus {NORMAL, LAST_BATCH, FAILED, ILLEGAL_BATCH_NUMBER};
    private final Map<Integer, SpaceIteratorBatchResult> _partitionIteratorBatchResults;
    private final SpaceIteratorBatchResultProvider _spaceIteratorBatchResultProvider;
    private int _activePartitions;

    public SpaceIteratorBatchResultsManager(ISpaceProxy spaceProxy, int batchSize, int readModifiers, ITemplatePacket queryPacket){
        this._partitionIteratorBatchResults = new HashMap<>();
        this._spaceIteratorBatchResultProvider = new SpaceIteratorBatchResultProvider(spaceProxy, batchSize, readModifiers, queryPacket, UUID.randomUUID());
        this._activePartitions = this._spaceIteratorBatchResultProvider.getInitialNumberOfActivePartitions();
    }

    public Object[] getNextBatch(long timeout) throws InterruptedException, SpaceIteratorException {
        if(_activePartitions == 0){
            return finish();
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
                throw new SpaceIteratorException("");
        }
    }

    private Object[] finish() throws SpaceIteratorException{
        if (anyFinishedExceptionally()) {
            SpaceIteratorException spaceIteratorException = new SpaceIteratorException("Space Iterator " + _spaceIteratorBatchResultProvider.getUuid() + " finished prematurely with exceptions, not all entries were iterated over.");
            for(Map.Entry<Integer, SpaceIteratorBatchResult> entry : _partitionIteratorBatchResults.entrySet()){
                if(entry.getValue().getException() != null)
                    spaceIteratorException.addException(entry.getKey(), entry.getValue().getException());
            }
            throw spaceIteratorException;
        }
        if (_logger.isLoggable(Level.FINE))
            _logger.fine("Space Iterator has finished successfully.");
        return null;
    }

    private ResultStatus inspectBatchResults(SpaceIteratorBatchResult previous, SpaceIteratorBatchResult current){
        if(isIllegalBatchNumber(previous, current))
            return ResultStatus.ILLEGAL_BATCH_NUMBER;
        if(current.isFailed())
            return ResultStatus.FAILED;
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
        if (_logger.isLoggable(Level.WARNING))
            _logger.warning("Space Iterator batch result " + spaceIteratorBatchResult + " batch number is illegal");
        deactivatePartition(spaceIteratorBatchResult);
        return getNextBatch(timeout);
    }

    private Object[] handleFailedBatchResult(SpaceIteratorBatchResult spaceIteratorBatchResult, long timeout) throws InterruptedException {
        if (_logger.isLoggable(Level.WARNING))
            _logger.warning("Space Iterator batch result " + spaceIteratorBatchResult + " failed with exception " + spaceIteratorBatchResult.getException().getMessage());
        Exception exception = spaceIteratorBatchResult.getException();
        if(!(exception instanceof GetBatchForIteratorException) && exception instanceof RuntimeException)
            throw (RuntimeException) exception;
        deactivatePartition(spaceIteratorBatchResult);
        return getNextBatch(timeout);
    }

    private Object[] handleLastBatchResult(SpaceIteratorBatchResult spaceIteratorBatchResult){
        if (_logger.isLoggable(Level.FINE))
            _logger.fine("Space Iterator batch result " + spaceIteratorBatchResult + " has finished");
        deactivatePartition(spaceIteratorBatchResult);
        return spaceIteratorBatchResult.getEntries();
    }

    private Object[] handleNormalBatchResult(SpaceIteratorBatchResult spaceIteratorBatchResult){
        try {
            _spaceIteratorBatchResultProvider.triggerSinglePartitionBatchTask(spaceIteratorBatchResult.getPartitionId(), spaceIteratorBatchResult.getBatchNumber() + 1);
        } catch (RemoteException | TransactionException e) {
            // TODO fail partition that threw this exception
            e.printStackTrace();
        }
        return spaceIteratorBatchResult.getEntries();
    }

    private boolean anyFinishedExceptionally(){
        return _partitionIteratorBatchResults
                .values()
                .stream()
                .anyMatch(SpaceIteratorBatchResult::isFailed);
    }

    private void deactivatePartition(SpaceIteratorBatchResult spaceIteratorBatchResult){
        if(_logger.isLoggable(Level.FINE))
            _logger.fine("Deactivating partition " + spaceIteratorBatchResult.getPartitionId());
        _activePartitions--;
    }

    public void close() {
        _partitionIteratorBatchResults.clear();
        _spaceIteratorBatchResultProvider.close();
    }
}
