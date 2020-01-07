package com.gigaspaces.client.iterator.server_based;

import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.Constants;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
    - Lease Management
    - Logging!!!!!
    - Exception Handling
    - Optional: Number of batches user configuration/algorithm
    - STOP if iterator is closed
 */
/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceIteratorBatchResultsManager {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_GSITERATOR);
    private final Map<Integer, SpaceIteratorBatchResult> _partitionIteratorBatchResults;
    private final SpaceIteratorBatchResultProvider _spaceIteratorBatchResultProvider;
    private int _activePartitions;

    public SpaceIteratorBatchResultsManager(ISpaceProxy spaceProxy, int batchSize, int readModifiers, ITemplatePacket queryPacket){
        this._partitionIteratorBatchResults = new HashMap<>();
        this._spaceIteratorBatchResultProvider = new SpaceIteratorBatchResultProvider(spaceProxy, batchSize, readModifiers, queryPacket, UUID.randomUUID());
        this._activePartitions = this._spaceIteratorBatchResultProvider.getInitialNumberOfActivePartitions();
    }


    /*
    Get next batch flow:
        - Check if all partitions finished
            - If finished successfully, return null
            - If finished with exception throw exception
        -----Not finished yet-------
        - Get next batch result from manager
        - If result is failed
            - Deactivate partition result arrived from
            - Call method recursively
        - If result is finished, deactivate partition result arrived from
        - If batch result is empty, call method recursively
     */
    public Object[] getNextBatch(long timeout) throws TimeoutException, InterruptedException {
        if(allFinished()) {
            if (allFinishedSuccessfully()) {
                if (_logger.isLoggable(Level.INFO))
                    _logger.info("Space Iterator has finished successfully.");
                return null;
            }
            if (anyFinishedExceptionally()) {
                if (_logger.isLoggable(Level.SEVERE))
                    _logger.severe("Space Iterator finished with exception, not all entries were retrieved.");
                // TODO throw IteratorException
                return null;
            }
        }
        SpaceIteratorBatchResult spaceIteratorBatchResult = _spaceIteratorBatchResultProvider.consumeBatch(timeout);
        if (spaceIteratorBatchResult == null)
            throw new TimeoutException("Did not find any batch for iterator " + _spaceIteratorBatchResultProvider.getUuid() + " under " + timeout + " milliseconds");
        _partitionIteratorBatchResults.put(spaceIteratorBatchResult.getPartitionId(), spaceIteratorBatchResult);
        if (spaceIteratorBatchResult.isFailed()) {
            if (_logger.isLoggable(Level.WARNING))
                _logger.warning("Space Iterator batch result " + spaceIteratorBatchResult + " returned with exception " + spaceIteratorBatchResult.getException().getMessage());
            deactivatePartition(spaceIteratorBatchResult);
            return getNextBatch(timeout);
        }
        if (spaceIteratorBatchResult.getEntries() != null && spaceIteratorBatchResult.getEntries().length < _spaceIteratorBatchResultProvider.getBatchSize()) {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine("Space Iterator batch result " + spaceIteratorBatchResult + " has finished");
            spaceIteratorBatchResult.setFinished(true);
            deactivatePartition(spaceIteratorBatchResult);
        } else {
            _spaceIteratorBatchResultProvider.triggerSinglePartitionBatchTask(spaceIteratorBatchResult.getPartitionId(), false);
        }
        return spaceIteratorBatchResult.getEntries();
    }

    /*
    check if any results have failed
     */
    private boolean anyFinishedExceptionally(){
        return allFinished() && _partitionIteratorBatchResults
                .values()
                .stream()
                .anyMatch(SpaceIteratorBatchResult::isFailed);
    }

    /*
    check if all results are in FINISHED state
     */
    private boolean allFinishedSuccessfully(){
        return allFinished() &&
                _partitionIteratorBatchResults
                        .values()
                        .stream()
                        .allMatch(SpaceIteratorBatchResult::isFinished);
    }

    private void deactivatePartition(SpaceIteratorBatchResult spaceIteratorBatchResult){
        if(_logger.isLoggable(Level.FINE))
            _logger.fine("Deactivating partition " + spaceIteratorBatchResult.getPartitionId());
        _activePartitions--;
    }

    private boolean allFinished(){
        return _activePartitions == 0;
    }

    public void close() {
        _partitionIteratorBatchResults.clear();
        _spaceIteratorBatchResultProvider.close();
    }
}
