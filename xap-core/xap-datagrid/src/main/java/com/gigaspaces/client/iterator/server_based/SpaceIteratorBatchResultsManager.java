package com.gigaspaces.client.iterator.server_based;

import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.Constants;

import java.io.Closeable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
public class SpaceIteratorBatchResultsManager implements Closeable{
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_GSITERATOR);
    private final Map<Integer, SpaceIteratorBatchResult> _partitionIteratorBatchResults;
    private final SpaceIteratorBatchResultProvider _spaceIteratorBatchResultProvider;
    private int _activePartitions;

    public SpaceIteratorBatchResultsManager(ISpaceProxy spaceProxy, int batchSize, int readModifiers, ITemplatePacket queryPacket){
        this._partitionIteratorBatchResults = new ConcurrentHashMap<>();
        this._spaceIteratorBatchResultProvider = new SpaceIteratorBatchResultProvider(spaceProxy, batchSize, readModifiers, queryPacket, UUID.randomUUID());
        this._activePartitions = this._spaceIteratorBatchResultProvider.getInitialNumberOfActivePartitions();
    }

    /*
    check if any results have failed
     */
    public boolean anyFinishedExceptionally(){
        return allFinished() && _partitionIteratorBatchResults
                .values()
                .stream()
                .anyMatch(SpaceIteratorBatchResult::isFailed);
    }

    /*
    check if all results are in FINISHED state
     */
    public boolean allFinishedSuccessfully(){
        return allFinished() &&
                _partitionIteratorBatchResults
                .values()
                .stream()
                .allMatch(SpaceIteratorBatchResult::isFinished);
    }

    public SpaceIteratorBatchResult getNextBatch(long timeout){
        try {
            SpaceIteratorBatchResult spaceIteratorBatchResult = _spaceIteratorBatchResultProvider.consumeBatch(timeout); //Timeout Exception? Fail iterator completely?
            if(spaceIteratorBatchResult != null) {
                _partitionIteratorBatchResults.put(spaceIteratorBatchResult.getPartitionId(), spaceIteratorBatchResult);
                if (spaceIteratorBatchResult.getEntries() == null) {
                    spaceIteratorBatchResult.setIteratorStatus(SpaceIteratorBatchStatus.FINISHED);
                }
                if (spaceIteratorBatchResult.getEntries() != null && spaceIteratorBatchResult.getEntries().length < _spaceIteratorBatchResultProvider.getBatchSize()) {
                    spaceIteratorBatchResult.setIteratorStatus(SpaceIteratorBatchStatus.FINISHED);
                }
                if (spaceIteratorBatchResult.getIteratorStatus().equals(SpaceIteratorBatchStatus.READY)){
                    spaceIteratorBatchResult.setIteratorStatus(SpaceIteratorBatchStatus.WAITING);
                    _spaceIteratorBatchResultProvider.triggerSinglePartitionBatchTask(spaceIteratorBatchResult.getPartitionId(), false);
                }
                return spaceIteratorBatchResult;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void deactivatePartition(SpaceIteratorBatchResult spaceIteratorBatchResult){
        if(_logger.isLoggable(Level.FINE))
            _logger.fine("Deactivating partition " + spaceIteratorBatchResult.getPartitionId());
        _activePartitions--;
    }

    public boolean allFinished(){
        return _activePartitions == 0;
    }

    @Override
    public void close() {
        _partitionIteratorBatchResults.clear();
        _spaceIteratorBatchResultProvider.close();
    }

}
