package com.gigaspaces.client.iterator.server_based;

import com.gigaspaces.executor.SpaceTask;
import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.j_spaces.core.IJSpace;
import net.jini.core.transaction.Transaction;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class SinglePartitionGetBatchForIteratorSpaceTask implements SpaceTask<SpaceIteratorBatchResult> {
    private final SpaceIteratorBatchResultProvider _spaceIteratorBatchResultProvider;
    private final int _batchNumber;

    public SinglePartitionGetBatchForIteratorSpaceTask(SpaceIteratorBatchResultProvider _spaceIteratorBatchResultProvider, int batchNumber) {
        this._spaceIteratorBatchResultProvider = _spaceIteratorBatchResultProvider;
        this._batchNumber = batchNumber;
    }

    @Override
    public SpaceIteratorBatchResult execute(IJSpace space, Transaction tx) throws Exception {
        return space.getDirectProxy().getBatchForIterator(_spaceIteratorBatchResultProvider.getQueryPacket(), _spaceIteratorBatchResultProvider.getBatchSize(), _batchNumber, _spaceIteratorBatchResultProvider.getReadModifiers(), _spaceIteratorBatchResultProvider.getUuid());
    }

}
