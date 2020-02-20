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
    private boolean _firstTime;

    public SinglePartitionGetBatchForIteratorSpaceTask(SpaceIteratorBatchResultProvider _spaceIteratorBatchResultProvider) {
        this._spaceIteratorBatchResultProvider = _spaceIteratorBatchResultProvider;
    }

    public boolean isFirstTime() {
        return _firstTime;
    }

    public SinglePartitionGetBatchForIteratorSpaceTask setFirstTime(boolean firstTime) {
        this._firstTime = firstTime;
        return this;
    }

    @Override
    public SpaceIteratorBatchResult execute(IJSpace space, Transaction tx) throws Exception {
        return space.getDirectProxy().getBatchForIterator(_spaceIteratorBatchResultProvider.getQueryPacket(), _spaceIteratorBatchResultProvider.getBatchSize(), _spaceIteratorBatchResultProvider.getReadModifiers(), _spaceIteratorBatchResultProvider.getUuid(), _firstTime);
    }

}
