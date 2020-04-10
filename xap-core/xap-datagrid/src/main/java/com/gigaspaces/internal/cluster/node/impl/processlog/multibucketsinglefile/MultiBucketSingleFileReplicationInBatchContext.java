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

package com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile;

import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.impl.ReplicationInContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.IMultiBucketSingleFileReplicationOrderedPacket;
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.ShortLongMap;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@com.gigaspaces.api.InternalApi
public class MultiBucketSingleFileReplicationInBatchContext
        extends ReplicationInContext implements IReplicationInBatchContext {

    private final IBatchExecutedCallback _batchExecutedCallback;
    private final LinkedList<Object> _batchContext = new LinkedList<Object>();
    private final int _entireBatchSize;
    private final ShortLongMap _bucketProcessedKeys = CollectionsFactory.getInstance().createShortLongMap();
    private final ShortLongMap _bucketGlobalProcessedKeys = CollectionsFactory.getInstance().createShortLongMap();
    private long _lastUnprocessedKey;
    private IMultiBucketSingleFileReplicationOrderedPacket _currentPacket;
    private Object _tagObject;

    private int _snapshotBatchContextSize;
    private long _snapshotLastUnprocessedKeyInBatch;

    public MultiBucketSingleFileReplicationInBatchContext(
            IBatchExecutedCallback batchExecutedCallback,
            String sourceLookupName, String groupName, Logger contextLogger, int entireBatchSize) {
        super(sourceLookupName, groupName, contextLogger, false, false);
        _batchExecutedCallback = batchExecutedCallback;
        _entireBatchSize = entireBatchSize;
    }

    public void pendingConsumed() {
        // Protect from incorrect usage where pendingConsumed is called without
        // any prior addPendingContext
        // invocation
        if (_batchContext.isEmpty())
            throw new IllegalStateException("No pending operation to consume");

        _batchContext.clear();
        _batchExecutedCallback.batchConsumed(_bucketProcessedKeys,
                _bucketGlobalProcessedKeys,
                _lastUnprocessedKey);

    }

    public <T> void addPendingContext(T operationContext) {
        _batchContext.add(operationContext);
        updatePendingProcessedKeys();
    }

    public void updatePendingProcessedKeys() {
        for (short bucketIndex : _currentPacket.getBuckets()) {
            long bucketKey = _currentPacket.getBucketKey(bucketIndex);
            _bucketProcessedKeys.put(bucketIndex, bucketKey);
            _bucketGlobalProcessedKeys.put(bucketIndex, _currentPacket.getKey());
        }
        _lastUnprocessedKey = _currentPacket.getKey();
    }

    public <T> List<T> getPendingContext() {
        return (List<T>) _batchContext;
    }

    public void setCurrentPacket(
            IMultiBucketSingleFileReplicationOrderedPacket packet) {
        _currentPacket = packet;

    }

    public void currentConsumed() {
        updatePendingProcessedKeys();
        _batchExecutedCallback.batchConsumed(_bucketProcessedKeys,
                _bucketGlobalProcessedKeys,
                _lastUnprocessedKey);
    }

    public <T> void setTagObject(T tagObject) {
        _tagObject = tagObject;
    }

    @SuppressWarnings("unchecked")
    public <T> T getTagObject() {
        return (T) _tagObject;
    }

    public int getEntireBatchSize() {
        return _entireBatchSize;
    }

    public void snapshot() {
        _snapshotBatchContextSize = _batchContext.size();
        _snapshotLastUnprocessedKeyInBatch = _lastUnprocessedKey;
    }

    public void rollback() {
        int index = 0;
        for (Iterator<Object> iterator = _batchContext.iterator(); iterator.hasNext(); ) {
            iterator.next();
            if (index++ >= _snapshotBatchContextSize)
                iterator.remove();
        }
        _lastUnprocessedKey = _snapshotLastUnprocessedKeyInBatch;
    }

    @Override
    public boolean isBatchContext() {
        return true;
    }

}
