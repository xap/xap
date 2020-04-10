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

package com.gigaspaces.internal.cluster.node.impl;

import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author idan
 * @since 9.1
 */
public abstract class AbstractReplicationInBatchContext
        extends ReplicationInContext
        implements IReplicationInBatchContext {

    private final LinkedList<Object> _batchContext = new LinkedList<Object>();
    private final int _entireBatchSize;
    private long _lastUnprocessedKeyInBatch;
    private long _currentKey;
    private Object _tagObject;

    private int _snapshotBatchContextSize;
    private long _snapshotLastUnprocessedKeyInBatch;

    public AbstractReplicationInBatchContext(
            Logger contextLogger, int entireBatchSize, String sourceLookupName, String groupName, boolean supportDistTransactionConsolidation) {
        super(sourceLookupName, groupName, contextLogger, false, supportDistTransactionConsolidation);
        _entireBatchSize = entireBatchSize;
    }

    public void pendingConsumed() {
        // Protect from incorrect usage where pendingConsumed is called without
        // any prior addPendingContext
        // invocation
        if (_batchContext.isEmpty())
            throw new IllegalStateException("No pending operation to consume");

        _batchContext.clear();
        afterBatchConsumed(_lastUnprocessedKeyInBatch);
    }

    public <T> void addPendingContext(T operationContext) {
        _batchContext.add(operationContext);
        _lastUnprocessedKeyInBatch = _currentKey;
    }

    public <T> List<T> getPendingContext() {
        return (List<T>) _batchContext;
    }

    public void setCurrentKey(long currentKey) {
        _currentKey = currentKey;
    }

    public void currentConsumed() {
        _lastUnprocessedKeyInBatch = _currentKey;
        afterBatchConsumed(_lastUnprocessedKeyInBatch);
    }

    protected abstract void afterBatchConsumed(long lastProcessedKeyInBatch);

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

    public long getCurrentKey() {
        return _currentKey;
    }

    public void snapshot() {
        _snapshotBatchContextSize = _batchContext.size();
        _snapshotLastUnprocessedKeyInBatch = _lastUnprocessedKeyInBatch;
    }

    public void rollback() {
        int index = 0;
        for (Iterator<Object> iterator = _batchContext.iterator(); iterator.hasNext(); ) {
            iterator.next();
            if (index++ >= _snapshotBatchContextSize)
                iterator.remove();
        }
        _lastUnprocessedKeyInBatch = _snapshotLastUnprocessedKeyInBatch;
    }

    @Override
    public boolean isBatchContext() {
        return true;
    }

}
