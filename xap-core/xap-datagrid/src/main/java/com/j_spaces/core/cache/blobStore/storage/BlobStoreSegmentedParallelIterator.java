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

//
package com.j_spaces.core.cache.blobStore.storage;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.server.blobstore.BlobStoreGetBulkOperationResult;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A parallerl iterator for retrieving data parallely from blob store using segments used in
 * initial-load note- a single thread should use this iterator
 *
 * @author yechiel
 * @since 10.0
 */

public abstract class BlobStoreSegmentedParallelIterator implements DataIterator<BlobStoreGetBulkOperationResult> {

    private final int QUEUE_CAPACITY = 15000;

    private int _segmentsToDo;
    private final int _numSegments;
    private final BlockingQueue<Object> _objectsQueue;
    private final static Object _terminationIndicator = new Object();
    private boolean _finished;
    private boolean _initialized;

    private static final long TIME_TO_WAIT = 120;

    public BlobStoreSegmentedParallelIterator(int numSegments) {
        super();
        _segmentsToDo = numSegments;
        _numSegments = numSegments;
        _objectsQueue = new LinkedBlockingQueue<Object>(QUEUE_CAPACITY);
    }

    @Override
    public BlobStoreGetBulkOperationResult next() {
        if (!_initialized) {
            //create a per segment loader which will fill the objects queue
            startPerSegmentLoaders();
            _initialized = true;
        }

        while (true) {
            if (!hasNext())
                return null;
            Object o = null;
            try {
                o = _objectsQueue.poll(TIME_TO_WAIT, TimeUnit.SECONDS);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            if (o == null)
                throw new RuntimeException("next() : timeout reached while reading blobstore parallel iterator");

            if (o == _terminationIndicator) {
                _segmentsToDo--;
                if (hasNext())
                    continue;
                return null;
            }
            if (o instanceof Throwable) {
                _finished = true;
                throw new RuntimeException((Throwable) o);
            }
            return (BlobStoreGetBulkOperationResult) o;
        }
    }

    @Override
    public boolean hasNext() {
        return !_finished && (_segmentsToDo > 0);
    }

    @Override
    public void close() {

    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public Object getTerminationIndicator() {
        return _terminationIndicator;
    }

    public BlockingQueue<Object> getObjectsQueue() {
        return _objectsQueue;
    }

    private void startPerSegmentLoaders() {
        for (int segment = 0; segment < _numSegments; segment++) {
            BlobStoreSegmentDataLoader dl = new BlobStoreSegmentDataLoader(this, segment, createDataIteratorForSegmen(segment));
            dl.start();
        }
    }


    public abstract DataIterator<BlobStoreGetBulkOperationResult> createDataIteratorForSegmen(int segmentNumber);
}
