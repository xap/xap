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

import java.util.concurrent.TimeUnit;

@com.gigaspaces.api.InternalApi
public class BlobStoreSegmentDataLoader extends Thread {
    /**
     * loader of a sinfle segment  from blob store used in initial-load
     *
     * @author yechiel
     * @since 10.0
     */
    private static final int QUEUE_OFFER_TIME_OUT = 180;

    private final int _segment;
    private final BlobStoreSegmentedParallelIterator _mainIter;
    private final DataIterator<BlobStoreGetBulkOperationResult> _segmentIter;

    public BlobStoreSegmentDataLoader(BlobStoreSegmentedParallelIterator mainIter, int segment, DataIterator<BlobStoreGetBulkOperationResult> segmentIter) {
        _segment = segment;
        _mainIter = mainIter;
        _segmentIter = segmentIter;
    }

    @Override
    public void run() {
        //milk the segment iterator

        boolean terminated = false;
        try {
            while (true) {
                Object o = null;
                if (_segmentIter.hasNext())
                    o = _segmentIter.next();
                if (o == null) {
                    _segmentIter.close();
                    terminated = true;
                    offer(_mainIter.getTerminationIndicator());
                    return;
                }
                offer(o);
            }
        } catch (Exception ex) {
            RuntimeException rte = new RuntimeException(ex);
            _mainIter.getObjectsQueue().add(rte);
            _segmentIter.close();
            try {
                if (!terminated)
                    offer(_mainIter.getTerminationIndicator());
            } catch (Exception ex1) {
            }
            throw rte;
        }
    }

    private void offer(Object object)
            throws InterruptedException {
        while (true) {
            if (_mainIter.getObjectsQueue().offer(object, QUEUE_OFFER_TIME_OUT, TimeUnit.SECONDS))
                return;
        }
    }
}
