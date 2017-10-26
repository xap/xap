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

package com.j_spaces.core.cache.blobStore.sadapter;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.server.blobstore.BlobStoreGetBulkOperationResult;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.EntryCacheInfoFactory;
import com.j_spaces.core.cache.InitialLoadInfo;
import com.j_spaces.core.cache.blobStore.BlobStoreEntryLayout;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.blobStore.IBlobStoreEntryHolder;
import com.j_spaces.kernel.JSpaceUtilities;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

@com.gigaspaces.api.InternalApi
public class BlobStoreSegmentInitialLoader extends Thread {


    private final DataIterator<BlobStoreGetBulkOperationResult> _segmentIter;
    private final CacheManager _cacheManager;
    private final int _segmentNumber;
    private final CountDownLatch _sync;
    private final InitialLoadInfo _initialLoadInfo;


    public BlobStoreSegmentInitialLoader(CacheManager cacheManager, DataIterator<BlobStoreGetBulkOperationResult> segmentIter,
                                         CountDownLatch sync, int segmentNumber, InitialLoadInfo initialLoadInfo) {
        _segmentIter = segmentIter;
        _cacheManager = cacheManager;
        _sync = sync;
        _initialLoadInfo = initialLoadInfo;
        _segmentNumber = segmentNumber;
    }

    @Override
    public void run() {
        //milk the segment iterator
        Context context = _cacheManager.getCacheContext();

        try {

            while (true) {
                Object o = null;
                if (_segmentIter.hasNext())
                    o = _segmentIter.next();
                if (o == null) {
                    _segmentIter.close();
                    return;
                }
                _initialLoadInfo.incrementFoundInDatabase();

                BlobStoreGetBulkOperationResult res = (BlobStoreGetBulkOperationResult) o;
                IEntryHolder eh = ((BlobStoreEntryLayout) res.getData()).buildBlobStoreEntryHolder(_cacheManager);
                EntryCacheInfoFactory.createBlobStoreEntryCacheInfo(eh);
                IBlobStoreEntryHolder oeh = (IBlobStoreEntryHolder) eh;
                oeh.getBlobStoreResidentPart().setBlobStorePosition(res.getPosition());

                //is it fifo or fifo-grouping? if so put entry in temp sort
                if (eh.getServerTypeDesc().isFifoSupported() || eh.getServerTypeDesc().getTypeDesc().getFifoGroupingPropertyPath() != null) {//fifo or f-g need to sort
                    if (_initialLoadInfo.getCurTypeData() == null || _initialLoadInfo.getCurDesc() != eh.getServerTypeDesc()) {
                        _initialLoadInfo.setCurTypeData(_cacheManager.getTypeData(eh.getServerTypeDesc()));
                        _initialLoadInfo.setCurDesc(eh.getServerTypeDesc());
                    }
                    _initialLoadInfo.getBlobStoreFifoInitialLoader().add(eh, _initialLoadInfo.getCurTypeData());
                    continue;
                }


                //insert eh to space
                _cacheManager.safeInsertEntryToCache(context, eh, false /* newEntry */, null /*pType*/, false /*pin*/, CacheManager.InitialLoadOrigin.FROM_BLOBSTORE /*fromInitialLoad*/);
                ((IBlobStoreEntryHolder) eh).getBlobStoreResidentPart().unLoadFullEntryIfPossible(_cacheManager, context);

                _initialLoadInfo.incrementInsertedToCache();
                _initialLoadInfo.setLastLoggedTime(logInsertionIfNeeded(_initialLoadInfo.getRecoveryStartTime(), _initialLoadInfo.getLastLoggedTime(), _initialLoadInfo.getInsertedToCache()));
            }
        } catch (Exception ex) {
            RuntimeException rte = new RuntimeException(ex);
            _segmentIter.close();
            throw rte;
        } finally {

            if (context != null)
                _cacheManager.freeCacheContext(context);
            _sync.countDown();
        }
    }

    private long logInsertionIfNeeded(long startLogTime, long lastLogTime, int fetchedEntries) {
        if (_initialLoadInfo.isLogRecoveryProcess() && _initialLoadInfo.getLogger().isLoggable(Level.INFO)) {
            long curTime = SystemTime.timeMillis();
            if (curTime - lastLogTime > _initialLoadInfo.getRecoveryLogInterval()) {
                _initialLoadInfo.getLogger().info("BlobStore segment #" + _segmentNumber + " entries loaded so far : " + fetchedEntries + " [" + JSpaceUtilities.formatMillis(curTime - startLogTime) + "]");
                return curTime;
            }
        }
        return lastLogTime;
    }

}
