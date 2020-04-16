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
package com.j_spaces.core.cache.blobStore.storage.preFetch;

import com.gigaspaces.server.blobstore.BlobStoreException;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.blobStore.BlobStoreRefEntryCacheInfo;
import com.j_spaces.core.cache.blobStore.storage.bulks.BlobStoreReadBulkHandler;
import com.j_spaces.core.cache.context.Context;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * handlels prefetch activity
 *
 * @author yechiel
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class BlobStorePreFetchBatchHandler implements Callable<BlobStorePreFetchBatchResult> {

    public final static long FETCH_WAIT_TIME = 60 * 1000;

    private final List<BlobStoreRefEntryCacheInfo> _entries;
    private volatile BlobStorePreFetchBatchResult _result;
    private volatile Future<BlobStorePreFetchBatchResult> _future;
    private final boolean _executeByMainThread;
    private final CacheManager _cacheManager;
    private final BlobStorePreFetchIteratorBasedHandler _caller;
    private final Logger _logger;


    public BlobStorePreFetchBatchHandler(CacheManager cacheManager, boolean executeByMainThread, BlobStorePreFetchIteratorBasedHandler caller, Logger logger) {
        _entries = new LinkedList<BlobStoreRefEntryCacheInfo>();
        _cacheManager = cacheManager;
        _executeByMainThread = executeByMainThread;
        _caller = caller;
        _logger = logger;
    }

    public int addEntry(BlobStoreRefEntryCacheInfo e) {
        if (e != null)
            _entries.add(e);
        return _entries.size();
    }

    public List<BlobStoreRefEntryCacheInfo> getEntries() {
        return _entries;
    }

    public BlobStorePreFetchBatchResult getResult() {
        return _result;
    }


    public void setResult(BlobStorePreFetchBatchResult res) {
        _result = res;
    }

    public boolean hasResult() {
        return _result != null;
    }

    public BlobStorePreFetchBatchResult getReadyResult() {
        if (_executeByMainThread || _result != null) {
            return _result;
        }
        //poolthread execution wait for it
        try {
            _future.get(FETCH_WAIT_TIME, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (_result != null) {
                _result.setException(e);
            }
            _logger.error("PreFetchBatchHandler got an exception:", e);

            throw new BlobStoreException(e);
        }
        if (!_future.isDone()) {
            BlobStoreException ex = new BlobStoreException("BlobStorePreFetchBatchHandler:getReadyResult could not get any batch on time");
            _logger.error("BlobStorePreFetchBatchHandler got an exception:", ex);
            throw ex;
        }
        return _result;
    }

    public boolean isReadyResult() {
        return _executeByMainThread || _future.isDone();
    }

    public void setFuture(Future<BlobStorePreFetchBatchResult> f) {
        _future = f;
    }


    @Override
    public BlobStorePreFetchBatchResult call() {
        Thread.currentThread().setName("GS-BlobStorePreFetchHandler#" + Thread.currentThread().getId());
        Context context = _cacheManager.getCacheContext();
        try {
            BlobStorePreFetchBatchResult res = execute(context);
            //notify the iterator which called us
            _caller.notifyResultReady(this);
            return res;
        } finally {
            if (context != null)
                _cacheManager.freeCacheContext(context);
        }

    }

    public BlobStorePreFetchBatchResult execute(Context context) {
        //call execute bulk-read on the blob-store
        BlobStoreReadBulkHandler bh = new BlobStoreReadBulkHandler(_cacheManager, this);
        setResult(bh.execute());
        return _result;
    }

    public boolean isExcecuteByMainThread() {
        return _executeByMainThread;
    }

}
