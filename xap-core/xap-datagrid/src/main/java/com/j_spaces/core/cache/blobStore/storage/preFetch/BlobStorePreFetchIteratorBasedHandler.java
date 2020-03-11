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

package com.j_spaces.core.cache.blobStore.storage.preFetch;

import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.server.blobstore.BlobStoreException;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.blobStore.BlobStoreRefEntryCacheInfo;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.list.IScanListIterator;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * handles prefetch activity
 *
 * @author yechiel
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class BlobStorePreFetchIteratorBasedHandler implements IScanListIterator<IEntryCacheInfo> {
    private static final int MAX_BATCH_SIZE = 5000; //per thread
    private static final int MAX_QUOTA_SIZE_IN_BATCHES = 5;  //all threads together
    //    private static final double  PREFETCH_FACTOR = 1.5;  //actual tp bring
    private static final double PREFETCH_FACTOR = 1.0;  //actual tp bring


    private final IScanListIterator<BlobStoreRefEntryCacheInfo> _mainIterator;
    private boolean _mainIterTerminated;
    private final ITemplateHolder _template;
    private final Object _lock;
    private final LinkedList<BlobStorePreFetchBatchHandler> _requests;
    private Iterator<BlobStoreRefEntryCacheInfo> _curIter;
    private BlobStorePreFetchBatchHandler _curHandeledRequest;
    private boolean _finished;
    private final Context _mainThreadContext;
    private final CacheManager _cacheManager;
    private final int _numEntriesRequested;
    private int _numEntriesSoFar;
    private int _quotaNumber;
    private final Logger _logger;


    public BlobStorePreFetchIteratorBasedHandler(Context mainThreadContext, CacheManager cacheManager, IScanListIterator<IEntryCacheInfo> mainIterator,
                                                 ITemplateHolder template, int numEntriesRequested, Logger logger) {
        _mainIterator = (IScanListIterator<BlobStoreRefEntryCacheInfo>) ((Object) mainIterator);
        _template = template;
        _lock = new Object();
        _requests = new LinkedList<BlobStorePreFetchBatchHandler>();
        _mainThreadContext = mainThreadContext;
        _cacheManager = cacheManager;
        _numEntriesRequested = numEntriesRequested != Integer.MAX_VALUE ? (int) (numEntriesRequested * PREFETCH_FACTOR) : numEntriesRequested;
        _logger = logger;
    }

    public static IScanListIterator<IEntryCacheInfo> createPreFetchIterIfRelevant(Context context, CacheManager cacheManager, IScanListIterator<IEntryCacheInfo> mainIterator, ITemplateHolder template, Logger logger) {
        return isRelevantForPreFetchIterator(context, cacheManager, mainIterator, template) ?
                new BlobStorePreFetchIteratorBasedHandler(context, cacheManager, mainIterator, template, template.getBatchOperationContext().getMaxEntries(), logger) : mainIterator;
//                new BlobStorePreFetchIteratorBasedHandler(context,cacheManager,mainIterator,template,Math.min(template.getBatchOperationContext().getMaxEntries(), MAX_OVERALL),logger) : mainIterator  ;
    }

    private static boolean isRelevantForPreFetchIterator(Context context, CacheManager cacheManager, IScanListIterator<IEntryCacheInfo> mainIterator, ITemplateHolder template) {
        return (mainIterator.isIterator() && template.isBatchOperation() && /*cacheManager.isPersistentBlobStore()*/ cacheManager.isBlobStoreCachePolicy() && cacheManager.useBlobStorePreFetch() && !context.isDisableBlobStorePreFetching() && template.getBatchOperationContext().getMaxEntries() > 1 && !template.isFifoGroupPoll() && template.getXidOriginated() == null);
    }

    @Override
    public boolean hasNext() throws SAException {

        while (true) {
            if (_finished) {
                replaceContextPreFetchResult();
                return false;
            }
            if (_curIter != null) {
                if (_curIter.hasNext())
                    return true;

                _curIter = null;
                //remove the processed request
                _requests.remove(_curHandeledRequest);
                _curHandeledRequest = null;
                if (_requests.isEmpty() && _mainIterTerminated) {
                    _finished = true;
                    replaceContextPreFetchResult();
                    return false;
                }
            }
            //do we have another result to use ?
            _curHandeledRequest = getNextBatch();
            if (_curHandeledRequest != null) {
                _curIter = _curHandeledRequest.getEntries().iterator();
                replaceContextPreFetchResult();
            }
        }
    }

    private void replaceContextPreFetchResult() {
        if (_finished)
            _mainThreadContext.setBlobStorePreFetchBatchResult(null);
        else
            _mainThreadContext.setBlobStorePreFetchBatchResult(_curHandeledRequest.getResult());
    }

    private BlobStorePreFetchBatchHandler getNextBatch() {
        if (_template.isFifoTemplate()) {
            return getFirstBatchForProcessing();
        } else {
            return getNextBatchNonFifo();
        }

    }

    private BlobStorePreFetchBatchHandler getFirstBatchForProcessing() {
        //is there more requests ? if yes wait for the first
        BlobStorePreFetchBatchHandler r = getFirstBatch();
        if (_finished)
            return null;
        if (!r.isExcecuteByMainThread())
            r.getReadyResult();

        return r;

    }

    private BlobStorePreFetchBatchHandler getFirstBatch() {
        //is there more requests ? if yes wait for the first
        if (!_requests.isEmpty())
            return _requests.getFirst();
        //empty requests
        bringNextQuota();
        if (_finished)
            return null;
        return _requests.getFirst();
    }

    private BlobStorePreFetchBatchHandler getNextBatchNonFifo() {
        if (_requests.isEmpty())
            bringNextQuota();
        if (_finished)
            return null;
        //at least one request-
        if (_requests.size() == 1)
            return getFirstBatchForProcessing();
        //several- look for finished, if not wait for it
        BlobStorePreFetchBatchHandler r = null;
        r = getAnyCompleted();
        if (r != null)
            return r;
        synchronized (_lock) {
            r = getAnyCompleted();
            if (r != null)
                return r;
            long upto = System.currentTimeMillis() + BlobStorePreFetchBatchHandler.FETCH_WAIT_TIME;
            long w = BlobStorePreFetchBatchHandler.FETCH_WAIT_TIME;
            while (true) {
                //wait for any to finish
                try {
                    _lock.wait(w);
                } catch (InterruptedException e) {
                    if (_logger.isLoggable(Level.SEVERE))
                        _logger.log(Level.SEVERE, "PreFetchIterator got an exception:", e);

                    throw new BlobStoreException(e);
                }
                r = getAnyCompleted();
                if (r != null)
                    return r;
                long cur = System.currentTimeMillis();
                if (cur < upto) {
                    w = upto - cur;
                    continue;
                }
                break;
            }
            if (r == null) {
                BlobStoreException ex = new BlobStoreException("PreFetchIterator:getNextBatchNonFifo could not get any batch on time");
                if (_logger.isLoggable(Level.SEVERE))
                    _logger.log(Level.SEVERE, "PreFetchIterator got an exception:", ex);
                throw ex;
            }
        }
        return r;
    }

    public void notifyResultReady(BlobStorePreFetchBatchHandler r) {
        if (!r.isExcecuteByMainThread()) {
            synchronized (_lock) {
                _lock.notify();
            }
        }
    }

    private BlobStorePreFetchBatchHandler getAnyCompleted() {
        for (BlobStorePreFetchBatchHandler r : _requests) {
            if (r.isReadyResult())
                return r;
        }
        return null;
    }


    private void bringNextQuota() {
        boolean finishedQuota = false;
        if (_finished)
            return;
        if (_mainIterTerminated) {
            _finished = true;
            return;
        }
        _requests.clear();
        _quotaNumber++;

        int numToLimit = _quotaNumber == 1 ? _numEntriesRequested : _numEntriesRequested / 2;
        if (numToLimit <= 1)
            numToLimit = 2;
        int numThisQuota = 0;

        //if we already passed the requested am

        try {
            //prepare batch requests and execute
            //milk the iterator prepare requests and feed them
            BlobStorePreFetchBatchHandler first = null;
            for (int bn = 0; bn < MAX_QUOTA_SIZE_IN_BATCHES; bn++) {
                BlobStorePreFetchBatchHandler cur = null;
                if (_mainIterTerminated || finishedQuota)
                    break;
                while (true) {
                    if (numThisQuota >= numToLimit) {
                        finishedQuota = true;
                        break;
                    }
                    if (!_mainIterator.hasNext()) {
                        _mainIterTerminated = true;
                        finishedQuota = true;
                        break;
                    }
                    if (cur == null)
                        cur = new BlobStorePreFetchBatchHandler(_cacheManager, first == null, this, _logger);
                    _numEntriesSoFar++;
                    numThisQuota++;
                    if (cur.addEntry(_mainIterator.next()) >= MAX_BATCH_SIZE)
                        break;
                }
                if (cur != null)
                    _requests.addLast(cur);
                if (first == null) {
                    first = cur;
                    cur = null;
                } else {
                    if (cur != null)
                        execute(_mainThreadContext, cur);
                }
            }
            if (first != null) {
                //directly by me
                execute(_mainThreadContext, first);
            }
            if (_requests.isEmpty()) {
                _finished = true;
                return;
            }
        } catch (Throwable t) {
            throw new BlobStoreException("PreFetchIterator:bringNextQuota " + t);
        }
    }


    private void execute(Context mainThreadContext, BlobStorePreFetchBatchHandler bh) {
        if (!bh.isExcecuteByMainThread()) {
            bh.setFuture(_cacheManager.getBlobStoreStorageHandler().getPreFetchPool().submit(bh));
        } else {//main thread direct call
            bh.execute(mainThreadContext);
        }
    }


    @Override
    public IEntryCacheInfo next() throws SAException {
        return _curIter.next();
    }

    @Override
    public void releaseScan() throws SAException {

    }

    /**
     * if the scan is on a property index, currently supported for extended index
     */
    @Override
    public int getAlreadyMatchedFixedPropertyIndexPos() {
        return -1;
    }

    @Override
    public String getAlreadyMatchedIndexPath() {
        return null;
    }


    /**
     * is the entry returned already matched against the searching template currently is true if the
     * underlying scan made by CacheManager::EntriesIter
     */
    @Override
    public boolean isAlreadyMatched() {
        return false;
    }

    @Override
    public boolean isExtensionIndex() {
        return false;
    }

    @Override
    public boolean isIterator() {
        return true;
    }
}
