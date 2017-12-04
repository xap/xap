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
package com.j_spaces.core.cache.blobStore;

import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.MetricConstants;
import com.gigaspaces.metrics.MetricRegistrator;
import com.j_spaces.core.cache.CacheOperationReason;
import com.j_spaces.core.cache.context.Context;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_DELAULT;
import static com.j_spaces.core.Constants.CacheManager.FULL_CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_PROP;

/**
 * Off heap interface for internal cache
 *
 * @author yechiel
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class BlobStoreCacheHandler implements IBlobStoreCacheHandler {
    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE);

    private final int BLOB_STORE_INTERNAL_CACHE_CAPACITY;

    private final MetricRegistrator _blobstoreMetricRegistrar;

    private final IBlobStoreCacheImpl _blobStoreCacheImpl;
    private BlobStoreInternalCacheFilter _blobStoreInternalCacheFilter;
    private volatile boolean _isBlobStoreInternalCacheFilterEnabled = false;
    private boolean printLog = true;


    public BlobStoreCacheHandler(Properties properties) {
        BLOB_STORE_INTERNAL_CACHE_CAPACITY = Integer.parseInt(properties.getProperty(FULL_CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_PROP, CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_DELAULT));
        if (_logger.isLoggable(Level.INFO)) {
            _logger.info("BlobStore space data internal cache size=" + BLOB_STORE_INTERNAL_CACHE_CAPACITY);
        }
        _blobStoreCacheImpl = new BlobStoreCacheImpl(BLOB_STORE_INTERNAL_CACHE_CAPACITY);

        _blobstoreMetricRegistrar = (MetricRegistrator) properties.get("blobstoreMetricRegistrar");
        registerOperations();
    }

    private boolean evaluateAndReturnIfEntryMatchesFilter(Context context, BlobStoreEntryHolder entry){
        boolean val = _isBlobStoreInternalCacheFilterEnabled ? _blobStoreInternalCacheFilter.isEntryHotData(entry,context) : true;
        entry.getBlobStoreResidentPart().setMatchCacheFilter(this,val);
        return val;
    }

    private boolean isEntryHotData(BlobStoreEntryHolder entry){
        return entry.getBlobStoreResidentPart().isMatchCacheFilter(this);
    }

    @Override
    public BlobStoreEntryHolder get(BlobStoreRefEntryCacheInfo entryCacheInfo) {
        // TODO Auto-generated method stub
        if (isDisabledCache())
            return null;
        return
                _blobStoreCacheImpl.get(entryCacheInfo);
    }

    @Override
    public void handleOnSpaceOperation(Context context, BlobStoreEntryHolder entry, CacheOperationReason cacheOperationReason) {
        if (isDisabledCache())
            return;
        if (cacheOperationReason != CacheOperationReason.ON_TAKE)
        {
            if (entry.isDeleted() || entry.isPhantom() || entry.isOptimizedEntry())
                return;  //entry deleted
        }

        switch(cacheOperationReason) {
            case ON_READ:
                if (isEntryHotData(entry)) {
                    _blobStoreCacheImpl.storeOrTouch(entry);
                }
                else{
                    if(_isBlobStoreInternalCacheFilterEnabled) _blobStoreInternalCacheFilter.incrementColdDataMisses();//reading a cold entry
                }
                break;

            case ON_WRITE:
                if (evaluateAndReturnIfEntryMatchesFilter(context, entry))
                    _blobStoreCacheImpl.storeOrTouch(entry);
                break;

            case ON_UPDATE:
                boolean curMatch = isEntryHotData(entry);
                if(!curMatch){
                    if(_isBlobStoreInternalCacheFilterEnabled) _blobStoreInternalCacheFilter.incrementColdDataMisses();//updating a cold entry
                }
                if (evaluateAndReturnIfEntryMatchesFilter(context, entry)) {
                    _blobStoreCacheImpl.storeOrTouch(entry);
                }
                else
                {
                    if (curMatch) {
                        _blobStoreCacheImpl.remove(entry);
                    }
                }

                break;


            case ON_INITIAL_LOAD:
                if (evaluateAndReturnIfEntryMatchesFilter(context, entry)) {
                    if (!isFull()) {
                        if(_isBlobStoreInternalCacheFilterEnabled){
                            _blobStoreInternalCacheFilter.incrementInsertedToBlobStoreInternalCacheOnInitialLoad();
                        }
                        _blobStoreCacheImpl.storeOrTouch(entry);
                    }
                }
                break;

            case ON_TAKE:
                if(isEntryHotData(entry)) {
                    _blobStoreCacheImpl.remove(entry);
                }
                else {
                    if(_isBlobStoreInternalCacheFilterEnabled) _blobStoreInternalCacheFilter.incrementColdDataMisses();//taking a cold entry
                }
                break;

            default:
                throw new UnsupportedOperationException("invalid space operation in BlobStore cache");
        }
    }

    private boolean isDisabledCache() {
        return BLOB_STORE_INTERNAL_CACHE_CAPACITY == 0;
    }

    private void registerOperations() {
        _blobstoreMetricRegistrar.register(MetricConstants.CACHE_SIZE, _blobStoreCacheImpl.getCacheSize());

        _blobstoreMetricRegistrar.register("cache-miss", new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return getMissCount();
            }
        });

        _blobstoreMetricRegistrar.register("cache-hit", _blobStoreCacheImpl.getHitCount());
    }

    @Override
    public boolean isFull(){
        boolean isFull = _blobStoreCacheImpl.isFull();
        if(isFull){
            if(printLog) {
                _logger.info("Blobstore cache is full with size [ " + getCacheSize() +" ]");
                printLog = false;
            }
        }
        return isFull;
    }

    @Override
    public long getCacheSize() {
        return _blobStoreCacheImpl.getCacheSize().getCount();
    }

    @Override
    public BlobStoreInternalCacheFilter getBlobStoreInternalCacheFilter() {
        return _blobStoreInternalCacheFilter;
    }

    @Override
    public void setBlobStoreInternalCacheFilter(BlobStoreInternalCacheFilter blobStoreInternalCacheFilter) {
        _blobStoreInternalCacheFilter = blobStoreInternalCacheFilter;
        _isBlobStoreInternalCacheFilterEnabled = blobStoreInternalCacheFilter != null;
        if(_isBlobStoreInternalCacheFilterEnabled){
            registerHotDataMetric();
        }
    }

    private void registerHotDataMetric() {
        _blobstoreMetricRegistrar.register("hot-data-cache-miss", _blobStoreCacheImpl.getMissCount());
    }

    @Override
    public long getMissCount(){
        long coldDataMisses = _isBlobStoreInternalCacheFilterEnabled ? _blobStoreInternalCacheFilter.getColdDataMissCount().getCount() : 0;

        return _blobStoreCacheImpl.getMissCount().getCount()+coldDataMisses;
    }

    @Override
    public long getHitCount(){
        return _blobStoreCacheImpl.getHitCount().getCount();
    }

}
