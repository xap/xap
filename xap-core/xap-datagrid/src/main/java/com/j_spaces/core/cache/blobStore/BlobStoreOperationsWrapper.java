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

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.metrics.MetricRegistrator;
import com.gigaspaces.metrics.ThroughputMetric;
import com.gigaspaces.server.blobstore.*;
import com.j_spaces.core.Constants;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.blobStore.offheap.OffHeapStorageContainer;
import com.j_spaces.core.cache.blobStore.offheap.OffHeapMemoryPool;
import com.j_spaces.kernel.threadpool.DynamicExecutors;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

/**
 * provide a wrapper over blobstore methods, used for serialization to byte-array, trapping stats
 * etc
 *
 * @author yechiel
 * @since 10.0
 */

@com.gigaspaces.api.InternalApi
public class BlobStoreOperationsWrapper extends BlobStoreExtendedStorageHandler {

    private static final int _blobStorePreFetchMinThreads = Integer.getInteger(Constants.CacheManager.CACHE_MANAGER_BLOBSTORE_PREFETCH_MIN_THREADS_PROP, Constants.CacheManager.CACHE_MANAGER_BLOBSTORE_PREFETCH_MIN_THREADS_DEFAULT);
    private static final int _blobStorePreFetchMaxThreads = Integer.getInteger(Constants.CacheManager.CACHE_MANAGER_BLOBSTORE_PREFETCH_MAX_THREADS_PROP, Constants.CacheManager.CACHE_MANAGER_BLOBSTORE_PREFETCH_MAX_THREADS_DEFAULT);

    private final CacheManager _cacheManager;
    private final BlobStoreStorageHandler _blobStore;
    private final BlobStoreSerializationUtils _serialization;
    private final boolean _needSerialization;
    private final OffHeapMemoryPool _offHeapCache;
    private final boolean _isOffHeap;
    private final OffHeapMemoryPool _offHeapStore;

    private MetricRegistrator _registrator;
    private ExecutorService _preFetchThreadPool;

    private final LongCounter add = new LongCounter();
    private final LongCounter get = new LongCounter();
    private final LongCounter replace = new LongCounter();
    private final LongCounter remove = new LongCounter();

    private final ThroughputMetric add_tp = new ThroughputMetric();
    private final ThroughputMetric get_tp = new ThroughputMetric();
    private final ThroughputMetric replace_tp = new ThroughputMetric();
    private final ThroughputMetric remove_tp = new ThroughputMetric();

    public BlobStoreOperationsWrapper(CacheManager cacheManager, BlobStoreStorageHandler blobStore) {
        _cacheManager = cacheManager;
        _blobStore = blobStore;
        _serialization = new BlobStoreSerializationUtils(cacheManager);
//		_needSerialization = !(blobStore instanceof BlobStoreNoSerializationHashMock);
        _needSerialization = true;
        _isOffHeap = blobStore instanceof OffHeapStorageContainer;
        _offHeapStore = _isOffHeap ? ((OffHeapStorageContainer)blobStore).getOffHeapStorage() : null;

        //TODO: some properties are only set after initialize , here we ONLY use 'off-heap-cache-memory-threshold' property
        Properties p = _blobStore.getProperties();
        String offHeapThreshold = p != null ? p.getProperty("off-heap-cache-memory-threshold") : null;
        _offHeapCache = offHeapThreshold == null ? null :new OffHeapMemoryPool(StringUtils.parseStringAsBytes(offHeapThreshold));
        //Validate _offHeapStore and _offHeapCache are mutually exclusive
        if(_offHeapCache != null && _offHeapStore != null){
            throw new RuntimeException("Configuration exception: can not enable off heap optimization when running with off-heap-blob-store configuration");
        }
        if(_offHeapStore != null && _cacheManager.isPersistentBlobStore()){
            throw new RuntimeException("Configuration exception: can not set "+ Constants.CacheManager.FULL_CACHE_MANAGER_BLOBSTORE_PERSISTENT_PROP+" to true when running with off-heap-blob-store configuration");
        }
    }

    @Override
    public OffHeapMemoryPool getOffHeapCache() {
        return _offHeapCache;
    }

    @Override
    public OffHeapMemoryPool getOffHeapStore() {
        return _offHeapStore;
    }

    @Override
    public BlobStoreStorageStatistics getStorageStatistics() {
        return _blobStore.getStatistics();
    }

    @Override
    public void initialize(BlobStoreConfig blobStoreConfig) {
        _registrator = blobStoreConfig.getMetricRegistrator();
        _blobStore.initialize(blobStoreConfig);
        registerOperations();
    }

    @Override
    public Properties getProperties() {
        return _blobStore.getProperties();
    }

    @Override
    public Object add(java.io.Serializable id, java.io.Serializable data, BlobStoreObjectType objectType, IBlobStoreOffHeapInfo offHeapInfo) {
        if (_isOffHeap && objectType != BlobStoreObjectType.DATA) {
            return null;
        }

        if (objectType.equals(BlobStoreObjectType.DATA)) {
            add.inc();
            add_tp.increment();
        }
        if (_needSerialization) {
            byte[] sdata = _serialization.serialize(data, objectType);
            return _isOffHeap ? _blobStore.add(offHeapInfo, sdata, objectType) : _blobStore.add(id, sdata, objectType);
        } else
            return _isOffHeap ? _blobStore.add(offHeapInfo, data, objectType) : _blobStore.add(id, data, objectType);
    }

    @Override
    public java.io.Serializable get(java.io.Serializable id, Object position, BlobStoreObjectType objectType, IBlobStoreOffHeapInfo offHeapInfo) {
        if (_isOffHeap && objectType != BlobStoreObjectType.DATA) {
            return null;
        }

        java.io.Serializable data = _isOffHeap ? _blobStore.get(offHeapInfo, position, objectType) : _blobStore.get(id, position, objectType);

        if (objectType.equals(BlobStoreObjectType.DATA)) {
            get.inc();
            get_tp.increment();
        }
        return (data != null && _needSerialization) ? _serialization.deserialize(data, objectType, false, false, offHeapInfo) : data;
    }


    @Override
    public java.io.Serializable get(java.io.Serializable id, Object
            position, BlobStoreObjectType objectType, boolean indexesPartOnly, IBlobStoreOffHeapInfo offHeapInfo) {
        if (_isOffHeap && objectType != BlobStoreObjectType.DATA) {
            return null;
        }
        java.io.Serializable data = _isOffHeap ? _blobStore.get(offHeapInfo, position, objectType) : _blobStore.get(id, position, objectType);

        if (objectType.equals(BlobStoreObjectType.DATA)) {
            get.inc();
            get_tp.increment();
        }
        return (data != null && _needSerialization) ? _serialization.deserialize(data, objectType, false, indexesPartOnly, offHeapInfo) : data;

    }

    @Override
    public Object replace(java.io.Serializable id, java.io.Serializable
            data, Object position, BlobStoreObjectType objectType, IBlobStoreOffHeapInfo offHeapInfo) {

        if (_isOffHeap && objectType != BlobStoreObjectType.DATA) {
            return null;
        }

        if (objectType.equals(BlobStoreObjectType.DATA)) {
            replace.inc();
            replace_tp.increment();
        }

        if (_needSerialization) {
            byte[] sdata = _serialization.serialize(data, objectType);
            return _isOffHeap ? _blobStore.replace(offHeapInfo, sdata, position, objectType) : _blobStore.replace(id, sdata, position, objectType);
        } else
            return _isOffHeap ? _blobStore.replace(offHeapInfo, data, position, objectType) : _blobStore.replace(id, data, position, objectType);
    }

    @Override
    public java.io.Serializable remove(java.io.Serializable id, Object
            position, BlobStoreObjectType objectType, IBlobStoreOffHeapInfo offHeapInfo) {
        if (_isOffHeap && objectType != BlobStoreObjectType.DATA) {
            return null;
        }
        //NOTE exception is thrown from underlying driver if remove fails
        java.io.Serializable data = _isOffHeap ? _blobStore.remove(offHeapInfo, position, objectType) : _blobStore.remove(id, position, objectType);
        if (objectType.equals(BlobStoreObjectType.DATA)) {
            remove.inc();
            remove_tp.increment();
        }

        return (data != null && _needSerialization) ? _serialization.deserialize(data, objectType, false, false, offHeapInfo) : data;
    }

    @Override
    public void removeIfExists(java.io.Serializable id, Object position, BlobStoreObjectType objectType, IBlobStoreOffHeapInfo offHeapInfo) {
        if (_isOffHeap && objectType != BlobStoreObjectType.DATA) {
            return;
        }
        //NOTE execption is thrown from underlying driver if remove fails
        java.io.Serializable data = _isOffHeap ? _blobStore.remove(offHeapInfo, position, objectType) : _blobStore.remove(id, position, objectType);
        if (objectType.equals(BlobStoreObjectType.DATA)) {
            remove.inc();
            remove_tp.increment();
        }
    }


    @Override
    public List<BlobStoreBulkOperationResult> executeBulk
            (List<BlobStoreBulkOperationRequest> operations, BlobStoreObjectType objectType, boolean transactional) {
        boolean isDataType = (objectType.equals(BlobStoreObjectType.DATA)) ? true : false;
        for (BlobStoreBulkOperationRequest request : operations) {
            if (isDataType)
                metricsByOpType(request);
            if (request.getData() != null && _needSerialization)
                request.setData(_serialization.serialize(request.getData(), objectType));
        }
        List<BlobStoreBulkOperationResult> results = _blobStore.executeBulk(operations, objectType, transactional);
        for (BlobStoreBulkOperationResult result : results) {
            if (result.getData() != null && _needSerialization)
                result.setData(_serialization.deserialize(result.getData(), objectType, false, false, result.getOffHeapInfo()));
        }

        return results;
    }

    @Override
    public DataIterator<BlobStoreGetBulkOperationResult> iterator(BlobStoreObjectType objectType) {
        return new IteratorWrapper(_cacheManager, objectType, _serialization, _needSerialization, _blobStore.iterator(objectType), false);
    }

    @Override
    public DataIterator<BlobStoreGetBulkOperationResult> initialLoadIterator() {
        return new IteratorWrapper(_cacheManager, BlobStoreObjectType.DATA, _serialization, _needSerialization, _blobStore.iterator(BlobStoreObjectType.DATA), true);
    }

    @Override
    public void close() {
        _blobStore.close();
        _registrator.clear();
        if (_preFetchThreadPool != null)
            _preFetchThreadPool.shutdown();
    }

    @Override
    public ExecutorService getPreFetchPool() {
        if (_preFetchThreadPool == null) {
            synchronized (this) {
                if (_preFetchThreadPool == null)
                    _preFetchThreadPool = DynamicExecutors.newScalingThreadPool(_blobStorePreFetchMinThreads, _blobStorePreFetchMaxThreads, 15000);
            }
        }
        return _preFetchThreadPool;

    }


    private static class IteratorWrapper implements DataIterator<BlobStoreGetBulkOperationResult> {
        private final DataIterator<BlobStoreGetBulkOperationResult> _iter;
        private boolean _finished;
        private final BlobStoreSerializationUtils _serialization;
        private final boolean _needSerialization;
        private final BlobStoreObjectType _objectType;
        private final CacheManager _cacheManager;
        private final boolean _fromInitialLoad;

        IteratorWrapper(CacheManager cacheManager, BlobStoreObjectType objectType,
                        BlobStoreSerializationUtils serialization, boolean needSerialization,
                        DataIterator<BlobStoreGetBulkOperationResult> iter, boolean fromInitialLoad) {
            _serialization = serialization;
            _iter = iter;
            _needSerialization = needSerialization;
            _objectType = objectType;
            _cacheManager = cacheManager;
            _fromInitialLoad = fromInitialLoad;
        }

        @Override
        public void close() {
            if (_iter != null)
                _iter.close();
        }

        @Override
        public boolean hasNext() {
            if (_finished || _iter == null)
                return false;
            if (!_iter.hasNext())
                _finished = true;
            return !_finished;
        }

        @Override
        public BlobStoreGetBulkOperationResult next() {
            if (_iter == null)
                return null;
            BlobStoreGetBulkOperationResult res = _iter.next();
            if (res == null) {
                _finished = true;
                return null;
            }

            if (res.getData() != null && _needSerialization) {
                res.setData(_serialization.deserialize(res.getData(), _objectType, _fromInitialLoad, _fromInitialLoad, res.getOffHeapInfo()));
            }
            return res;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private void registerOperations() {
        _registrator.register("add", add);
        _registrator.register("get", get);
        _registrator.register("remove", remove);
        _registrator.register("replace", replace);

        _registrator.register("add-tp", add_tp);
        _registrator.register("get-tp", get_tp);
        _registrator.register("remove-tp", remove_tp);
        _registrator.register("replace-tp", replace_tp);
        if (_offHeapCache != null)
            _offHeapCache.initMetrics(_registrator);
        if (_offHeapStore != null)
            _offHeapStore.initMetrics(_registrator);
    }

    private void metricsByOpType(BlobStoreBulkOperationRequest request) {
        switch (request.getOpType()) {
            case ADD:
                add.inc();
                add_tp.increment();
                break;
            case GET:
                get.inc();
                get_tp.increment();
                break;
            case REMOVE:
                remove.inc();
                remove_tp.increment();
                break;
            case REPLACE:
                replace.inc();
                replace_tp.increment();
                break;
        }
    }
}
