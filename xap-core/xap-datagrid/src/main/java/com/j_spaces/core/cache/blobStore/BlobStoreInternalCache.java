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

import com.gigaspaces.internal.utils.concurrent.UncheckedAtomicIntegerFieldUpdater;
import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.metrics.MetricRegistrator;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.StoredListFactory;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.list.IScanListIterator;
import com.j_spaces.kernel.list.ScanSingleListIterator;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
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
public class BlobStoreInternalCache implements IBlobStoreInternalCache {
    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE);

    private final int OFF_HEAP_INTERNAL_CACHE_CAPACITY;

    private final AtomicInteger _cacheSize;

    /**
     * <K:UID, V:PEntry>
     */
    private final ConcurrentMap<BlobStoreRefEntryCacheInfo, CacheInfoHolder> _entries;
    //segmented- each has LRU
    private final IStoredList<CacheInfoHolder> _quasiLru;

    private final MetricRegistrator _blobstoreMetricRegistrar;

    private final LongCounter _hit = new LongCounter();
    private final LongCounter _miss = new LongCounter();


    private BlobStoreInternalCacheInitialLoadFilter _blobStoreInternalCacheInitialLoadFilter;

    public BlobStoreInternalCache(Properties properties) {
        OFF_HEAP_INTERNAL_CACHE_CAPACITY = Integer.parseInt(properties.getProperty(FULL_CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_PROP, CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_DELAULT));
        int numOfCHMSegents = Integer.getInteger(SystemProperties.CACHE_MANAGER_HASHMAP_SEGMENTS, SystemProperties.CACHE_MANAGER_HASHMAP_SEGMENTS_DEFAULT);
        _entries = new ConcurrentHashMap<BlobStoreRefEntryCacheInfo, CacheInfoHolder>(OFF_HEAP_INTERNAL_CACHE_CAPACITY, 0.75f, numOfCHMSegents);
        _cacheSize = new AtomicInteger();
        _quasiLru = StoredListFactory.createConcurrentList(true /*segmented*/, true /*supportFifoPerSegment*/);
        if (_logger.isLoggable(Level.INFO)) {
            _logger.info("BlobStore space data internal cache size=" + OFF_HEAP_INTERNAL_CACHE_CAPACITY);
        }

        _blobstoreMetricRegistrar = (MetricRegistrator) properties.get("blobstoreMetricRegistrar");
        registerOperations();
    }

    public void setBlobStoreInternalCacheInitialLoadFilter(BlobStoreInternalCacheInitialLoadFilter blobStoreInternalCacheInitialLoadFilter) {
        _blobStoreInternalCacheInitialLoadFilter = blobStoreInternalCacheInitialLoadFilter;
    }

    @Override
    public BlobStoreEntryHolder get(BlobStoreRefEntryCacheInfo entryCacheInfo) {
        // TODO Auto-generated method stub
        if (isDisabledCache())
            return null;
        CacheInfoHolder cih = _entries.get(entryCacheInfo);

        if (cih != null) {
            _hit.inc();
            return cih.getEntry();
        } else {
            _miss.inc();
            return null;
        }
    }

    @Override
    public void store(BlobStoreEntryHolder entry) {
        // TODO Auto-generated method stub
        //store or touch if alread stored
        if (isDisabledCache())
            return;
        if (entry.isDeleted())
            return;  //entry deleted
        if(entry.isOptimizedEntry())
            return; // dont insert optimized entry we may need full one and optimized will be memory based

        CacheInfoHolder cih = _entries.get(entry.getBlobStoreResidentPart());
        if (cih == null)
            insert(entry);
        else
            touch(cih, entry);
    }

    private void insert(BlobStoreEntryHolder entry) {
        CacheInfoHolder cih = new CacheInfoHolder(entry);
        if (_entries.putIfAbsent(entry.getBlobStoreResidentPart(), cih) != null)
            return;  //already came in

        cih.setPos(_quasiLru.add(cih));
        cih.resetPending();
        if (cih.isDeleted() && remove(cih, false /*decrement size*/))
            return;

        //increment size
        if (_cacheSize.incrementAndGet() > OFF_HEAP_INTERNAL_CACHE_CAPACITY)
            evict(cih);

        if (entry.isDeleted() && !cih.isDeleted()) //can happen in NBR
            remove(entry);
        return;
    }


    private boolean remove(CacheInfoHolder cih, boolean decrementSize) {

        if (!cih.setRemoved())
            return false;

        if (decrementSize)
            _cacheSize.decrementAndGet();

        _quasiLru.remove(cih.getPos());
        _entries.remove(cih.getEntry().getBlobStoreResidentPart(), cih);

        return true;
    }


    private void touch(CacheInfoHolder cih, BlobStoreEntryHolder entry) {//touch this entry
        if (cih.getEntry().getBlobStoreVersion() < entry.getBlobStoreVersion())
            cih.setEntry(entry);
        if (_cacheSize.get() <= OFF_HEAP_INTERNAL_CACHE_CAPACITY / 10)
            return; //no use, empty practically

        if (!cih.setPending())
            return;   //busy deleting or touching

        _quasiLru.remove(cih.getPos());
        cih.setPos(_quasiLru.add(cih));
        cih.resetPending();
        if (cih.isDeleted())
            remove(cih, false /*dont decrement size*/);
    }

    @Override
    public void remove(BlobStoreEntryHolder entry) {
        // TODO Auto-generated method stub
        if (isDisabledCache())
            return;

        CacheInfoHolder cih = _entries.get(entry.getBlobStoreResidentPart());

        if (cih != null && !cih.isRemoved()) {
            cih.setDeleted();
            remove(cih, true/*decrementSize*/);
        }
    }

    private void evict(CacheInfoHolder cih) {
        try {
            IScanListIterator<CacheInfoHolder> toScan = new ScanSingleListIterator<CacheInfoHolder>(_quasiLru, true /*fifoPerSegment*/);
            try {
                while (toScan != null && toScan.hasNext()) {
                    CacheInfoHolder cur_cih = toScan.next();
                    if (cur_cih != null && cur_cih != cih && !cur_cih.isDeleted() && !cur_cih.isRemoved()) {
                        cur_cih.setDeleted();
                        remove(cur_cih, true /*decrementSize*/);
                        return;
                    }
                }
            } finally {
                if (toScan != null)
                    toScan.releaseScan();
            }
        } catch (SAException ex) {
        } //cant happen- its only cache
    }

    private boolean isDisabledCache() {
        return OFF_HEAP_INTERNAL_CACHE_CAPACITY == 0;
    }

    private void registerOperations() {
        _blobstoreMetricRegistrar.register("cache-miss", _miss);
        _blobstoreMetricRegistrar.register("cache-hit", _hit);
    }

    public boolean isFull(){
        return _cacheSize.get() >= OFF_HEAP_INTERNAL_CACHE_CAPACITY;
    }

    @Override
    public int size() {
        return _cacheSize.get();
    }

    @Override
    public BlobStoreInternalCacheInitialLoadFilter getBlobStoreInternalCacheInitialLoadFilter() {
        return _blobStoreInternalCacheInitialLoadFilter;
    }

    @Override
    public LongCounter getHitCount() {
        return _hit;
    }

    @Override
    public LongCounter getMissCount() {
        return _miss;
    }


    private static class CacheInfoHolder {
        private volatile BlobStoreEntryHolder _entry;
        private volatile IObjectInfo<CacheInfoHolder> _pos;

        static final int _STATUS_PENDING = 1 << 0;  //ongoing insert or touch
        static final int _STATUS_UNPENDING = ~_STATUS_PENDING;  //
        static final int _STATUS_DELETED = 1 << 1;  //logical deletion
        static final int _STATUS_REMOVED = 1 << 2;  //physical remove

        private volatile int _status;

        private static final AtomicIntegerFieldUpdater<CacheInfoHolder> cacheStatusUpdater = UncheckedAtomicIntegerFieldUpdater.newUpdater(CacheInfoHolder.class, "_status");


        CacheInfoHolder(BlobStoreEntryHolder entry) {
            _status = _STATUS_PENDING;
            _entry = entry;
        }

        boolean isPending() {
            return (_status & _STATUS_PENDING) == _STATUS_PENDING;
        }

        boolean setPending() {
            return (cacheStatusUpdater.compareAndSet(this, 0, _STATUS_PENDING));
        }

        void resetPending() {
            while (true) {
                int stat = _status;
                int newStat = stat & _STATUS_UNPENDING;
                if (cacheStatusUpdater.compareAndSet(this, stat, newStat))
                    return;
            }
        }

        boolean isDeleted() {
            return (_status & _STATUS_DELETED) == _STATUS_DELETED;
        }

        void setDeleted() {
            while (true) {
                int stat = _status;
                int newStat = stat | _STATUS_DELETED;
                if (cacheStatusUpdater.compareAndSet(this, stat, newStat))
                    return;
            }
        }

        boolean isRemoved() {
            return (_status & _STATUS_REMOVED) == _STATUS_REMOVED;
        }

        boolean setRemoved() {
            return (cacheStatusUpdater.compareAndSet(this, _STATUS_DELETED, _STATUS_REMOVED));
        }

        BlobStoreEntryHolder getEntry() {
            return _entry;
        }

        void setEntry(BlobStoreEntryHolder entry) {
            _entry = entry;
        }

        IObjectInfo<CacheInfoHolder> getPos() {
            return _pos;
        }

        void setPos(IObjectInfo<CacheInfoHolder> pos) {
            _pos = pos;
        }
    }

}
