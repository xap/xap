/*
 * Copyright (c) 2008-2018, GigaSpaces Technologies, Inc. All Rights Reserved.
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
package com.gigaspaces.server.blobstore;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;

/**
 * @author Niv Ingberg
 * @since 12.3
 */
public class BlobStoreStatisticsImpl implements BlobStoreStatistics, SmartExternalizable {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    private long cacheSize;
    private long cacheHitCount;
    private long cacheMissCount;
    private long hotDataCacheMissCount;
    private long offHeapCacheUsedBytes;
    private Collection<BlobStoreStorageStatistics> storageStatistics;

    /**
     * Required for @{@link Externalizable}
     */
    public BlobStoreStatisticsImpl() {
    }

    @Override
    public long getCacheSize() {
        return cacheSize;
    }
    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }
    @Override
    public long getCacheHitCount() {
        return cacheHitCount;
    }
    public void setCacheHitCount(long hitCount) {
        this.cacheHitCount = hitCount;
    }

    @Override
    public long getCacheMissCount() {
        return cacheMissCount;
    }
    public void setCacheMissCount(long missCount) {
        this.cacheMissCount = missCount;
    }

    @Override
    public long getHotDataCacheMissCount() {
        return hotDataCacheMissCount;
    }
    public void setHotDataCacheMissCount(long hotDataMissCount) {
        this.hotDataCacheMissCount = hotDataMissCount;
    }

    @Override
    public long getOffHeapCacheUsedBytes() {
        return offHeapCacheUsedBytes;
    }
    public void setOffHeapCacheUsedBytes(long offHeapCacheUsedBytes) {
        this.offHeapCacheUsedBytes = offHeapCacheUsedBytes;
    }

    @Override
    public Collection<BlobStoreStorageStatistics> getStorageStatistics() {
        return storageStatistics;
    }
    public void setStorageStatistics(Collection<BlobStoreStorageStatistics> storageStatistics) {
        this.storageStatistics = storageStatistics;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(buildFlags());
        if (cacheSize != 0)
            out.writeLong(cacheSize);
        if (cacheHitCount != 0)
            out.writeLong(cacheHitCount);
        if (cacheMissCount != 0)
            out.writeLong(cacheMissCount);
        if (hotDataCacheMissCount != 0)
            out.writeLong(hotDataCacheMissCount);
        if (offHeapCacheUsedBytes != 0)
            out.writeLong(offHeapCacheUsedBytes);
        if (storageStatistics != null)
            IOUtils.writeObject(out, storageStatistics);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int flags = in.readInt();
        cacheSize = ((flags & FLAG_CACHE_SIZE) != 0) ? in.readLong() : 0;
        cacheHitCount = ((flags & FLAG_CACHE_HIT) != 0) ? in.readLong() : 0;
        cacheMissCount = ((flags & FLAG_CACHE_MISS) != 0) ? in.readLong() : 0;
        hotDataCacheMissCount = ((flags & FLAG_HOT_DATA_CACHE_MISS) != 0) ? in.readLong() : 0;
        offHeapCacheUsedBytes = ((flags & FLAG_OHC_USED_BYTES) != 0) ? in.readLong() : 0;
        if ((flags & FLAG_STORAGE_STATS) != 0)
            storageStatistics = IOUtils.readObject(in);
    }

    private static final short FLAG_CACHE_SIZE = 1 << 0;
    private static final short FLAG_CACHE_HIT = 1 << 1;
    private static final short FLAG_CACHE_MISS = 1 << 2;
    private static final short FLAG_HOT_DATA_CACHE_MISS = 1 << 3;
    private static final short FLAG_OHC_USED_BYTES = 1 << 4;
    private static final short FLAG_STORAGE_STATS = 1 << 5;

    private int buildFlags() {
        int flags = 0;

        if (cacheSize != 0)
            flags |= FLAG_CACHE_SIZE;
        if (cacheHitCount != 0)
            flags |= FLAG_CACHE_HIT;
        if (cacheMissCount != 0)
            flags |= FLAG_CACHE_MISS;
        if (hotDataCacheMissCount != 0)
            flags |= FLAG_HOT_DATA_CACHE_MISS;
        if (offHeapCacheUsedBytes != 0)
            flags |= FLAG_OHC_USED_BYTES;
        if (storageStatistics != null)
            flags |= FLAG_STORAGE_STATS;

        return flags;
    }
}
