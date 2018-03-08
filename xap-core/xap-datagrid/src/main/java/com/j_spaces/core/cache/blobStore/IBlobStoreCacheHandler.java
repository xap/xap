package com.j_spaces.core.cache.blobStore;

import com.j_spaces.core.cache.CacheOperationReason;
import com.j_spaces.core.cache.context.Context;

/**
 * Created by alon on 11/8/17.
 */
public interface IBlobStoreCacheHandler {
    /*  get an entry if exists in cache*/
    BlobStoreEntryHolder get(BlobStoreRefEntryCacheInfo entryCacheInfo);

    /* perform cache operation on entry according to space operation*/
    void handleOnSpaceOperation(Context context, BlobStoreEntryHolder entry, CacheOperationReason cacheOperationReason);

    boolean isFull();

    long getCacheSize();

    BlobStoreInternalCacheFilter getBlobStoreInternalCacheFilter();

    void setBlobStoreInternalCacheFilter(BlobStoreInternalCacheFilter blobStoreInternalCacheFilter);

    long getMissCount();

    long getHitCount();

    long getHotDataCacheMiss();
}
