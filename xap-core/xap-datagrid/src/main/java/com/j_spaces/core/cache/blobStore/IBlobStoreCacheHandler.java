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
