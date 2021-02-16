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

package com.j_spaces.core.cache;

import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_ALL_IN_CACHE;
import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_BLOB_STORE;
import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_PLUGGED_EVICTION;


abstract public class AbstractCacheManager {

    /**
     * the current cache policy.
     */
    private int _cachePolicy;

    /**
     * in case of eviction policy we evict unused entries in order to ensure space.
     */
    protected int _evictionQuota;

    protected int m_CacheSize;


    /**
     * Evict a quota of entries from cache.
     *
     * @return number of actual evicted
     */
    abstract public int evictBatch(int evictionQuota);

    public void setEvictionQuota(int evictionQuota) {
        _evictionQuota = evictionQuota;
    }

    public int getCachePolicy() {
        return _cachePolicy;
    }

    public void setCachePolicy(int cachePolicy) {
        this._cachePolicy = cachePolicy;
    }

    public boolean isResidentEntriesCachePolicy() {
        return isAllInCachePolicy() || isBlobStoreCachePolicy();
    }

    public boolean isBlobStoreCachePolicy() {
        return getCachePolicy() == CACHE_POLICY_BLOB_STORE;
    }

    public boolean isEvictableCachePolicy() {
        return !isResidentEntriesCachePolicy();
    }

    public boolean isAllInCachePolicy() {
        return getCachePolicy() == CACHE_POLICY_ALL_IN_CACHE;
    }

    public abstract boolean isTieredStorage();

    public boolean isPluggedEvictionPolicy() {
        return getCachePolicy() == CACHE_POLICY_PLUGGED_EVICTION;
    }


    public int getMaxCacheSize() {
        return !isEvictableCachePolicy() ? Integer.MAX_VALUE : m_CacheSize;
    }
}
