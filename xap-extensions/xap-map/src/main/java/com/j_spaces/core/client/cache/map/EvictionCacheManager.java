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

package com.j_spaces.core.client.cache.map;

import com.j_spaces.core.Constants.CacheManager;
import com.j_spaces.core.cache.AbstractCacheManager;
import com.j_spaces.javax.cache.Cache;
import com.j_spaces.javax.cache.EvictionStrategy;
import com.j_spaces.map.eviction.AbstractEvictionStrategy;

import java.lang.ref.WeakReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EvictionCacheManager extends AbstractCacheManager {
    private EvictionStrategy _evictionStrategy;
    private WeakReference<Cache> _cache;

    //logger
    final static private Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE);

    public EvictionCacheManager(EvictionStrategy evictionStrategy, Cache cache) {
        setCachePolicy(CacheManager.CACHE_POLICY_LRU);
        _evictionStrategy = evictionStrategy;
        _cache = new WeakReference<Cache>(cache);
    }

    @Override
    public int evictBatch(int evictionQuota) {

        final Cache cache = _cache.get();
        if (_logger.isDebugEnabled()) {
            _logger.trace("Cache size before eviction=" + cache.size());
        }

        int size = _evictionStrategy.evict(cache);

        if (_logger.isDebugEnabled()) {
            _logger.debug("Cache size after eviction=" + cache.size() + " Evicted=" + size);
        }

        return size;
    }

    @Override
    public void setEvictionQuota(int evictionQuota) {
        super.setEvictionQuota(evictionQuota);
        if (_evictionStrategy instanceof AbstractEvictionStrategy) {
            ((AbstractEvictionStrategy) _evictionStrategy).setBatchSize(evictionQuota);
        }
    }

    @Override
    public boolean isTieredStorage() {
        return false;
    }
}

