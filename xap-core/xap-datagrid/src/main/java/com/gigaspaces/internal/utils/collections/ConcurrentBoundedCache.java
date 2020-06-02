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

package com.gigaspaces.internal.utils.collections;

import com.j_spaces.jdbc.Query;
import com.j_spaces.jdbc.QueryCache;
import com.j_spaces.kernel.SystemProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.SoftReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Concurrent bounded cache. It uses a ConcurrentHashMap and a counter to set the upper bound to the
 * cache size. If one of the threads reaches the upperBound, the cache is cleared.
 *
 * @author Boris
 * @version 1.0
 * @since 10.1.0
 */
@com.gigaspaces.api.InternalApi
public class ConcurrentBoundedCache extends QueryCache {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    // Concurrent map to store the values
    private final ConcurrentHashMap<String, SoftReference<Query>> _map;
    private final AtomicInteger approximateSize = new AtomicInteger(0);
    private final Object clearLock = new Object();
    private final long upperBound;
    private final boolean warnWhenFull;

    public ConcurrentBoundedCache(long upperBound, boolean warnWhenFull) {
        this.upperBound = upperBound;
        this.warnWhenFull = warnWhenFull;
        this._map = new ConcurrentHashMap<>();
    }

    @Override
    public void addQueryToCache(String statement, Query query) {
        if (approximateSize.getAndIncrement() >= upperBound) {
            synchronized (clearLock) {
                if (approximateSize.get() >= upperBound) {
                    if (warnWhenFull) {
                        logger.warn("Cache is being evicted because it exceeded max capacity {}. Consider increasing cache size with the {} system property, or using an unbound cache with the {} system property",
                                upperBound, SystemProperties.BOUNDED_QUERY_CACHE_SIZE, SystemProperties.ENABLE_BOUNDED_QUERY_CACHE);
                    }
                    clear();
                    approximateSize.set(1);
                }
            }
        }
        _map.putIfAbsent(statement, new SoftReference<>(query));
    }

    @Override
    public Query getQueryFromCache(String statement) {
        SoftReference<Query> result = _map.get(statement);
        return result != null ? result.get() : null;
    }

    @Override
    public void clear() {
        _map.clear();
        approximateSize.set(0);
    }
}
