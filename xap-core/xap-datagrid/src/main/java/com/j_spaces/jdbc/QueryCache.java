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

package com.j_spaces.jdbc;

import com.gigaspaces.internal.utils.collections.ConcurrentBoundedCache;
import com.gigaspaces.internal.utils.collections.ConcurrentSoftCache;
import com.j_spaces.kernel.SystemProperties;

import java.util.Map;


/**
 * Caches JDBC queries by their string representation
 *
 * @author anna
 * @since 6.1
 */
@com.gigaspaces.api.InternalApi
public abstract class QueryCache {

    public static QueryCache create() {
        String val = System.getProperty(SystemProperties.ENABLE_BOUNDED_QUERY_CACHE);
        boolean isCacheBounded = Boolean.parseBoolean(val != null ? val : SystemProperties.ENABLE_BOUNDED_QUERY_CACHE_DEFAULT);
        if (isCacheBounded) {
            long upperBound = Long.getLong(SystemProperties.BOUNDED_QUERY_CACHE_SIZE, SystemProperties.BOUNDED_QUERY_CACHE_SIZE_DEFAULT);
            boolean warnWhenFull = val == null; // If implicit config, warn when full to ensure user's aware of potential problem
            return upperBound != 0 ? new ConcurrentBoundedCache(upperBound, warnWhenFull) : new EmptyCache();
        } else {
            return new ConcurrentSoftCache();
        }
    }

    public abstract void addQueryToCache(String statement, Query query);

    public abstract Query getQueryFromCache(String statement);

    public abstract void clear();

    private static class EmptyCache extends QueryCache {

        @Override
        public void addQueryToCache(String statement, Query query) {
        }

        @Override
        public Query getQueryFromCache(String statement) {
            return null;
        }

        @Override
        public void clear() {
        }
    }
}