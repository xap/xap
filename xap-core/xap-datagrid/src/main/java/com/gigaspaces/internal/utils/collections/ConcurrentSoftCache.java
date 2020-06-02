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

import java.lang.ref.SoftReference;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Soft reference concurrent cache. It uses a ConcurrentHashMap and a soft reference for the
 * values.
 *
 * An entry in a <tt>ConcurrentSoftCache</tt> will be removed on JVM memory shortage.
 *
 * A double-reference is used to store the values, so that the finalize() method can be used to
 * remove the entry from cache when it is collected by the garbage collector.
 *
 * <i>Note</i> The entry will be removed even if there is a hard reference to it somewhere, since
 * the double-reference is used.
 *
 * @author anna
 * @version 1.0
 * @since 5.1
 * @deprecated Since 10.1.0 - use ConcurrentBoundedSoftCache instead
 */
@Deprecated
@com.gigaspaces.api.InternalApi
public class ConcurrentSoftCache extends QueryCache {
    // Concurrent map to store the values with soft double reference
    protected final ConcurrentHashMap<String, SoftReference<DoubleRef<String, Query>>> _map = new ConcurrentHashMap<>();

    @Override
    public void addQueryToCache(String statement, Query query) {
        _map.put(statement, reference(statement, query));
    }

    @Override
    public Query getQueryFromCache(String statement) {
        return dereference(_map.get(statement));
    }

    @Override
    public void clear() {
        _map.clear();
    }

    /**
     * Create a soft double reference to the object
     */
    private <Key, Value> SoftReference<DoubleRef<Key, Value>> reference(Key key, Value value) {
        return new SoftReference<>(new DoubleRef<>(key, value));
    }

    /**
     * Extract the referenced value
     *
     * @return the value referenced by ref
     */
    private <Key, Value> Value dereference(SoftReference<DoubleRef<Key, Value>> ref) {
        if (ref == null)
            return null;

        DoubleRef<Key, Value> doubleRef = ref.get();
        return doubleRef == null ? null : doubleRef.getValue();
    }

    /**
     * DoubleRef is a wrapper class for the real value. Double referencing allows to clean the
     * object from cache when its garbage collected.
     *
     * @author anna
     * @version 1.0
     * @since 5.1
     */
    private final class DoubleRef<K, V> {
        private final K _key;
        private final V _value;

        DoubleRef(K key, V value) {
            _key = key;
            _value = value;
        }

        public K getKey() {
            return _key;
        }

        public V getValue() {
            return _value;
        }

        /**
         * Remove any reference to this object from the cache
         */
        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            // when deleting - make sure that the correct reference is deleted,
            // since there might exist several SoftReferences on the same key
            // in case of multi threaded access
            _map.remove(_key, _value);
        }
    }
}