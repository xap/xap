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

package com.gigaspaces.internal.stubcache;

import com.gigaspaces.internal.classloader.ClassLoaderCache;
import com.gigaspaces.internal.classloader.IClassLoaderCacheStateListener;
import com.gigaspaces.internal.utils.collections.ConcurrentHashSet;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;
import com.gigaspaces.logger.Constants;
import com.j_spaces.kernel.ClassLoaderHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A global cache for dynamic smart stubs
 *
 * @author eitany
 * @since 7.5
 */
@com.gigaspaces.api.InternalApi
public class StubCache implements IClassLoaderCacheStateListener {
    private final static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_STUB_CACHE);

    private static final int CLEAN_RATE = Integer.getInteger("com.gs.transport_protocol.lrmi.stub-cache.clean-rate", 200000);
    private static final int CACHE_MAX_SIZE = Integer.getInteger("com.gs.transport_protocol.lrmi.stub-cache.size", 1024 * 10);
    //Not volatile because it is not that important to clean at the clean rate
    private int _iteration = 0;

    private final ConcurrentMap<StubId, TouchedItem<Object>> _cachedStubs = new ConcurrentHashMap<StubId, TouchedItem<Object>>();
    private final ConcurrentMap<Long, Set<StubId>> _classLoaderContext = new CopyOnUpdateMap<Long, Set<StubId>>();

    public StubCache() {
        ClassLoaderCache.getCache().registerCacheStateListener(this);
    }

    public Object getStub(StubId id) {
        if (++_iteration % CLEAN_RATE == 0)
            clearStaleEntries();

        TouchedItem<Object> touchedItem = _cachedStubs.get(id);
        if (touchedItem == null)
            return null;

        Object stub = touchedItem.getItem();

        return stub;
    }

    public synchronized void addStub(StubId id, Object stub) {
        if (_cachedStubs.size() >= CACHE_MAX_SIZE) {
            if (_logger.isDebugEnabled())
                _logger.debug("stub cache size reached, clearing the cache");

            _cachedStubs.clear();
            _classLoaderContext.clear();
        }

        TouchedItem<Object> previousStub = _cachedStubs.putIfAbsent(id, new TouchedItem<Object>(stub));
        //If null we insert first
        if (previousStub == null) {
            //Associate this stub to the context class loader so it will be removed when this class loader is removed
            Long contextClassLoaderKey = ClassLoaderCache.getCache().putClassLoader(ClassLoaderHelper.getContextClassLoader());
            Set<StubId> set = _classLoaderContext.get(contextClassLoaderKey);
            if (set == null) {
                set = new ConcurrentHashSet<StubId>();
                Set<StubId> prevSet = _classLoaderContext.putIfAbsent(contextClassLoaderKey, set);
                if (prevSet != null)
                    set = prevSet;
            }
            set.add(id);
        }
    }

    /**
     * Clear stale entries, an entry is considered stale if it was not touched in the last two clean
     * cycles
     */
    public synchronized void clearStaleEntries() {
        if (_logger.isDebugEnabled())
            _logger.debug("clearing stale entries from stub cache");

        List<StubId> staleEntries = new ArrayList<StubId>();
        for (Map.Entry<StubId, TouchedItem<Object>> entry : _cachedStubs.entrySet()) {
            //If not touched two cycles, remove it
            if (!entry.getValue().clearTouched())
                staleEntries.add(entry.getKey());
        }

        if (_logger.isDebugEnabled()) {
            int staleEntriesCount = staleEntries.size();
            if (staleEntriesCount > 0)
                _logger.debug("found " + staleEntriesCount + " stale entries in cache, removing them");
            else
                _logger.debug("no stale entries found in cache");
        }

        for (StubId id : staleEntries) {
            _cachedStubs.remove(id);
            for (Set<StubId> set : _classLoaderContext.values())
                set.remove(id);
        }


    }

    public synchronized void onClassLoaderRemoved(Long classLoaderKey, boolean explicit) {
        Set<StubId> associatedCachedParticipants = _classLoaderContext.remove(classLoaderKey);
        if (associatedCachedParticipants != null) {
            for (StubId id : associatedCachedParticipants)
                _cachedStubs.remove(id);
        }

    }

    public synchronized void clear() {
        _cachedStubs.clear();
        _classLoaderContext.clear();
    }

    /**
     * Holds an item and a touched state
     *
     * @author eitany
     * @since 7.1
     */
    private static class TouchedItem<K> {
        private boolean _touched;

        private final K _item;

        public TouchedItem(K item) {
            _item = item;
            _touched = true;
        }

        public K getItem() {
            _touched = true;
            return _item;
        }

        /**
         * clear touched state and return previous touch state
         */
        public boolean clearTouched() {
            boolean result = _touched;
            _touched = false;
            return result;
        }
    }
}
