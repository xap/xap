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

package com.gigaspaces.internal.server.space.eviction;

import com.gigaspaces.server.eviction.EvictableServerEntry;
import com.gigaspaces.server.eviction.SpaceEvictionStrategy;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * note- concurrent LRU is not an  "accurate" eviction policy in the sense that the number of
 * objects stored can be lower than the limit defined but new objects will be rejected. it is mostly
 * suitable  for large LRU sizes & large # of threads
 *
 * @author Yechiel Feffer
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class ConcurrentLruSpaceEvictionStrategy extends SpaceEvictionStrategy {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE);
    private static final int TOUCH_UNSAFE_MARGIN = 500;
    private static final int MIN_TOUCH_THRESHOLD = 0;
    private static final int MAX_TOUCH_THRESHOLD = 100;

    private final int _touchThreshold;
    private final IEvictionChain _chain;
    private final AtomicInteger _estimatedNumCachedEntries; //including pinned
    private int _estimatedNumCachedEntriesUnsafe; //non volatile- touch estimation
    private int _touchLimit;  //don't need a volatile here

    public ConcurrentLruSpaceEvictionStrategy(int touchThreshold, int maxCacheSize) {
        if (touchThreshold > MAX_TOUCH_THRESHOLD || touchThreshold < MIN_TOUCH_THRESHOLD)
            throw new IllegalArgumentException("Illegal LRU touch threshold " + touchThreshold + " - must be between " +
                    MIN_TOUCH_THRESHOLD + " and " + MAX_TOUCH_THRESHOLD + " (inclusive).");
        this._touchThreshold = touchThreshold;
        if (_touchThreshold > 0)
            _touchLimit = maxCacheSize == Integer.MAX_VALUE ? Integer.MAX_VALUE : (maxCacheSize * _touchThreshold) / 100;
        this._chain = new ChainsSegments(false /*unsafe*/, touchThreshold);
        this._estimatedNumCachedEntries = new AtomicInteger();

        if (_logger.isDebugEnabled())
            _logger.debug(this.getClass().getSimpleName() + " started: [" +
                    "max-cache-size= " + maxCacheSize +
                    ", touch-threshold= " + _touchThreshold +
                    ", touch-limit=" + _touchLimit +
                    "]");
    }

    /**
     * Determines whether this eviction strategy implementation requires concurrency protection from
     * the space.
     */
    @Override
    public boolean requiresConcurrencyProtection() {
        return false;
    }


    @Override
    public void onInsert(EvictableServerEntry entry) {
        introduce(entry);
    }

    @Override
    public void onLoad(EvictableServerEntry entry) {
        introduce(entry);
    }

    @Override
    public void onRead(EvictableServerEntry entry) {
        touch(entry);
    }

    @Override
    public void onUpdate(EvictableServerEntry entry) {
        touch(entry);
    }

    @Override
    public void onRemove(EvictableServerEntry entry) {
        if (!_chain.remove(entry))
            throw new IllegalStateException("The removed entry is not registered in the eviction strategy - [type=" + entry.getSpaceTypeDescriptor().getTypeName() + ", uid=" + entry.getUID() + "]");

        _estimatedNumCachedEntriesUnsafe = _estimatedNumCachedEntries.decrementAndGet();
    }

    @Override
    public int evict(int numOfEntries) {
        int curSize = _estimatedNumCachedEntries.get();
        int numToEvict = Math.min(numOfEntries, curSize);

        //do we have to set a new touch timit ?
        if (_touchThreshold != MAX_TOUCH_THRESHOLD && _touchThreshold != MIN_TOUCH_THRESHOLD) {
            int newMax = curSize - numToEvict;
            int newLimit = (newMax * _touchThreshold) / 100;
            if (_touchLimit > newLimit)
                _touchLimit = newLimit; //no volatile here
        }
        return numToEvict > 0 ? evictEntriesFromCache(numToEvict) : 0;
    }

    private void introduce(EvictableServerEntry entry) {
        // Note: the estimated is intentionally incremented before the entry is added to avoid actual# > limit.
        _estimatedNumCachedEntriesUnsafe = _estimatedNumCachedEntries.incrementAndGet();
        _chain.insert(entry);
    }

    private void touch(EvictableServerEntry entry) {
        if (_touchThreshold == MAX_TOUCH_THRESHOLD)
            return; //touch always skipped

        if (_touchThreshold != MIN_TOUCH_THRESHOLD) {
            //can we spare touching atomic int?
            if (_touchLimit - _estimatedNumCachedEntriesUnsafe >= TOUCH_UNSAFE_MARGIN)
                return;

            int curSize = _estimatedNumCachedEntries.get();
            if (curSize < _touchLimit)
                return;
        }
        _chain.touch(entry);
    }

    private int evictEntriesFromCache(int numToEvict) {
        int evicted = 0;
        Iterator<EvictableServerEntry> iter = _chain.evictionCandidates();
        while (iter.hasNext()) {
            if (isClosed())
                return evicted;
            EvictableServerEntry entry = (EvictableServerEntry) iter.next();

            if (entry == null)
                continue;

            if (!_chain.isEvictable(entry))
                continue; //deleted or pinned

            //try this one
            if (getEvictionManager().tryEvict(entry))
                evicted++;

            if (evicted == numToEvict)
                break;
        }
        return evicted;
    }
}
