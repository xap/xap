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

import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.list.IScanListIterator;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yechielf on 01/02/2016.
 */
/*

   updates recently in extended index. enables traversing via iterator and not missing updates within range
 */
@com.gigaspaces.api.InternalApi
public class RecentExtendedIndexUpdates {

    private static final long RECENT_EXTENDED_UPDATE_TIME = 10 * 1000;

    private final CacheManager _cacheManager;
    private final ConcurrentMap<String, RecentExtendedUpdatesInfo> _updates;
    private final AtomicInteger _size;

    public RecentExtendedIndexUpdates(CacheManager cacheManager) {
        _cacheManager = cacheManager;
        _updates = new ConcurrentHashMap<String, RecentExtendedUpdatesInfo>();
        _size = new AtomicInteger();
    }

    public void onUpdate(IEntryCacheInfo eci) {
        if (!_updates.containsKey(eci.getUID()))
            _size.incrementAndGet();
        _updates.put(eci.getUID(), new RecentExtendedUpdatesInfo(eci.getUID()));
    }

    public void onUpdateEnd(IEntryCacheInfo eci) {
        RecentExtendedUpdatesInfo r = _updates.get(eci.getUID());
        if (r != null)
            r.setAfterUpdate();
    }


    public void onRemove(IEntryCacheInfo eci) {
        Object res = _updates.remove(eci.getUID());
        if (res != null)
            _size.decrementAndGet();
    }

    public int reapExpired() {
        if (isEmpty())
            return 0;
        int reaped = 0;
        long curtime = System.currentTimeMillis();
        Iterator<RecentExtendedUpdatesInfo> iter = _updates.values().iterator();
        while (iter.hasNext()) {
            RecentExtendedUpdatesInfo r = iter.next();
            long limit = r.getTime();
            if (limit + RECENT_EXTENDED_UPDATE_TIME < curtime) {
                Object res = _updates.remove(r.getUid(), r);
                if (res != null) {
                    _size.decrementAndGet();
                    reaped++;
                }
            }
        }
        return reaped;
    }

    public IScanListIterator<IEntryCacheInfo> iterator(long startTime, ExtendedIndexIterator mainIter) {
        if (isEmpty())
            return null;
        return new RecentExtendedUpdatesIter(_cacheManager, _updates, startTime, mainIter);
    }

    public boolean isEmpty() {
        return size() <= 0;
    }

    int size() {
        return _size.get();
    }

    public static class RecentExtendedUpdatesInfo {
        private final String _uid;
        private volatile long _time;
        private volatile boolean _afterUpdate;

        RecentExtendedUpdatesInfo(String uid) {
            _uid = uid;
            _time = System.currentTimeMillis();
        }

        public String getUid() {
            return _uid;
        }

        public long getTime() {
            return _time;
        }

        public boolean isAfterUpdate() {
            return _afterUpdate;
        }

        public void setAfterUpdate() {
            _time = System.currentTimeMillis();
            _afterUpdate = true;
        }
    }

    public static class RecentExtendedUpdatesIter implements IScanListIterator<IEntryCacheInfo> {
        private final CacheManager _cacheManager;
        private final long _start;
        private Iterator<RecentExtendedUpdatesInfo> _iter;
        private IEntryCacheInfo _cur;
        private final long _startTime;
        private final ExtendedIndexIterator _mainIter;

        RecentExtendedUpdatesIter(CacheManager cacheManager, ConcurrentMap<String, RecentExtendedUpdatesInfo> updates, long startTime, ExtendedIndexIterator mainIter) {
            _cacheManager = cacheManager;
            _start = System.currentTimeMillis();
            _iter = updates.values().iterator();
            _startTime = startTime;
            _mainIter = mainIter;
        }

        public boolean hasNext() throws SAException {
            _cur = null;
            if (_iter == null)
                return false;
            while (_iter.hasNext()) {
                RecentExtendedUpdatesInfo r = _iter.next();
                if (r.isAfterUpdate() && _startTime > r.getTime())
                    continue;
                _cur = _cacheManager.getPEntryByUid(r.getUid());
                if (_cur != null)
                    return true;
            }
            _iter = null;
            return false;
        }

        public IEntryCacheInfo next() throws SAException {
            IEntryCacheInfo cur = _cur;
            _cur = null;
            return cur;
        }

        /**
         * release SLHolder for this scan
         */
        public void releaseScan() throws SAException {

        }

        /**
         * if the scan is on a property index, currently supported for extended index
         */
        public int getAlreadyMatchedFixedPropertyIndexPos() {
            return -1;
        }

        /**
         * is the entry returned already matched against the searching template currently is true if
         * the underlying scan made by CacheManager::EntriesIter
         */
        public boolean isAlreadyMatched() {
            return false;
        }

        public boolean isIterator() {
            return true;
        }

        @Override
        public int hashCode()
        {
            return _mainIter.hashCode();

        }

        @Override
        public boolean equals(Object o)
        {
            if (o == this)
                return true;
            if (!(o instanceof RecentExtendedUpdatesIter))
                return false;
            return
                    _mainIter.equals(((RecentExtendedUpdatesIter)o)._mainIter);
        }
    }
}
