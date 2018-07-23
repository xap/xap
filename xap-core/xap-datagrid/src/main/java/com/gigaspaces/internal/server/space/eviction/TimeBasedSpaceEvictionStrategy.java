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

import com.gigaspaces.internal.backport.java.util.concurrent.FastConcurrentSkipListMap;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.server.eviction.EvictableServerEntry;
import com.gigaspaces.server.eviction.SpaceEvictionManager;
import com.gigaspaces.server.eviction.SpaceEvictionStrategy;
import com.gigaspaces.server.eviction.SpaceEvictionStrategyConfig;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.IStoredListIterator;
import com.j_spaces.kernel.StoredListFactory;

import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * abstract class- infra' for time-based eviction strategy
 *
 * @author Yechiel Feffer
 * @since 9.1
 */
public abstract class TimeBasedSpaceEvictionStrategy extends SpaceEvictionStrategy {
    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE);
    private final static long TIMEBASED_EVICTION_MIN_FORCE_EXPIRATION_INTERVAL = 500;
    private final static long SHORT_LIVED_ENTRY_INTERVAL_LIMIT = 1;
    private final static long CELLS_GRANULARITY_INTERVAL = 10 * 1000;

    //how long does a new entry stay in cache before harvest
    private final long _inCacheTimeInterval;
    //how long does a reinserted entry stay in cache before harvest
    private final long _inCacheReinsertedTimeInterval;
    // every such period we scan for entries to evict
    private final long _harvestMainInterval;
    //for short-lived entries interval
    private final long _harvestShortLivedInterval;

    //key = expiration due time //val = Cell of entries to reap
    private final FastConcurrentSkipListMap<Long, Cell> _expirationList;
    //list of entries reread from EDS & temporary inserted to the space
    private final IStoredList<EvictableServerEntry> _shortLived;
    private TimeBasedEvictionReaper _reaper;

    public TimeBasedSpaceEvictionStrategy(long inCacheTimeInterval, long inCacheReinsertedTimeInterval, long harvestMainInterval,
                                          long harvestShortLivedInterval) {
        _inCacheTimeInterval = inCacheTimeInterval;
        _inCacheReinsertedTimeInterval = Math.max(inCacheReinsertedTimeInterval, SHORT_LIVED_ENTRY_INTERVAL_LIMIT);
        _harvestMainInterval = harvestMainInterval;
        _harvestShortLivedInterval = harvestShortLivedInterval;
        _expirationList = new FastConcurrentSkipListMap<Long, Cell>();
        _shortLived = StoredListFactory.createConcurrentList(true/* fifo*/);
    }

    @Override
    public void initialize(SpaceEvictionManager evictionManager, SpaceEvictionStrategyConfig config) {
        super.initialize(evictionManager, config);
        String spaceName = ((CacheManager) (evictionManager)).getEngine().getSpaceName();
        _reaper = new TimeBasedEvictionReaper("TimeBasedEvictionReaper_" + spaceName);
        _reaper.start();
    }

    @Override
    public void close() {
        super.close();
        _reaper.clean();
    }

    @Override
    public void onInsert(EvictableServerEntry entry) {
        introduce(entry, true);
    }

    @Override
    public void onLoad(EvictableServerEntry entry) {
        introduce(entry, false);
    }

    @Override
    public void onRemove(EvictableServerEntry entry) {
        remove(entry, false);
    }

    @Override
    public int evict(int numOfEntries) {
        try {
            return forceReaperCycle();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /*
    @Override
    public boolean isVisible(EvictableServerEntry entry)
    {
        TimeBasedEvictionPayload payLoad = (TimeBasedEvictionPayload)entry.getEvictionPayLoad();
        if (payLoad == null )
            return false;
        return  (!payLoad.isShortLived() && payLoad.getExpirationTime() >= SystemTime.timeMillis());
    }
    */
    private void introduce(EvictableServerEntry entry, boolean newEntry) {
        //set insertion time for new entry. for old entry set to 30 seconds-
        //will be cleared in next round of thread if approved by space
        long interval = newEntry ? _inCacheTimeInterval : _inCacheReinsertedTimeInterval;
        long expiration = 0;
        expiration = interval + SystemTime.timeMillis();
        long expirationCellTime = generateListExpirationUponInsertion(interval, expiration);
        //for very short lived entries just insert to the SL
        if (interval <= SHORT_LIVED_ENTRY_INTERVAL_LIMIT) {
            Object payLoad = new TimeBasedEvictionPayload(_shortLived.add(entry), expiration, true);
            entry.setEvictionPayLoad(payLoad);
            return;
        }
        //insert into cells list
        Cell curCell = _expirationList.get(expirationCellTime);
        Cell newCell = null;
        while (true) {
            if (curCell != null) {
                Object payload = curCell.add(entry, expiration);
                if (payload == null) {//cell remove since it was empty
                    _expirationList.remove(expirationCellTime, curCell);
                    curCell = null;
                } else {//ok
                    entry.setEvictionPayLoad(payload);
                    break;
                }
            }
            if (newCell == null) {
                newCell = new Cell(expirationCellTime);
            }
            curCell = _expirationList.putIfAbsent(expirationCellTime, newCell);
            if (curCell == null) {
                Object payload = newCell.add(entry, expiration);
                if (payload == null) {//cell remove may be by cleanning thread- very rare but can happen
                    _expirationList.remove(expirationCellTime, newCell);
                    newCell = null;
                    continue;
                }
                entry.setEvictionPayLoad(payload);
                break;
            }
        }
    }

    private long generateListExpirationUponInsertion(long interval, long expiration) {
        return interval == 0 ? expiration : generateListExpiration(expiration);
    }

    private long generateListExpiration(long expiration) {
        return ((expiration / CELLS_GRANULARITY_INTERVAL + 1) * CELLS_GRANULARITY_INTERVAL);
    }

    private void remove(EvictableServerEntry entry, boolean fromReaperThread) {
        TimeBasedEvictionPayload payLoad = (TimeBasedEvictionPayload) entry.getEvictionPayLoad();
        if (payLoad == null) {
            if (fromReaperThread)
                return; //entry not completly inserted to the eviction strategy
            throw new IllegalStateException("Payload is null while remove called");
        }
        if (!payLoad.isShortLived()) {
            long expirationTime = generateListExpiration(payLoad.getExpirationTime());
            Cell curCell = _expirationList.get(expirationTime);
            curCell.remove(entry);
            if (curCell.setCleanedIfEmpty())
                _expirationList.remove(expirationTime, curCell);
        } else
            _shortLived.remove(payLoad._backref);
    }

    private int forceReaperCycle() throws InterruptedException {
        long currentCycle = _reaper.getCurrentCycle();
        if (_reaper.forceCycle())
            return _reaper.waitForCycleCompletion(currentCycle);
        else
            return 0;
    }

    private int reapExpiredEntries(boolean forced, boolean reapMainList) {
        int evicted = 0;
        //scan the shortlived list first
        IStoredListIterator<EvictableServerEntry> slh = null;
        long currentTime;
        try {
            if (!_shortLived.isEmpty()) {
                currentTime = SystemTime.timeMillis();
                for (slh = _shortLived.establishListScan(false); slh != null; slh = _shortLived.next(slh)) {
                    if (_reaper.shouldDie())
                        return evicted;
                    EvictableServerEntry entry = slh.getSubject();
                    if (entry == null || entry.getEvictionPayLoad() == null)
                        continue;
                    if (((TimeBasedEvictionPayload) (entry.getEvictionPayLoad()))._expirationTime <= currentTime && getEvictionManager().tryEvict(entry))
                        evicted++;

                }
            }
        } finally {
            if (slh != null)
                slh.release();
        }

        //should we process the main list too ?
        if (!forced && !reapMainList)
            return evicted;
        Iterator<Cell> iter = _expirationList.values().iterator();

        while (iter.hasNext()) {
            currentTime = SystemTime.timeMillis();
            Cell cell = (Cell) iter.next();
            long expirationTime = cell.getCellKey();

            if (expirationTime > currentTime) {
                if (currentTime < expirationTime - (CELLS_GRANULARITY_INTERVAL + 1))
                    break;
            }
            if (!cell._entriesExpired.isEmpty()) {
                try {
                    for (slh = cell._entriesExpired.establishListScan(false); slh != null; slh = cell._entriesExpired.next(slh)) {
                        if (_reaper.shouldDie())
                            return evicted;
                        EvictableServerEntry entry = slh.getSubject();
                        if (entry == null || entry.getEvictionPayLoad() == null)
                            continue;
                        if (((TimeBasedEvictionPayload) (entry.getEvictionPayLoad()))._expirationTime <= currentTime &&
                                getEvictionManager().tryEvict(entry))
                            evicted++;

                    }
                } finally {
                    if (slh != null)
                        slh.release();
                }
            }


        }
        return evicted;
    }

    private static class Cell {
        //cell key in cells' list
        private final Long _expirationTime;
        //uids of entries expired here
        private final IStoredList<EvictableServerEntry> _entriesExpired;

        private Cell(long expirationTime) {
            _expirationTime = expirationTime;
            _entriesExpired = StoredListFactory.createConcurrentList(true/* fifo*/);
        }

        private Long getCellKey() {
            return _expirationTime;
        }

        private boolean isEmpty() {
            return _entriesExpired.isEmpty();
        }

        //note-
        //return null if the cell is remove/being removed after all cleaned from it
        private Object add(EvictableServerEntry entry, long expiration) {
            IObjectInfo<EvictableServerEntry> backref = _entriesExpired.add(entry);
            return backref != null ? new TimeBasedEvictionPayload(backref, expiration, false) : null;
        }

        private void remove(EvictableServerEntry entry) {
            TimeBasedEvictionPayload payload = (TimeBasedEvictionPayload) entry.getEvictionPayLoad();
            _entriesExpired.remove(payload._backref);
        }

        private boolean setCleanedIfEmpty() {
            return isEmpty() && _entriesExpired.invalidate();
        }
    }

    public static class TimeBasedEvictionPayload {
        private final IObjectInfo<EvictableServerEntry> _backref;
        private final long _expirationTime;
        private final boolean _shortLived;

        public TimeBasedEvictionPayload(IObjectInfo<EvictableServerEntry> backref, long expirationTime, boolean shortLived) {
            _backref = backref;
            _expirationTime = expirationTime;
            _shortLived = shortLived;
        }

        private long getExpirationTime() {
            return _expirationTime;
        }

        private boolean isShortLived() {
            return _shortLived;
        }

    }

    /**
     * harvester daemon thread. Wakes up every _harvestInterval to evict expired entries and cleanup
     * obsolete data structures.
     */
    private final class TimeBasedEvictionReaper extends GSThread {
        private boolean _shouldDie;
        private long _nextExpirationTimeInterval = SystemTime.timeMillis();
        private long _nextExpirationMainListTimeInterval = _nextExpirationTimeInterval;
        private final Object _cycleLock = new Object();
        private long _cycleCount;
        private long _lastCycleEnded;
        private boolean _force;
        private int _evicted;

        public TimeBasedEvictionReaper(String threadName) {
            super(threadName);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            try {
                while (!isInterrupted()) {
                    try {
                        boolean reapMainList = fallAsleep();

                        if (_shouldDie)
                            break;

                        if (_logger.isLoggable(Level.FINEST))
                            _logger.finest(this.getName() + " - woke up for reaping.");

                        _evicted = reapExpiredEntries(_force, reapMainList);
                        signalEndCycle();
                    } catch (Exception e) {
                        if (_logger.isLoggable(Level.SEVERE))
                            _logger.log(Level.SEVERE, this.getName() + " - caught Exception", e);
                    }
                }
            } finally {
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine(this.getName() + " terminated.");
            }
        }

        /**
         * falls asleep for <tt>LM_EXPIRATION_TIME_INTERVAL</tt> repeatedly maintaining a
         * <i>fixed-rate execution</i>. <p> In fixed-rate execution, each execution is scheduled
         * relative to the scheduled execution time of the initial execution.  If an execution is
         * delayed for any reason (such as garbage collection or other background activity), two or
         * more executions will occur in rapid succession to "catch up." In the long run, the
         * frequency of execution will be exactly the reciprocal of the specified period (assuming
         * the system clock underlying <tt>Object.wait(long)</tt> is accurate).
         */
        private final boolean fallAsleep() {
            synchronized (this) {
                try {
                    if (!_shouldDie) {
                        _nextExpirationTimeInterval += _harvestShortLivedInterval;
                        long fixedRateDelay = _nextExpirationTimeInterval - SystemTime.timeMillis();

                        if (fixedRateDelay <= 0) {
                            if (_logger.isLoggable(Level.FINEST))
                                _logger.finest("Skipped fallAsleep since fixedRateDelay=" + fixedRateDelay);
                            _nextExpirationTimeInterval = SystemTime.timeMillis();
                            return shouldProcessMainList();
                        }

                        if (_logger.isLoggable(Level.FINEST))
                            _logger.finest("fallAsleep - going to wait fixedRateDelay=" + fixedRateDelay);
                        wait(fixedRateDelay);
                        if (_force) {
                            if (_logger.isLoggable(Level.FINEST))
                                _logger.finest("TimeBased Eviction reaper was forcibly waken up");
                            _nextExpirationTimeInterval = SystemTime.timeMillis();
                        }
                        return shouldProcessMainList();
                    }
                } catch (InterruptedException ie) {
                    if (_logger.isLoggable(Level.FINEST))
                        _logger.log(Level.FINEST, this.getName() + " interrupted.", ie);

                    _shouldDie = true;
                    interrupt();
                }
                return shouldProcessMainList();
            }
        }

        private boolean shouldProcessMainList() {
            if (_nextExpirationMainListTimeInterval <= SystemTime.timeMillis()) {
                _nextExpirationMainListTimeInterval += _harvestMainInterval;
                return true;
            }
            return false;
        }

        /**
         * Wakes up, waits for reaper to die, and clears all lease mappings.
         */
        protected final void clean() {
            if (!isAlive())
                return;

            synchronized (this) {
                _shouldDie = true;
                notify();
            }

            // wait for termination
            try {
                join();
            } catch (InterruptedException e) {
            }

            if (_expirationList != null)
                _expirationList.clear();
        }

        public boolean forceCycle() {
            synchronized (_cycleLock) {
                if (SystemTime.timeMillis() - _lastCycleEnded < TIMEBASED_EVICTION_MIN_FORCE_EXPIRATION_INTERVAL)
                    return false;
                //Signal force state
                _force = true;
            }
            synchronized (this) {
                notify();
                return true;
            }
        }

        public int waitForCycleCompletion(long currentCycle) throws InterruptedException {
            synchronized (_cycleLock) {
                while (_cycleCount == currentCycle && !_shouldDie)
                    _cycleLock.wait();
                return _evicted;
            }
        }

        private void signalEndCycle() {
            synchronized (_cycleLock) {
                _force = false;
                _cycleCount++;
                _lastCycleEnded = SystemTime.timeMillis();
                _cycleLock.notifyAll();
            }
        }

        private long getCurrentCycle() {
            synchronized (_cycleLock) {
                return _cycleCount;
            }
        }

        private boolean shouldDie() {
            return _shouldDie;
        }
    }
}
