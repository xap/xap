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

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.utils.concurrent.UncheckedAtomicIntegerFieldUpdater;
import com.gigaspaces.server.eviction.EvictableServerEntry;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @author Yechiel Fefer
 * @version 1.0
 * @since 5.0
 */
@com.gigaspaces.api.InternalApi
public class EvictableEntryCacheInfo extends MemoryBasedEntryCacheInfo implements EvictableServerEntry {
    private static final long serialVersionUID = -2238541327938669250L;

    //status of the entry regarding caching
    //pending- created or being in insertion process
    private static final int PENDING = 0;
    //inserted to cache & eviction strategy
    private static final int INCACHE = 1;
    //pinned or being inserted & pinned   to cache & eviction strategy
    private static final int PINNED = 2;
    //being removed  from cache
    private static final int REMOVING = 3;
    //removed from cache & eviction strategy
    private static final int REMOVED = 4;
    //entry is a recent deleted dummy
    private static final int RECENT_DELETE = 5;

    //cache status , one of the above values
    private volatile int _cacheStatus;
    private static final AtomicIntegerFieldUpdater<EvictableEntryCacheInfo> cacheStatusUpdater = UncheckedAtomicIntegerFieldUpdater.newUpdater(EvictableEntryCacheInfo.class, "_cacheStatus");
    private volatile boolean _notifyOnInsertionCompletion;

    //++++++++++++++++  eviction-strategy related fields +++++++++++++++++++++++++++++++++++++++//
    //eviction strategy status values
    //not inserted to eviction strategy
    private static final short NOT_INSERTED_TO_EVICTION_STRATEGY = 0;
    //inserted to eviction strategy- but not completly to cache
    private static final short IN_EVICTION_STRATEGY = 1;
    //removed from eviction strategy
    private static final short REMOVED_FROM_EVICTION_STRATEGY = 2;

    private volatile short _evictionStrategyStatus;

    private volatile boolean _notifyOnRemovalFromEvictionStrategy;

    //number of callers (currently touch callers) currently inside. in a protected E.S.
    //we dont allow touches after entry was removed
    private volatile int _numOfCurrentEvictionStrategyCallers;
    private static final AtomicIntegerFieldUpdater<EvictableEntryCacheInfo> _numOfCurrentEvictionStrategyCallersUpdater = UncheckedAtomicIntegerFieldUpdater.newUpdater(EvictableEntryCacheInfo.class, "_numOfCurrentEvictionStrategyCallers");
    private volatile boolean _needToRemoveFromEvictionStrategy;


    private volatile Object _evictionBackRef;


    public EvictableEntryCacheInfo(IEntryHolder entryHolder) {
        super(entryHolder);
    }

    public EvictableEntryCacheInfo(IEntryHolder entryHolder, int backRefsSize) {
        this(entryHolder, backRefsSize, false/*pinned*/);

    }

    public EvictableEntryCacheInfo(IEntryHolder entryHolder, int backRefsSize, boolean pinned) {
        super(entryHolder, backRefsSize);
        if (pinned)//pinned indicator is preset
            _cacheStatus = PINNED;

    }

    /**
     * in order to avoid searching the eviction handler data structures when an existing entry is
     * rendered to it by the space cache manager, the eviction-handler may store a backref to the
     * entry in it data structure and retrieve it later
     */
    public void setEvictionPayLoad(Object evictionBackRef) {
        _evictionBackRef = evictionBackRef;
    }


    /**
     * in order to avoid searching the eviction handler data structures when an existing entry is
     * rendered to it by the space cache manager, the eviction-handler may store a backref to the
     * entry in it data structure and retrieve it later
     *
     * @return the eviction back-ref to the entry, or null if not stored
     */
    public Object getEvictionPayLoad() {
        return _evictionBackRef;
    }

    public boolean setPinned(boolean value, boolean waitIfPendingInsertion) {
        if (value) {
            if (isPinned())
                return true;
            else //need CAS since readers can evict through eviction strategy
            {
                boolean res = cacheStatusUpdater.compareAndSet(this, INCACHE, PINNED);
                if (res || !waitIfPendingInsertion)
                    return res;
                //if needed- wait for insertion proceess to complete
                if (isPending()) {
                    _notifyOnInsertionCompletion = true;
                    synchronized (this) {
                        while (true) {
                            if (isPending()) {
                                try {
                                    wait();
                                } catch (InterruptedException e) {
                                    throw new RuntimeException("waiting for pinning interrupted " + e);
                                }
                            } else
                                break;
                        }
                    }
                }
                res = cacheStatusUpdater.compareAndSet(this, INCACHE, PINNED);
                return res;
            }
        } else {//unpin-//no need for CAS since entry is wlways locked by the unpinner
            //and cannot be evicted by non-lockers since its pinned
            _cacheStatus = INCACHE;
            return true;
        }
    }

    public boolean setPinned(boolean value) {
        return setPinned(value, false /*waitIfPendingInsertion*/);
    }


    private boolean isPending() {
        return _cacheStatus == PENDING;
    }

    public boolean isPinned() {
        return _cacheStatus == PINNED;
    }

    @Override
    public boolean isRemoving() {
        return _cacheStatus == REMOVING;
    }


    @Override
    public boolean setRemoving(boolean isPinned) {
        if (isPinned) {//entry pinned & locked no need for cas
            _cacheStatus = REMOVING;
            return true;
        }
        //need cas to protect against eviction
        return cacheStatusUpdater.compareAndSet(this, INCACHE, REMOVING);
    }

    @Override
    public void setRemoved() {
        _cacheStatus = REMOVED;
    }

    @Override
    public boolean isRemoved() {
        return _cacheStatus == REMOVED;
    }

    @Override
    public boolean isRemovingOrRemoved() {
        return isRemoving() || isRemoved();
    }

    public void notifyWaitersOnFailure() {//insertion failed. if any part waiting- kick it out
        synchronized (this) {
            notifyAll();
        }
    }

    //during insertion process we have an entry with same uid in removing process.
    //wait for it to be out of eviction strategy
    public void verifyEntryRemovedFromStrategy() {
        if (isRemovedFromEvictionStrategy())
            return;

        _notifyOnRemovalFromEvictionStrategy = true;

        synchronized (this) {
            while (true) {
                if (!isRemovedFromEvictionStrategy()) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } else
                    return;
            }
        }

    }


    public void setInCache(boolean checkPendingPin) {//no need for cas- the inserter is changing the status
        _cacheStatus = INCACHE;
        //is a locking thread (pinner) waiting for insertion completion ?
        if (checkPendingPin && _notifyOnInsertionCompletion) {
            synchronized (this) {
                notifyAll();
            }
        }
    }

    public boolean isInCache() {
        return _cacheStatus == INCACHE;
    }

    public boolean isInserted() {
        return isInCache() || isPinned();
    }

    public boolean wasInserted() {
        return _cacheStatus != PENDING;
    }

    public boolean isRecentDelete() {
        return _cacheStatus == RECENT_DELETE;
    }

    public void setRecentDelete() {
        _cacheStatus = RECENT_DELETE;
    }

    public boolean isRemovedFromEvictionStrategy() {
        return _evictionStrategyStatus == REMOVED_FROM_EVICTION_STRATEGY;
    }

    public void setRemovedFromEvictionStrategy(boolean checkWaiters) {
        _evictionStrategyStatus = REMOVED_FROM_EVICTION_STRATEGY;
        //is eviction strategy wrapper waiting for this entry to be removed ?
        //in order not to allow for calling to inset to eviction strategy before the same uid has been removed
        //from it , notify any other thread waiting for this removal
        if (checkWaiters && _notifyOnRemovalFromEvictionStrategy) {
            synchronized (this) {
                notifyAll();
            }
        }
    }

    public boolean isInEvictionStrategy() {
        return _evictionStrategyStatus == IN_EVICTION_STRATEGY;
    }

    public void setInEvictionStrategy() {
        _evictionStrategyStatus = IN_EVICTION_STRATEGY;
    }


    public void verifyBeforeEntryRemoval() {
        _needToRemoveFromEvictionStrategy = true;
        if (_numOfCurrentEvictionStrategyCallersUpdater.compareAndSet(this, 0, (Integer.MIN_VALUE / 2)))
            return; //ok-no one in

        synchronized (this) {
            while (true) {
                try {
                    if (_numOfCurrentEvictionStrategyCallersUpdater.compareAndSet(this, 0, (Integer.MIN_VALUE / 2)))
                        return; //ok-no one in
                    wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * can eviction strategy be called ?
     *
     * @return true if E.S. can be called
     */
    public boolean permitCallingEvictionStrategy() {
        if (_needToRemoveFromEvictionStrategy || !isInEvictionStrategy())
            return false;
        int res = _numOfCurrentEvictionStrategyCallersUpdater.incrementAndGet(this);
        return res > 0;
    }

    /**
     * returned from call to E.S., notify remover if waiting
     */
    public void afterCallingEvictionStrategy() {
        int res = _numOfCurrentEvictionStrategyCallersUpdater.decrementAndGet(this);
        if (res == 0 && _needToRemoveFromEvictionStrategy) {
            synchronized (this) {
                notifyAll();

            }
        }
    }
}
