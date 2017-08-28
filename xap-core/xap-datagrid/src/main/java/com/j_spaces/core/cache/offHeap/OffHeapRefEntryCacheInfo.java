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

//
package com.j_spaces.core.cache.offHeap;

import com.gigaspaces.internal.cluster.node.IReplicationOutContext;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.metadata.ServerTypeDesc;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.server.blobstore.BlobStoreException;
import com.gigaspaces.server.blobstore.BlobStoreObjectType;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.ILeasedEntryCacheInfo;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.offHeap.errors.BlobStoreErrorBulkEntryInfo;
import com.j_spaces.core.cache.offHeap.errors.BlobStoreErrorsHandler;
import com.j_spaces.core.cache.offHeap.optimizations.OffHeapIndexesValuesHandler;
import com.j_spaces.core.cache.offHeap.storage.InternalCacheControl;
import com.j_spaces.core.cache.offHeap.storage.bulks.BlobStoreBulkInfo;
import com.j_spaces.core.cache.offHeap.storage.bulks.BlobStoreBusyInBulkException;
import com.j_spaces.core.cache.offHeap.storage.bulks.delayedReplication.DelayedReplicationBasicInfo;
import com.j_spaces.core.cache.offHeap.storage.bulks.delayedReplication.DelayedReplicationInsertInfo;
import com.j_spaces.core.cache.offHeap.storage.bulks.delayedReplication.DelayedReplicationRemoveInfo;
import com.j_spaces.core.cache.offHeap.storage.bulks.delayedReplication.DelayedReplicationUpdateInfo;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.IStoredListIterator;
import com.j_spaces.kernel.locks.ILockObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * The resident part of entry that resides off-heap
 *
 * @author yechiel
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class OffHeapRefEntryCacheInfo
        implements IEntryCacheInfo, IOffHeapRefCacheInfo, ILockObject {

    public static final long UNALLOCATED_OFFHEAP_MEMORY = -1;

    private static final Object DummyOffHeapPos = new Object();
    private static final BlobStoreBusyInBulkException BusyInBulkIndicator = new BlobStoreBusyInBulkException();

    //in order to minimize access to disk we keep 8 bytes (long) and create a crc from the 4 bytes of each field for
    //the first 8 property (non dynamic) fields. in an equality match we use the preMatch method to decide weather to bring the entry from
    //offheap
    private volatile long _crcForFields;
    //indicator (bit for index) , bit 1 in pos i means the index ins single value- used in construction
    //of backrefs
    private long _singleValueIndexIndicators;
    private long _offHeapIndexValuesAddress = UNALLOCATED_OFFHEAP_MEMORY;

    private static final byte STATUS_PINNED = ((byte) 1) << 0;
    private static final byte STATUS_UNPINNED = ~STATUS_PINNED;

    private static final byte STATUS_DIRTY = ((byte) 1) << 1;
    private static final byte STATUS_UNDIRTY = ~STATUS_DIRTY;

    private static final byte STATUS_DELETED = ((byte) 1) << 2;
    private static final byte STATUS_UNDELETED = ~STATUS_DELETED;

    private static final byte STATUS_FULL_INDEXES_BACREFS_FORCED = ((byte) 1) << 3;
    private static final byte STATUS_UNFORCE_FULL_INDEXES_BACREFS = ~STATUS_FULL_INDEXES_BACREFS_FORCED;

    private static final byte STATUS_BULK_FLUSHING = ((byte) 1) << 4;
    private static final byte STATUS_UNBULK_FLUSHING = ~STATUS_BULK_FLUSHING;


    //is not null when entry is loaded. if null entry is not deleted. Its not null as long as the entry
    // is pinned- locked or under xtn including waiting-for != null
    private volatile OffHeapEntryHolder _loadedOffHeapEntry;
    private final String _m_Uid;

    private Object _backRefs; //only the ref to the main list or full list excluding single entry ref

    //pos in LM, or null if N.A.
    private IObjectInfo<Object> _leaseManagerEntryPos;


    //returned by the persister to indicate pos of entry offheap. can be null if not needed (like in hash-based disk table)
    private Object _offHeapPosition;

    //how many time the underlying entry was written to offheap. used in order to save unneeded offheap gets
    private volatile short _offHeapVersion;

    private final short _serverTypeDescCode;
    //the entry status
    private volatile byte _status;
    //creation number of latest index addition to the entry
    private byte _latestIndexCreationNumber;


    public OffHeapRefEntryCacheInfo(IEntryHolder eh, int backRefsSize) {
        boolean recoveredFromOffHeap = false;
        _loadedOffHeapEntry = (OffHeapEntryHolder) eh;
        if (((OffHeapEntryHolder) eh).getOffHeapVersion() != (short) 0) {
            //happens in recovery from OH
            _offHeapVersion = ((OffHeapEntryHolder) eh).getOffHeapVersion();
            recoveredFromOffHeap = true;
            setOffHeapIndexValuesAddress(OffHeapIndexesValuesHandler.allocate());
        }

        if (indexesBackRefsKept()) {
            if (backRefsSize != -1)
                _backRefs = new ArrayList<IObjectInfo<IEntryCacheInfo>>(backRefsSize);
            else
                _backRefs = new ArrayList<IObjectInfo<IEntryCacheInfo>>(3);
        }
        _m_Uid = eh.getUID();
        pin();        //a new entry is always set pinned
        if (!recoveredFromOffHeap)
            setDirty_impl(true, false/*set_indexes*/, null);
        _serverTypeDescCode = eh.getServerTypeDesc().getServerTypeDescCode();
    }

    public OffHeapRefEntryCacheInfo(IEntryHolder eh) {
        this(eh, -1);
    }

    //+++++++++++++++++++  IOffHeapRefCacheInfo methods


    private boolean isDirty() {
        return (_status & STATUS_DIRTY) == STATUS_DIRTY;
    }


    @Override
    public boolean isInOffHeapStorage() {
        return getOffHeapStoragePos() != null;
    }

    @Override
    public Object getOffHeapStoragePos() {
        synchronized (getStateLockObject()) {
            return getOffHeapPos();
        }

    }

    @Override
    public void setOffHeapVersion(short offHeapVersion) {
        synchronized (getStateLockObject()) {
            _offHeapVersion = offHeapVersion;
            _loadedOffHeapEntry.setOffHeapVersion(offHeapVersion);
        }

    }

    public void setOffHeapIndexValuesAddress(long _offHeapIndexValuesAddress) {
        this._offHeapIndexValuesAddress = _offHeapIndexValuesAddress;
    }

    public long getOffHeapIndexValuesAddress() {
        return _offHeapIndexValuesAddress;
    }

    //note- this method must be called when the entry  is locked
    @Override
    public void setDirty(boolean value, CacheManager cacheManager) {
        synchronized (getStateLockObject()) {
            setDirty_impl(value, true /*set_indexses*/, cacheManager);
        }
    }

    @Override
    public void flushedFromBulk(CacheManager cacheManager, Object offHeapPos, boolean removed) {
        synchronized (getStateLockObject()) {
            setDirty_impl(false, false /*set_indexses*/, cacheManager);
            if (removed || isDeleted()) {
                removeFromInternalCache(cacheManager, _loadedOffHeapEntry);
                OffHeapIndexesValuesHandler.delete(this);
                _offHeapPosition = null;
            } else {
                if (!isWrittenToOffHeap()) {
                    insertOrTouchInternalCache(cacheManager, _loadedOffHeapEntry); //new entry- insert to cache
                    setOffHeapIndexValuesAddress(OffHeapIndexesValuesHandler.allocate());
                } else {
                    OffHeapIndexesValuesHandler.update(getOffHeapIndexValuesAddress());
                }
                if (offHeapPos == null)
                    _offHeapPosition = DummyOffHeapPos;
                else
                    _offHeapPosition = offHeapPos;
            }
        }
    }


    private void setDirty_impl(boolean value, boolean set_indexses, CacheManager cacheManager) {
        if (value) {
            _status |= STATUS_DIRTY;
            _crcForFields = buildCrcForFields(_loadedOffHeapEntry);
            _offHeapVersion = (short) (_offHeapVersion + (short) 1);
            _loadedOffHeapEntry.setOffHeapVersion(_offHeapVersion);
            if (set_indexses && !isDeleted())
                economizeBackRefs((ArrayList<IObjectInfo<IEntryCacheInfo>>) _backRefs, _loadedOffHeapEntry, cacheManager.getTypeData(_loadedOffHeapEntry.getServerTypeDesc()), false /*unloading*/, true/*flushingEntryHolder*/);
        } else
            _status &= STATUS_UNDIRTY;
    }

    @Override
    public void buildCrcForFields() {
        synchronized (getStateLockObject()) {
            _crcForFields = buildCrcForFields(_loadedOffHeapEntry);
        }
    }

    @Override
    public boolean isBulkFlushing() {
        return (_status & STATUS_BULK_FLUSHING) == STATUS_BULK_FLUSHING;
    }

    @Override
    public boolean setBulkFlushing(BlobStoreBulkInfo caller) {
        synchronized (getStateLockObject()) {
            if (isInBulk() && _loadedOffHeapEntry.getBulkInfo().isActive() && _loadedOffHeapEntry.getBulkInfo() == caller) {
                setBulkFlushing_impl(true);
                return true;
            } else
                return false;   //irrelevant- reject request
        }
    }


    private void setBulkFlushing_impl(boolean value) {
        if (value) {
            _status |= STATUS_BULK_FLUSHING;
        } else
            _status &= STATUS_UNBULK_FLUSHING;
    }

    @Override
    public IServerTypeDesc getServerTypeDesc()
    {
        IEntryHolder eh = _loadedOffHeapEntry;
        return eh != null ? eh.getServerTypeDesc() : ServerTypeDesc.getByServerTypeDescCode(_serverTypeDescCode);
    }

    //NOTE- we dont return "this" because its used as the lock object for the underlying entry
    private Object getStateLockObject() {
        return _m_Uid;
    }


    @Override
    public void setDeleted(boolean deleted) {
        if (!isPinned())
            throw new RuntimeException("setDeleted but entry not pinned uid=" + _m_Uid);
        if (deleted)
            _status |= STATUS_DELETED;
        else
            _status &= STATUS_UNDELETED;
    }


    @Override
    public boolean isPhantom() {
        synchronized (getStateLockObject()) {
            return _loadedOffHeapEntry.isPhantom();
        }

    }

    @Override
    public void removeEntryFromOffHeapStorage(CacheManager cacheManager) {
        synchronized (getStateLockObject()) {
            removeEntryFromOffHeapStorage_impl(cacheManager);
        }
    }

    private void removeEntryFromOffHeapStorage_impl(CacheManager cacheManager) {
        removeFromInternalCache(cacheManager, _loadedOffHeapEntry);
        if (isWrittenToOffHeap()) {
            cacheManager.getBlobStoreStorageHandler().removeIfExists(getStorageKey_impl(), getOffHeapPos(), BlobStoreObjectType.DATA);
        }
        _offHeapPosition = null;
    }

    @Override
    public IEntryHolder getLatestEntryVersion(CacheManager cacheManager, boolean attach, IOffHeapEntryHolder lastKnownEntry, Context attachingContext) {

        return getLatestEntryVersion(cacheManager, attach, lastKnownEntry, attachingContext, false/* onlyIndexesPart*/);
    }

    @Override
    public void resetNonTransactionalFailedBlobstoreOpStatus(CacheManager cm) {//reset status of failed op- the entry must be logically locked
        synchronized (getStateLockObject()) {
            removeFromInternalCache(cm, _loadedOffHeapEntry);
            setDirty(false, cm);
        }
    }

    @Override
    public IEntryHolder getLatestEntryVersion(CacheManager cacheManager, boolean attach, IOffHeapEntryHolder lastKnownEntry, Context attachingContext, boolean onlyIndexesPart) {
        OffHeapEntryHolder res = null;
        if (!onlyIndexesPart &&  lastKnownEntry != null && lastKnownEntry.isOptimizedEntry())
            lastKnownEntry = null;
        if (!attach) {
            res = _loadedOffHeapEntry;
            if (res != null && !onlyIndexesPart && res.isOptimizedEntry())
                res = null;
            if (res != null)
                return res;
        }
        while (true) {
            try {
                 return getLatestEntryVersion_impl(cacheManager, attach, lastKnownEntry, attachingContext, onlyIndexesPart);
            } catch (BlobStoreBusyInBulkException ex) {//the current entry is within a bulk
                //1. if this thread is the owner of a different bulk terminate it in order to prevent intersection between 2 bulks
                //      which can cause deadlocks && delays
                if (attachingContext.isActiveBlobStoreBulk())
                    attachingContext.getBlobStoreBulkInfo().bulk_flush(attachingContext, false /*only_if_chunk_reached*/, true);

                //2  I need an entry which is in bulk. in order to prevent delayes & deadlocks check if I
                //can remove the entry from the bulk and perform the SSD op on it myself
                BlobStoreBulkInfo bulkInfo = null;
                synchronized (getStateLockObject()) {
                    res = _loadedOffHeapEntry;
                    bulkInfo = res != null ? res.getBulkInfo() : null;
                    if (res != null && bulkInfo != null && !isBulkFlushing()) {
                        try {
                            BlobStoreErrorBulkEntryInfo.setOnContext(attachingContext, bulkInfo.getPerviousStateForEntry(_m_Uid));
                            flush_impl(cacheManager, attachingContext, false /*unloadingEntry*/
                                    , InternalCacheControl.DONT_INSERT_TO_INTERNAL_CACHE_FROM_INITIAL_LOAD);
                            if (bulkInfo.getDirectPersistencyCoordinationObject(getUID()) != null) {
                                //report to direct persistency
                                cacheManager.getEngine().getReplicationNode().getDirectPesistencySyncHandler().afterOperationPersisted(bulkInfo.getDirectPersistencyCoordinationObject(getUID()));
                                DelayedReplicationBasicInfo dri = bulkInfo.getDelayedReplicationInfo(_m_Uid);
                                if (dri != null)
                                    //replication delayed now put in redolog
                                    handleDelayedReplication(cacheManager, attachingContext, dri);
                            }

                            bulkUnRegister(cacheManager);
                            continue;  //next try
                        } catch (Exception t) {
                            res.getBulkInfo().setExecption(t);
                            throw !(t instanceof BlobStoreException) ? new BlobStoreException(t) : (BlobStoreException) t;
                        }
                    }
                }//synchronized
                //3. wait for the flush on the current entry to complete while not locking the eci
                if (res != null && res.getBulkInfo() != null) {
                    BlobStoreBulkInfo bi = res.getBulkInfo();
                    if (bi != null && bi == bulkInfo)
                        bi.waitForBulkFlush(this);
                }
            }
        }

    }

    private IEntryHolder getLatestEntryVersion_impl(CacheManager cacheManager, boolean attach, IOffHeapEntryHolder lastKnownEntry, Context context, boolean onlyIndexesPart) {
        OffHeapEntryHolder res = null;
        synchronized (getStateLockObject()) {
            //is this entry part of a bulk?
            //report back
            res = _loadedOffHeapEntry;
            if (res != null && !attach) {
                if (!onlyIndexesPart && res.isOptimizedEntry())
                {
                    if (!isWrittenToOffHeap() || res.isDeleted() || isPhantom())
                        return res;
                    res = null;
                }
                else
                    return res;
            }
            if (!attach && context != null && (res = getPreFetchedEntry(cacheManager, context)) != null) {
                OffHeapIndexesValuesHandler.get(getOffHeapIndexValuesAddress());
                return res;
            }
            if (res != null) {
                if (res.getBulkInfo() != null && res.getBulkInfo().isActive() && res.getBulkInfo().getOwnerThread() != Thread.currentThread()) {
                    throw BusyInBulkIndicator;
                }
                //entry can be pinned
                // 1. after bulk-flush when its not unpinned yet from after-bulk op
                //in BlobStoreBulkInfo- but it can not be dirty
                //2. when entry is locked under xtn . in this case it cant be optimized since optimized is disabled
                //   unser xtns
                if (!isPinned())
                    throw new RuntimeException("entry attach and entry in RefEntryCacheInfo but not pinned " + _m_Uid);
                if (!onlyIndexesPart && res.isOptimizedEntry())
                {
                    if (isDirty())
                        throw new RuntimeException("invalid entry state- entry attach and dirty" + _m_Uid);
                    unLoadFullEntryIfPossible_impl(cacheManager,context, InternalCacheControl.DONT_INSERT_TO_INTERNAL_CACHE);
                    res = null;
                }
                else
                    return res;
            }

            if (lastKnownEntry != null && lastKnownEntry.getOffHeapVersion() == _offHeapVersion)
            {//the latest known entry wasnt changed- use it, no need to access off-heap storage
                res = (OffHeapEntryHolder) lastKnownEntry;
                if (attach) {
                    _loadedOffHeapEntry = res;
                    if (indexesBackRefsKept() && !is_full_indexes_backrefs_forced())
                        _backRefs = buildBackrefsArrayListFromOffHeap(cacheManager.getTypeData(res.getServerTypeDesc()), res);

                    pin();
                }
                return res;
            }
            res = getFullEntry(cacheManager, onlyIndexesPart);
            if (attach) {
                _loadedOffHeapEntry = res;
                if (indexesBackRefsKept() && !is_full_indexes_backrefs_forced()) {
                    _backRefs = buildBackrefsArrayListFromOffHeap(cacheManager.getTypeData(res.getServerTypeDesc()), res);
                }
                pin();
            }
            return res;
        }
    }


    private void handleDelayedReplication(CacheManager cacheManager, Context context, DelayedReplicationBasicInfo dri) {
        try {
            IReplicationOutContext ro = cacheManager.getReplicationContext(context);
            ro.setDirectPersistencyPendingEntry(dri.getDirectPersistencyOpInfo());
            switch (dri.getOpCode()) {
                case INSERT:
                    DelayedReplicationInsertInfo ii = (DelayedReplicationInsertInfo) dri;
                    cacheManager.handleInsertEntryReplication(context, dri.getEntry());
                    break;
                case UPDATE:
                    DelayedReplicationUpdateInfo ui = (DelayedReplicationUpdateInfo) dri;
                    cacheManager.handleUpdateEntryReplication(context, ui.getEntry(), ui.getOriginalData(), ui.getMutators());
                    break;
                case REMOVE:
                    DelayedReplicationRemoveInfo ri = (DelayedReplicationRemoveInfo) dri;
                    cacheManager.handleRemoveEntryReplication(context, ri.getEntry(), ri.getRemoveReason());
                    break;
            }
        } catch (SAException ex) {
            CacheManager.getLogger().severe("Blobstore- OHRECI:handleDelayedReplication got execption" + ex.toString() + ex.getStackTrace());
            throw new RuntimeException("Blobstore- OHRECI:handleDelayedReplication got execption" + ex.toString() + ex.getStackTrace());
        }
    }

    private OffHeapEntryHolder getPreFetchedEntry(CacheManager cacheManager, Context context) {
        OffHeapEntryHolder res = null;
        if (context.getBlobStorePreFetchBatchResult() != null) {
            OffHeapEntryLayout ole = context.getBlobStorePreFetchBatchResult().getFromStore(this);
            if (ole != null) {
                if (ole.getOffHeapVersion() != _offHeapVersion)
                    return null;
                res = ole.buildOffHeapEntryHolder(cacheManager, this);
            } else {
                //was it in local offheap cache
                res = context.getBlobStorePreFetchBatchResult().getFromCache(this);
                if (res != null && res.getOffHeapVersion() != _offHeapVersion)
                    res = null;
            }
        }
        return res;
    }


    private OffHeapEntryHolder getFullEntry(CacheManager cacheManager, boolean onlyIndexesPart) {
        OffHeapEntryHolder dbe = getFromInternalCache(cacheManager);
        if (dbe != null && dbe.getOffHeapVersion() != _offHeapVersion)
            dbe = null;  //not the recent one= ignore
        if (dbe == null) {
            if (isWrittenToOffHeap()) {
                OffHeapIndexesValuesHandler.get(getOffHeapIndexValuesAddress());
                OffHeapEntryLayout ole = (OffHeapEntryLayout) cacheManager.getBlobStoreStorageHandler().get(getStorageKey_impl(), _offHeapPosition, BlobStoreObjectType.DATA, onlyIndexesPart);
                dbe = ole != null ? ole.buildOffHeapEntryHolder(cacheManager, this) : null;
                if (dbe == null)
                    throw new RuntimeException("loadFullEntryIfNeeded entry not found in blob-storage key=" + getUID() + " deleted=" + isDeleted() + " pinned=" + isPinned() + " isWrittenToOffHeap=" + isWrittenToOffHeap());
            }
        }
        //if (dbe != null)
        //	insertOrTouchInternalCache(cacheManager,dbe);
        return dbe;
    }

    @Override
    public OffHeapEntryHolder getFromInternalCache(CacheManager cacheManager) {
        OffHeapEntryHolder res = cacheManager.getOffHeapInternalCache().get(this);
        return (res != null && res.getOffHeapVersion() == _offHeapVersion) ? res : null;
    }

    private void removeFromInternalCache(CacheManager cacheManager, OffHeapEntryHolder entry) {
        cacheManager.getOffHeapInternalCache().remove(entry);

    }

    public void insertOrTouchInternalCache(CacheManager cacheManager, OffHeapEntryHolder entry) {
        cacheManager.getOffHeapInternalCache().store(entry);
    }

    @Override
    public void unLoadFullEntryIfPossible(CacheManager cacheManager, Context context) {
        synchronized (getStateLockObject()) {
            unLoadFullEntryIfPossible_impl(cacheManager, context, InternalCacheControl.INSERT_IF_NEEDED_BY_OP );
        }
    }

    @Override
    public void unLoadFullEntryIfPossible(CacheManager cacheManager, Context context, InternalCacheControl internalCacheControl) {
        synchronized (getStateLockObject()) {
            unLoadFullEntryIfPossible_impl(cacheManager, context, internalCacheControl);
        }
    }

    private void unLoadFullEntryIfPossible_impl(CacheManager cacheManager, Context context, InternalCacheControl internalCacheControl) {
        OffHeapEntryHolder entry = _loadedOffHeapEntry;
        if (entry == null)
            return;
        if (isDirty())
            flush_impl(cacheManager, context, true /*unloadingEntry*/, internalCacheControl);
        else {
            if (indexesBackRefsKept() && !isDeleted()) {
                economizeBackRefs((ArrayList<IObjectInfo<IEntryCacheInfo>>) _backRefs, entry, cacheManager.getTypeData(entry.getServerTypeDesc()), true /*unloading*/, false/*flushingEntryHolder*/);
            }
            if (internalCacheControl == InternalCacheControl.INSERT_TO_INTERNAL_CACHE_FROM_INITIAL_LOAD)
                insertOrTouchInternalCache(cacheManager, entry);
        }
        if (!isDeleted()) {
            _loadedOffHeapEntry = null;
            unpin();
        }
    }


    @Override
    public void flush(CacheManager cacheManager, Context context) {
        synchronized (getStateLockObject()) {
            flush_impl(cacheManager, context, false /* unloadingEntry*/, InternalCacheControl.INSERT_IF_NEEDED_BY_OP);
        }
    }


    private void flush_impl(CacheManager cacheManager, Context context, boolean unloadingEntry, InternalCacheControl internalCacheControl) {
        boolean isFromInitialLoad = internalCacheControl == InternalCacheControl.DONT_INSERT_TO_INTERNAL_CACHE_FROM_INITIAL_LOAD
                || internalCacheControl == InternalCacheControl.INSERT_TO_INTERNAL_CACHE_FROM_INITIAL_LOAD;
        try {
            if (!isDirty())
                return;
            OffHeapEntryHolder entry = _loadedOffHeapEntry;
            if (entry == null)
                return;
            if (entry.isPhantom() && !isDeleted())
                //embedded sync list we need to update
                throw new BlobStoreException("inconsistent state - phantom but entry not signaled as deleted!!! uid=" + _m_Uid);

            if (isDeleted() && !entry.isPhantom()) {
                removeEntryFromOffHeapStorage_impl(cacheManager);
                OffHeapIndexesValuesHandler.delete(this);
            } else {
                if (isPhantom())
                    removeFromInternalCache(cacheManager, _loadedOffHeapEntry);
                //create an economized backref array if bacrefs kept
                if (indexesBackRefsKept() && unloadingEntry)
                    economizeBackRefs((ArrayList<IObjectInfo<IEntryCacheInfo>>) _backRefs, entry, cacheManager.getTypeData(entry.getServerTypeDesc()), unloadingEntry, true/*flushingEntryHolder*/);

                if (!isWrittenToOffHeap()) {
                    if (internalCacheControl == InternalCacheControl.INSERT_IF_NEEDED_BY_OP)
                        insertOrTouchInternalCache(cacheManager, entry); //new entry- insert to cache
                    _offHeapPosition = cacheManager.getBlobStoreStorageHandler().add(getStorageKey_impl(), getEntryLayout_impl(cacheManager, entry), BlobStoreObjectType.DATA);
                    setOffHeapIndexValuesAddress(OffHeapIndexesValuesHandler.allocate());
                } else {
                    _offHeapPosition = cacheManager.getBlobStoreStorageHandler().replace(getStorageKey_impl(), getEntryLayout_impl(cacheManager, entry), getOffHeapPos(), BlobStoreObjectType.DATA);
                    OffHeapIndexesValuesHandler.update(getOffHeapIndexValuesAddress());
                }

                if (_offHeapPosition == null)
                    _offHeapPosition = DummyOffHeapPos;
            }
            setDirty_impl(false, false, cacheManager);
        } catch (BlobStoreException bex) {
            if (!isFromInitialLoad)
                revertFailedOp(cacheManager, context, bex);
            throw bex;
        }
    }


    private void revertFailedOp(CacheManager cacheManager, Context context, BlobStoreException cause) {
        try {
            if (isDeleted())
                BlobStoreErrorsHandler.onFailedRemove(cacheManager, context, this, _loadedOffHeapEntry);
            else if (!isWrittenToOffHeap())
                BlobStoreErrorsHandler.onFailedWrite(cacheManager, context, this, _loadedOffHeapEntry);
            else
                BlobStoreErrorsHandler.onFailedUpdate(cacheManager, context, this, _loadedOffHeapEntry);
        } catch (Exception ex) {
        }
    }

    @Override
    public java.io.Serializable getStorageKey() {
        synchronized (getStateLockObject()) {
            return getStorageKey_impl();
        }
    }


    private java.io.Serializable getStorageKey_impl() {
        return _m_Uid;
    }

    @Override
    public java.io.Serializable getEntryLayout(CacheManager cacheManager) {
        //NOTE- function must be called when entry is pinned
        synchronized (getStateLockObject()) {
            return getEntryLayout_impl(cacheManager, _loadedOffHeapEntry);
        }

    }

    private java.io.Serializable getEntryLayout_impl(CacheManager cacheManager, OffHeapEntryHolder entry) {
        return new OffHeapEntryLayout(entry, cacheManager.isPersistentBlobStore()/*recoverable*/);
    }

    private Object getOffHeapPos() {
        return _offHeapPosition;
    }

    @Override
    public void setOffHeapPosition(Object pos) {
        synchronized (getStateLockObject()) {
            _offHeapPosition = pos != null ? pos : DummyOffHeapPos;
        }
    }


    //always called from synchronized code
    private void pin() {
        if (!isPinned())
            _status |= STATUS_PINNED;
        else
            throw new RuntimeException("OffHeapCacheInfo: pin called but already pinned " + getUID());
    }

    //always called from synchronized code
    private void unpin() {
        if (isPinned())
            _status &= STATUS_UNPINNED;
        else
            throw new RuntimeException("OffHeapCacheInfo: unpin called but not pinned uid= " + getUID());

//>>>>>>>>>>>>>>>DEBUG INFO REMOVE
        if (_loadedOffHeapEntry != null && !isDeleted())
            throw new RuntimeException("OffHeapCacheInfo: unpin called and _loadedOffHeapEntry != null uid= " + getUID());
    }


    private void set_full_indexes_backrefs_forced(boolean value) {
        if (value)
            _status |= STATUS_FULL_INDEXES_BACREFS_FORCED;
        else
            _status &= STATUS_UNFORCE_FULL_INDEXES_BACREFS;
    }

    private boolean is_full_indexes_backrefs_forced() {
        return (_status & STATUS_FULL_INDEXES_BACREFS_FORCED) == STATUS_FULL_INDEXES_BACREFS_FORCED;
    }


    private boolean isWrittenToOffHeap() {
        return _offHeapPosition != null;
    }


    @Override
    public IEntryHolder getEntryHolderIfInMemory() {
        return _loadedOffHeapEntry;
    }


    @Override
    public void bulkRegister(Context context, BlobStoreBulkInfo bulkInfo, int spaceOperation, boolean registerDirectPersistency) {

        synchronized (getStateLockObject()) {
            bulkInfo.add(context, this, spaceOperation, registerDirectPersistency);
            _loadedOffHeapEntry.setBulkInfo(bulkInfo);
        }
    }

    @Override
    public void bulkUnRegister(CacheManager cacheManager) {
        synchronized (getStateLockObject()) {
            OffHeapEntryHolder loadedOffHeapEntry = _loadedOffHeapEntry;
            loadedOffHeapEntry.setBulkInfo(null);
            setBulkFlushing_impl(false);
        }
    }


    public boolean isInBulk() {
        OffHeapEntryHolder loadedOffHeapEntry = _loadedOffHeapEntry;
        return (loadedOffHeapEntry != null && loadedOffHeapEntry.getBulkInfo() != null);
    }


    //------------------ end of  IOffHeapCacheInfo methods


    public IEntryHolder getEntryHolder() {
        return _loadedOffHeapEntry;
    }


    public IEntryHolder getEntryHolder(CacheManager cacheManager) {
        Context context = cacheManager.viewCacheContext();
        boolean canBeOptimized = context != null && context.getOptimizedBlobStoreReadEnabled() != null && context.getOptimizedBlobStoreReadEnabled();
        if (context != null)
            return getLatestEntryVersion(cacheManager, false /*attach*/, null, context, canBeOptimized || context.isInInitialLoad());
        else
            return getLatestEntryVersion(cacheManager, false, null, null);
    }

    @Override
    public IEntryHolder getEntryHolder(CacheManager cacheManager, Context context) {
        boolean canBeOptimized = context != null && context.getOptimizedBlobStoreReadEnabled() != null && context.getOptimizedBlobStoreReadEnabled();
        if (context != null)
            return getLatestEntryVersion(cacheManager, false /*attach*/, null, context, canBeOptimized || context.isInInitialLoad());
        else
            return getLatestEntryVersion(cacheManager, false, null, context);
    }


    /**
     * @return the m_BackRefs.
     */
    @Override
    public ArrayList<IObjectInfo<IEntryCacheInfo>> getBackRefs() {
        if (indexesBackRefsKept())
            return (ArrayList<IObjectInfo<IEntryCacheInfo>>) _backRefs;
        else
            return null;
    }

    private ArrayList<IObjectInfo<IEntryCacheInfo>> buildBackrefsArrayListFromOffHeap(TypeData pType, OffHeapEntryHolder entryHolder) {
        ArrayList<IObjectInfo<IEntryCacheInfo>> builtBrefs = new ArrayList<IObjectInfo<IEntryCacheInfo>>();
        //fill it
        long singleValueIndexIndicators = _singleValueIndexIndicators;
        IObjectInfo<IEntryCacheInfo>[] nonSingleRefs = null;

        if (_backRefs.getClass().isArray()) {
            nonSingleRefs = (IObjectInfo<IEntryCacheInfo>[]) _backRefs;
            builtBrefs.add(nonSingleRefs[0]); //main list
        } else
            builtBrefs.add((IObjectInfo<IEntryCacheInfo>) _backRefs);  //main list

        int bitpos = 0;
        //build the backrefs indexes
        if (pType.hasIndexes()) {
            int indxNum = 0;
            int backrefNonSelfPos = 1;

            IEntryData entryData = entryHolder.getEntryData();
            final TypeDataIndex[] indexes = pType.getIndexes();
            int pos = 0;
            for (TypeDataIndex<Object> index : indexes) {
                if (index.disableIndexUsageForOperation(pType, getLatestIndexCreationNumber()/*inputIndexCreationNumber*/))
                    continue;
                int numBackRefs = index.numOfEntryIndexBackRefs(index.getIndexValue(entryData));
                if (((singleValueIndexIndicators >> bitpos) & 1L) == 1L)
                    builtBrefs.add(this);
                else
                    builtBrefs.add(nonSingleRefs[backrefNonSelfPos++]);

                if (index.isExtendedIndex()) {
                    if ((((singleValueIndexIndicators >> (bitpos + 1)) & 1L) == 1L))
                        builtBrefs.add(this);
                    else if (numBackRefs == 2)
                        builtBrefs.add(nonSingleRefs[backrefNonSelfPos++]);
                }
                indxNum++;
                bitpos += (index.isExtendedIndex()) ? 2 : 1;

            }//for
        }

        return builtBrefs;
    }

    @Override
    public void setBackRefs(ArrayList<IObjectInfo<IEntryCacheInfo>> backRefs) {
        if (indexesBackRefsKept())
            _backRefs = backRefs;
        else {
            if (backRefs != null)
                throw new UnsupportedOperationException();
        }
    }


    private void economizeBackRefs(ArrayList<IObjectInfo<IEntryCacheInfo>> backRefs, OffHeapEntryHolder entryHolder, TypeData pType, boolean unloadingEntry, boolean flushingEntry) {
        if (is_full_indexes_backrefs_forced())
            return;    //no economizing

        Object newBackRefs = null;
        long singleValueIndexIndicators = 0;

        try {
            if (!pType.hasIndexes()) {
                newBackRefs = backRefs.get(0);
                return;
            }
            final TypeDataIndex[] indexes = pType.getIndexes();
            boolean anySingleRef = false;
            int indxNum = 0;
            int bitpos = 0;
            int arrayListPos = 1;
            int numNonSingleRefs = 0;

            IEntryData entryData = entryHolder.getEntryData();
            for (TypeDataIndex<Object> index : indexes) {
                if (index.disableIndexUsageForOperation(pType, getLatestIndexCreationNumber()))
                    continue;
                int numBackRefs = index.numOfEntryIndexBackRefs(index.getIndexValue(entryData));
                if (bitpos > 62) {
                    if (CacheManager.getLogger().isLoggable(Level.INFO)) {
                        CacheManager.getLogger().info("Blobstore- num of indexes exceeds efficient limit, num=" + (indxNum));
                    }
                    set_full_indexes_backrefs_forced(true);
                    _singleValueIndexIndicators = 0;
                    newBackRefs = null;
                    return;
                }
                if (backRefs.get(arrayListPos++) == this)
                    singleValueIndexIndicators |= (1L << bitpos);  //basic index ref
                else
                    numNonSingleRefs++;

                if (numBackRefs == 2) {
                    if (bitpos + 1 > 62) {
                        if (CacheManager.getLogger().isLoggable(Level.INFO)) {
                            CacheManager.getLogger().info("Blobstore- num of indexes exceeds efficient limit, num=" + (indxNum));
                        }
                        set_full_indexes_backrefs_forced(true);
                        _singleValueIndexIndicators = 0;
                        newBackRefs = null;
                        return;
                    }
                    if (backRefs.get(arrayListPos++) == this)
                        singleValueIndexIndicators |= (1L << (bitpos + 1));  //basic index ref
                    else
                        numNonSingleRefs++;
                }
                bitpos += (index.isExtendedIndex()) ? 2 : 1;
                indxNum++;
            }//for

            if (numNonSingleRefs > 0) {
                IObjectInfo<IEntryCacheInfo>[] nonSingleRefs = new IObjectInfo[numNonSingleRefs + 1];
                int pos = 0;
                for (IObjectInfo<IEntryCacheInfo> ref : backRefs) {
                    if (ref != this)
                        nonSingleRefs[pos++] = ref;
                }
                newBackRefs = nonSingleRefs;
            } else {
                newBackRefs = backRefs.get(0);
            }
        } finally {
            if (newBackRefs != null)//no exception thrown
            {
                _singleValueIndexIndicators = singleValueIndexIndicators;
                if (unloadingEntry)
                    _backRefs = newBackRefs;
            }
        }

    }


    @Override
    public IObjectInfo<IEntryCacheInfo> getMainListBackRef() {
        if (indexesBackRefsKept())
            return ((ArrayList<IObjectInfo<IEntryCacheInfo>>) _backRefs).get(0);
        else
            return (IObjectInfo<IEntryCacheInfo>) _backRefs;
    }

    @Override
    public boolean indexesBackRefsKept() {
        return TypeDataIndex.isIndexesBackRefsForOffHeapData();
    }

    @Override
    public void setMainListBackRef(IObjectInfo<IEntryCacheInfo> mainListBackref) {
        if (indexesBackRefsKept())
            ((ArrayList<IObjectInfo<IEntryCacheInfo>>) _backRefs).add(mainListBackref);
        else
            _backRefs = mainListBackref;
    }


    @Override
    public void setLeaseManagerListRefAndPosition(IStoredList<Object> entriesList, IObjectInfo<Object> entryPos) {
        _leaseManagerEntryPos = entryPos;
    }


    @Override
    public IStoredList<Object> getLeaseManagerListRef() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IObjectInfo<Object> getLeaseManagerPosition() {
        return _leaseManagerEntryPos;
    }

    @Override
    public boolean isConnectedToLeaseManager() {
        return _loadedOffHeapEntry.getExpirationTime() != Long.MAX_VALUE;
    }

    @Override
    public boolean isSameLeaseManagerRef(ILeasedEntryCacheInfo other) {
        return _leaseManagerEntryPos == other.getLeaseManagerPosition();
    }

    @Override
    public boolean isOffHeapEntry() {
        return true;
    }

    @Override
    public Object getObjectStoredInLeaseManager() {
        return _m_Uid;
    }


    @Override
    public int getLatestIndexCreationNumber() {
        return (int) _latestIndexCreationNumber;
    }

    @Override
    public void setLatestIndexCreationNumber(int val) {
        if (val > 255) {
            CacheManager.getLogger().severe("Blobstore- LatestIndexCreationNumber exceeds supported limit, num=" + val);
            throw new RuntimeException("Blobstore- LatestIndexCreationNumber exceeds supported limit, num=" + val);
        }
        _latestIndexCreationNumber = (byte) val;
    }


    @Override
    public String getClassName() {
        return _loadedOffHeapEntry.getClassName();
    }

    @Override
    public void setEvictionPayLoad(Object evictionBackRef) {
        throw new RuntimeException("setEvictionBackref invalid here");
    }


    @Override
    public Object getEvictionPayLoad() {
        return null;
    }


    @Override
    public String getUID() {
        return _m_Uid;
    }

    //dummy cache manipulation methods
    @Override
    public void setInCache(boolean checkPendingPin) {
    }

    @Override
    public boolean setPinned(boolean value, boolean waitIfPendingInsertion) {//relevant for lru/eviction based cache policy
        return true;
    }

    @Override
    public boolean setPinned(boolean value) {
        return setPinned(value, false /*waitIfPendingInsertion*/);
    }

    @Override
    public boolean isPinned() {
        return (_status & STATUS_PINNED) == STATUS_PINNED;
    }

    @Override
    public boolean setRemoving(boolean isPinned) {
        return true;
    }

    @Override
    public boolean isRemoving() {
        return false;
    }

    @Override
    public void setRemoved() {
    }

    @Override
    public boolean isRemoved() {
        return false;
    }

    @Override
    public boolean isRemovingOrRemoved() {
        return false;
    }


    @Override
    public boolean wasInserted() {
        return true;
    }

    @Override
    public boolean isRecentDelete() {
        return false;
    }

    @Override
    public void setRecentDelete() {
        throw new RuntimeException("invalid usage !!!");
    }

    @Override
    public boolean isDeleted() {
        return (_status & STATUS_DELETED) == STATUS_DELETED;

    }

    //+++++++++++++++++++  IStoredList-IObjectInfo methods for a unique-index single entry
    //+++++++++++++++++++  or single-values index
    @Override
    public IObjectInfo<IEntryCacheInfo> getHead() {
        throw new RuntimeException(" invalid usage");
    }

    @Override
    public IEntryCacheInfo getObjectFromHead() {
        return this;
    }

    @Override
    public void freeSLHolder(IStoredListIterator slh) {
        throw new RuntimeException(" invalid usage");
    }

    @Override
    public void remove(IObjectInfo oi) {
        throw new RuntimeException(" invalid usage");
    }

    @Override
    public void removeUnlocked(IObjectInfo oi) {
        throw new RuntimeException(" invalid usage");
    }

    @Override
    public boolean invalidate() {
        return false;
    }

    @Override
    public void dump(Logger logger, String msg) {
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public IStoredListIterator<IEntryCacheInfo> establishListScan(boolean random_scan) {
        return this;
    }

    @Override
    public IStoredListIterator<IEntryCacheInfo> next(IStoredListIterator<IEntryCacheInfo> slh) {
        return null;
    }

    @Override
    public IObjectInfo<IEntryCacheInfo> add(IEntryCacheInfo subject) {
        throw new RuntimeException(" invalid usage");
    }

    @Override
    public IObjectInfo<IEntryCacheInfo> addUnlocked(IEntryCacheInfo subject) {
        throw new RuntimeException(" invalid usage");
    }

    @Override
    public boolean removeByObject(IEntryCacheInfo obj) {
        return false;
    }

    @Override
    public boolean contains(IEntryCacheInfo obj) {
        return this == obj;
    }

    @Override
    public void setSubject(IEntryCacheInfo subject) {
    }

    @Override
    public IEntryCacheInfo getSubject() {
        return this;
    }

    @Override
    public boolean isMultiObjectCollection() {
        //its a single entry and not a container
        return false;
    }

    /**
     * return true if we can save iterator creation and get a single entry
     *
     * @return true if we can optimize
     */
    @Override
    public boolean optimizeScanForSingleObject() {
        return true;
    }

    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    //methoda for IScanListIterator
    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    @Override
    public boolean hasNext()
            throws SAException {
        return true;
    }

    @Override
    public IEntryCacheInfo next()
            throws SAException {
        return this;
    }

    /**
     * release SLHolder for this scan
     */
    @Override
    public void releaseScan()
            throws SAException {
    }

    /**
     * if the scan is on a property index, currently supported for extended index
     */
    @Override
    public int getAlreadyMatchedFixedPropertyIndexPos() {
        return -1;
    }

    @Override
    public String getAlreadyMatchedIndexPath() {
        return null;
    }

    /**
     * is the entry returned already matched against the searching template currently is true if the
     * underlying scan made by CacheManager::EntriesIter
     */
    @Override
    public boolean isAlreadyMatched() {
        return false;
    }

    @Override
    public boolean isIterator() {
        return false;
    }


    //+++++++++++++++++++ServerEntry methods

    /**
     * Gets the entry's type descriptor.
     *
     * @return Current entry's type descriptor.
     */
    @Override
    public SpaceTypeDescriptor getSpaceTypeDescriptor() {
        return getEntryHolder().getEntryData().getSpaceTypeDescriptor();
    }

    /**
     * Gets the specified fixed property's value.
     *
     * @param position Position of requested property.
     * @return Requested property's value in current entry.
     */
    @Override
    public Object getFixedPropertyValue(int position) {
        return getEntryHolder().getEntryData().getFixedPropertyValue(position);

    }

    /**
     * Gets the specified property's value.
     *
     * @param name Name of requested property.
     * @return Requested property's value in current entry.
     */
    @Override
    public Object getPropertyValue(String name) {
        return getEntryHolder().getEntryData().getPropertyValue(name);
    }

    @Override
    public Object getPathValue(String path) {
        return getEntryHolder().getEntryData().getPathValue(path);
    }

    /**
     * Gets the entry version.
     *
     * @return the entry version.
     * @since 9.0.0
     */
    @Override
    public int getVersion() {
        return getEntryHolder().getEntryData().getVersion();

    }

    /**
     * Gets the entry expiration time.
     *
     * @return the entry expiration time.
     * @since 9.0.0
     */
    @Override
    public long getExpirationTime() {
        return getEntryHolder().getEntryData().getExpirationTime();

    }


    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // methods for hash entry handling
    //---------------------------------------------------------------

    @Override
    public int getHashCode(int id) {
        return getKey(id).hashCode();
    }

    @Override
    public Object getKey(int id) {
        if (id == IEntryCacheInfo.UID_HASH_INDICATOR)
            return _m_Uid;
        return getEntryHolder().getEntryData().getFixedPropertyValue(id);
    }

    @Override
    public IStoredList<IEntryCacheInfo> getValue(int id) {
        return this;
    }

    @Override
    public boolean isNativeHashEntry() {
        return false;
    }

    @Override
    public void release() {

    }

    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    //+++++++++++++++ ILockObject methods
    //---------------------------------------------------------------
    @Override
    public boolean isLockSubject() {
        return false;
    }


    // *******************************************************************************************************
    //MATCHING
    // +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    //note - called when entry is unlocked
    @Override
    public boolean preMatch(Context context, ITemplateHolder template) {
        if (_loadedOffHeapEntry != null)
            return true;  //entry loaded- dont bother
        return template.getExtendedMatchCodes() == null ? preMatch_impl(context, template, _crcForFields) :
                preMatch_extendex_impl(context, template, _crcForFields);

    }


    //perform prematch onthe entry if applicable- return false if failed
    private static boolean preMatch_impl(Context context, ITemplateHolder template, long entryCrcForFields) {
        if (template.getEntryData().getFixedPropertiesValues() == null || template.getEntryData().getFixedPropertiesValues().length == 0)
            return true;   //cannot disqualify
        int lim = Math.min(8, template.getEntryData().getFixedPropertiesValues().length);
        for (int i = 0; i < lim; i++) {
            if (template.getEntryData().getFixedPropertyValue(i) == null)
                continue;
            byte entryCrc = (byte) ((entryCrcForFields >>> (i * 8)) & 0xFF);
            if (entryCrc == 0)
                continue;
            byte templateCrc = create8BitsCRCFromHashCode(template.getEntryData().getFixedPropertyValue(i).hashCode());
            if (templateCrc != entryCrc) {
                context.incrementNumOfEntriesMatched();
                return false;
            }
        }

        return true;
    }

    //perform prematch onthe entry if applicable- return false if failed
    private static boolean preMatch_extendex_impl(Context context, ITemplateHolder template, long entryCrcForFields) {
        if (template.getEntryData().getFixedPropertiesValues() == null || template.getEntryData().getFixedPropertiesValues().length == 0)
            return true;   //cannot disqualify
        int lim = Math.min(8, template.getExtendedMatchCodes().length);
        for (int i = 0; i < lim; i++) {
            if (template.getEntryData().getFixedPropertyValue(i) == null)
                continue;
            if (template.getExtendedMatchCodes()[i] != TemplateMatchCodes.EQ)
                continue;
            byte entryCrc = (byte) ((entryCrcForFields >>> (i * 8)) & 0xFF);
            if (entryCrc == 0)
                continue;
            byte templateCrc = create8BitsCRCFromHashCode(template.getEntryData().getFixedPropertyValue(i).hashCode());
            if (templateCrc != entryCrc) {
                context.incrementNumOfEntriesMatched();
                return false;
            }
        }

        return true;
    }


    //note- entry should be locked when calling this routine
    private static long buildCrcForFields(IEntryHolder entry) {
        long result = 0;

        int lim = Math.min(8, entry.getEntryData().getFixedPropertiesValues().length);
        for (int i = 0; i < lim; i++) {
            if (entry.getEntryData().getFixedPropertyValue(i) != null && !(entry.getEntryData().getFixedPropertyValue(i).getClass().isArray()) &&
                    !(entry.getEntryData().getFixedPropertyValue(i) instanceof Collection)) {
                int hashCode = entry.getEntryData().getFixedPropertyValue(i) == null ? 0 : entry.getEntryData().getFixedPropertyValue(i).hashCode();
                byte crc = create8BitsCRCFromHashCode(hashCode);
                long tmp = crc;
                tmp &= 0XFF;
                tmp = tmp << (i * 8);
                result |= tmp;
            }
        }
        return result;
    }


    private static byte create8BitsCRCFromHashCode(int hashCode) {
        byte checksum = 0;
        for (int i = 0; i < 4; i++) {
            byte data = (byte) ((hashCode >>> (i * 8)) & 0xFF);
            checksum = (byte) ((checksum + data) & 0xFF);
        }
        checksum = (byte) (((checksum ^ 0xFF) + 1) & 0xFF);
        if (checksum == 0)
            checksum = (byte) 1;
        return checksum;
    }

}
