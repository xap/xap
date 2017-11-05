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
package com.j_spaces.core.cache.blobStore.storage.bulks;

import com.gigaspaces.internal.cluster.node.IReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.IDirectPersistencyOpInfo;
import com.gigaspaces.server.blobstore.BlobStoreBulkOperationRequest;
import com.gigaspaces.server.blobstore.BlobStoreBulkOperationResult;
import com.gigaspaces.server.blobstore.BlobStoreException;
import com.gigaspaces.server.blobstore.BlobStoreObjectType;
import com.gigaspaces.server.blobstore.BlobStoreRemoveBulkOperationRequest;
import com.gigaspaces.server.blobstore.BlobStoreReplaceBulkOperationRequest;
import com.gigaspaces.server.blobstore.BlobStoreAddBulkOperationRequest;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.blobStore.BlobStoreEntryLayout;
import com.j_spaces.core.cache.blobStore.optimizations.OffHeapIndexesValuesHandler;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.blobStore.BlobStoreRefEntryCacheInfo;
import com.j_spaces.core.cache.blobStore.errors.BlobStoreErrorBulkEntryInfo;
import com.j_spaces.core.cache.blobStore.errors.BlobStoreErrorsHandler;
import com.j_spaces.core.cache.blobStore.storage.bulks.delayedReplication.DelayedReplicationBasicInfo;
import com.j_spaces.core.cache.blobStore.storage.bulks.delayedReplication.DelayedReplicationInsertInfo;
import com.j_spaces.core.cache.blobStore.storage.bulks.delayedReplication.DelayedReplicationRemoveInfo;
import com.j_spaces.core.cache.blobStore.storage.bulks.delayedReplication.DelayedReplicationUpdateInfo;
import com.j_spaces.core.server.processor.BlobStoreUnpinMultiplePacket;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

@com.gigaspaces.api.InternalApi
public class BlobStoreBulkInfo {
    /**
     * contains info & operations regarding blob store bulk used in write/updatae/take Multiple
     *
     * @author yechiel
     * @since 10.1
     */
    private static final int BULK_CHUNK_SIZE = 1000;
    private static final long BULK_MAX_WAIT_TIME = 1000 * 60 * 5;


    private final Logger _logger;
    private final CacheManager _cacheManager;
    private final HashMap<String, BulkOpHolder> _uids;    //entries participants
    private final List<BulkOpHolder> _ops;
    private volatile boolean _terminated;
    private final Object _lock;
    private final Thread _ownerThread;
    private final boolean _takeMultipleBulk;
    private volatile Throwable _exceptionOccured;
    private int _lastNotificationStamp;
    private volatile boolean _disabled;
    private HashSet<String> _removedEntries;
    //direct persistency - uid to IDirectPersistencyEntry
    private final Map<String, IDirectPersistencyOpInfo> _directPersistencyCoordinationObjects;
    //embedded list- handled delayed replication
    private final Map<String, DelayedReplicationBasicInfo> _delayedReplMap;
    // backup bulks support
    private static final AtomicInteger _bulkIdCounter = new AtomicInteger(1);
    private int _blobstoreBulkId;
    //revert on error support-currently on backup
    private final Map<String, BlobStoreErrorBulkEntryInfo> _previousStates;


    public BlobStoreBulkInfo(CacheManager cacheManager, boolean takeMultipleBulk) {
        _cacheManager = cacheManager;
        _uids = new HashMap<String, BulkOpHolder>();
        _ops = new LinkedList<BulkOpHolder>();
        _lock = new Object();
        _logger = _cacheManager.getLogger();
        _ownerThread = Thread.currentThread();
        _takeMultipleBulk = takeMultipleBulk;
        _directPersistencyCoordinationObjects = (cacheManager.getEngine().getReplicationNode() != null && cacheManager.getEngine().getReplicationNode().getDirectPesistencySyncHandler() != null) ? new Hashtable<String, IDirectPersistencyOpInfo>() : null;
        _delayedReplMap = new Hashtable<String, DelayedReplicationBasicInfo>();
        _previousStates = new Hashtable<String, BlobStoreErrorBulkEntryInfo>();
    }

    public void add(Context context, BlobStoreRefEntryCacheInfo eci, int operation /*SpaceOperation*/, boolean registerDirectPersistency) {
        synchronized (_lock) {
            if (!isActive())
                throw new RuntimeException("try to add uid to bulk-info when bulk inactive, uid=" + eci.getUID());
            if (_exceptionOccured != null)
                throw new BlobStoreException("bulk operation failed- a previous exception have been thrown " + _exceptionOccured);

            if (_uids.containsKey(eci.getUID()))
                throw new RuntimeException("Duplicate uid in bulk-info, uid=" + eci.getUID());

            generateBulkIdIfNeeded(registerDirectPersistency);

            BulkOpHolder boh = new BulkOpHolder(operation, eci);
            _ops.add(boh);
            _uids.put(eci.getUID(), boh);
            if (_removedEntries != null && _ops.size() == 1)
                _removedEntries.clear();//prev chunk

            if (registerDirectPersistency)
                _directPersistencyCoordinationObjects.put(eci.getUID(), context.getReplicationContext().getDirectPersistencyPendingEntry());

            if (_previousStates != null && BlobStoreErrorBulkEntryInfo.isRelevant(operation))
                _previousStates.put(eci.getUID(), new BlobStoreErrorBulkEntryInfo(context));
        }
    }

    public void addDelayedReplicationInfo(Context context, BlobStoreRefEntryCacheInfo eci, DelayedReplicationBasicInfo dri) {
        synchronized (_lock) {
            if (!isActive())
                throw new RuntimeException("try to addDelayedReplicationInfo to bulk-info when bulk inactive, uid=" + eci.getUID());

            IDirectPersistencyOpInfo oi = _directPersistencyCoordinationObjects.get(eci.getUID());
            if (oi == null)
                throw new RuntimeException("try to addDelayedReplicationInfo to bulk-info when no direct persistency for uid=" + eci.getUID());
            BulkOpHolder boh = _uids.get(eci.getUID());
            if (boh == null || boh.isDiscarded())
                throw new RuntimeException("try to addDelayedReplicationInfo to bulk-info when no relevant bulkinfoholder for uid=" + eci.getUID());
            dri.setDirectPersistencyOpInfo(oi);
            _delayedReplMap.put(eci.getUID(), dri);
            //clear the replication context since its delayed
            IReplicationOutContext ro = _cacheManager.getReplicationContext(context);
            ro.setDirectPersistencyPendingEntry(null);
        }
    }

    public boolean isActive() {
        return !_terminated;
    }


    public IDirectPersistencyOpInfo getDirectPersistencyCoordinationObject(String uid) {
        return _directPersistencyCoordinationObjects != null ? _directPersistencyCoordinationObjects.get(uid) : null;
    }

    public DelayedReplicationBasicInfo getDelayedReplicationInfo(String uid) {
        return _delayedReplMap.get(uid);
    }


    public void bulk_flush(Context context, boolean only_if_chunk_reached) {

        bulk_flush(context, only_if_chunk_reached, false /*termination*/);
    }


    public void bulk_flush(Context context, boolean only_if_chunk_reached, boolean termination) {
        if (only_if_chunk_reached && termination) {
            throw new IllegalArgumentException();
        }

        if (_cacheManager.getEngine().isReplicatedPersistentBlobstore() && _cacheManager.getEngine().getSpaceImpl().isPrimary()) {
            _cacheManager.getReplicationContext(context).blobstorePendingReplicationBulk();
        }

        if (_terminated) {
            return;
        }
        if (only_if_chunk_reached && _uids.size() < BULK_CHUNK_SIZE) {
            return;
        }

        if (_uids.isEmpty()) {
            afterBulkFlush(context, termination);
            return;
        }

        if (_exceptionOccured != null) {
            throw new BlobStoreException("bulk operation failed- a previous exception has been thrown " + _exceptionOccured);
        }

        //prepare the input to bulk op
        //call the underlying blobStore
        try {
            List<BlobStoreBulkOperationRequest> operations = new LinkedList<BlobStoreBulkOperationRequest>();
            for (BulkOpHolder boh : _ops) {
                //for each entry set its status to BULK_FLUSHING. if the set command is rejected-
                //ignore this entry
                if (!boh.getBlobStoreRefEntryCacheInfo().setBulkFlushing(this)) {
                    boh.setDiscarded();
                    continue;
                }
                switch (boh.getOperation()) {
                    case SpaceOperations.WRITE:
                        operations.add(new BlobStoreAddBulkOperationRequest(boh.getBlobStoreRefEntryCacheInfo().getStorageKey(), boh.getBlobStoreRefEntryCacheInfo().getEntryLayout(_cacheManager)));
                        break;
                    case SpaceOperations.UPDATE:
                        operations.add(new BlobStoreReplaceBulkOperationRequest(boh.getBlobStoreRefEntryCacheInfo().getStorageKey(),
                                boh.getBlobStoreRefEntryCacheInfo().getEntryLayout(_cacheManager), boh.getBlobStoreRefEntryCacheInfo().getBlobStoreStoragePos()));
                        break;
                    case SpaceOperations.TAKE:
                        boolean phantom = _cacheManager.isDirectPersistencyEmbeddedtHandlerUsed() && boh.getBlobStoreRefEntryCacheInfo().isPhantom();
                        if (!phantom) //actual remove
                            operations.add(new BlobStoreRemoveBulkOperationRequest(boh.getBlobStoreRefEntryCacheInfo().getStorageKey(), boh.getBlobStoreRefEntryCacheInfo().getBlobStoreStoragePos()));
                        else //update
                            operations.add(new BlobStoreReplaceBulkOperationRequest(boh.getBlobStoreRefEntryCacheInfo().getStorageKey(),
                                    boh.getBlobStoreRefEntryCacheInfo().getEntryLayout(_cacheManager), boh.getBlobStoreRefEntryCacheInfo().getBlobStoreStoragePos()));
                        break;
                    default:
                        throw new UnsupportedOperationException("uid=" + boh.getBlobStoreRefEntryCacheInfo().getUID() + " operation=" + boh.getOperation());
                }

            }

            Throwable t = null;
            List<BlobStoreBulkOperationResult> results = !operations.isEmpty() ? _cacheManager.getBlobStoreStorageHandler().executeBulk(operations, BlobStoreObjectType.DATA, false/*transactional*/) : new LinkedList<BlobStoreBulkOperationResult>();
            //scan and if execption in any result- throw it

            for (BlobStoreBulkOperationResult res : results) {
                BulkOpHolder boh = _uids.get(res.getId());
                if (res.getException() != null) {
                    if (t == null)
                        t = res.getException();
                    continue;
                }
                //report to directpersistency of persisting the object
                if (_directPersistencyCoordinationObjects != null && !_directPersistencyCoordinationObjects.isEmpty()) {
                    _cacheManager.getEngine().getReplicationNode().getDirectPesistencySyncHandler().afterOperationPersisted(_directPersistencyCoordinationObjects.remove(boh.getBlobStoreRefEntryCacheInfo().getUID()));
                }

                //for each entry in result list perform setDirty to false, and set the blobStoreStoragePosition if applicable
                //the entry is also marked unpinned and out of bulk and is removed from memory!!!!!!!!!!!!!!!!!
                switch (boh.getOperation()) {
                    case SpaceOperations.WRITE:
                    case SpaceOperations.UPDATE:
                        boh.getBlobStoreRefEntryCacheInfo().flushedFromBulk(context,_cacheManager, res.getPosition(), false/*removed*/,false /*onXtnEnd*/);
                        break;
                    case SpaceOperations.TAKE:
                        boh.getBlobStoreRefEntryCacheInfo().flushedFromBulk(context,_cacheManager, null, true/*removed*/,false /*onXtnEnd*/);
                        //removeEntryFromCache only after removed from blob-store
                        _cacheManager.removeEntryFromCache(boh.getBlobStoreRefEntryCacheInfo().getEntryHolder(), false /*initiatedByEvictionStrategy*/, true/*locked*/, boh.getBlobStoreRefEntryCacheInfo()/* pEntry*/, CacheManager.RecentDeleteCodes.NONE);
                        if (_removedEntries == null)
                            _removedEntries = new HashSet<String>();
                        _removedEntries.add(boh.getBlobStoreRefEntryCacheInfo().getUID());
                        break;
                }
                //put in redo log if applicable
                DelayedReplicationBasicInfo dri = _delayedReplMap.get(boh.getBlobStoreRefEntryCacheInfo().getUID());
                if (dri != null) {//replication delayed now put in redolog
                    IReplicationOutContext ro = _cacheManager.getReplicationContext(context);
                    ro.setDirectPersistencyPendingEntry(dri.getDirectPersistencyOpInfo());
                    ro.setDirectPesistencySyncHandler(_cacheManager.getEngine().getReplicationNode().getDirectPesistencySyncHandler());
                    switch (dri.getOpCode()) {
                        case INSERT:
                            DelayedReplicationInsertInfo ii = (DelayedReplicationInsertInfo) dri;
                            _cacheManager.handleInsertEntryReplication(context, dri.getEntry(), _blobstoreBulkId);
                            break;
                        case UPDATE:
                            DelayedReplicationUpdateInfo ui = (DelayedReplicationUpdateInfo) dri;
                            _cacheManager.handleUpdateEntryReplication(context, ui.getEntry(), ui.getOriginalData(), ui.getMutators(), _blobstoreBulkId);
                            break;
                        case REMOVE:
                            DelayedReplicationRemoveInfo ri = (DelayedReplicationRemoveInfo) dri;
                            _cacheManager.handleRemoveEntryReplication(context, ri.getEntry(), ri.getRemoveReason(), _blobstoreBulkId);
                            break;
                    }
                }
            }
            if (t == null && _exceptionOccured != null)
                t = _exceptionOccured;
            if (t != null)
                throw !(t instanceof BlobStoreException) ? new BlobStoreException(t) : (BlobStoreException) t;
            afterBulkFlush(context, termination);
        } catch (Throwable t) {
            _logger.log(Level.SEVERE, getClass().getName() + " blobstore:execute-bulk ", t);
            BlobStoreException ex = (t instanceof BlobStoreException) ? (BlobStoreException) t : (new BlobStoreException(t));
            if (getException() == null)
                setExecption(t);
            _exceptionOccured = t;
            onFlushError(context);
            throw ex;
        }
    }

    private void afterBulkFlush(Context context, boolean termination) {
        synchronized (_lock) {
            if (_exceptionOccured == null) {
                List<IEntryCacheInfo> unpined = null;
                //for each entry disconnect from the bulk + remove from memory if possible
                for (BulkOpHolder boh : _ops) {
                    if (boh.isDiscarded())
                        continue;
                    //NOTE- the currently handled entry is not unpinned, it will be in CacheManeger before
                    // released from its lock
                    boh.getBlobStoreRefEntryCacheInfo().bulkUnRegister(_cacheManager);
                    if (boh.getBlobStoreRefEntryCacheInfo().isDeleted())
                        continue;
                    if (unpined == null)
                        unpined = new LinkedList<IEntryCacheInfo>();
                    unpined.add(boh.getBlobStoreRefEntryCacheInfo());
                }
                if (unpined != null)
                    _cacheManager.getEngine().getProcessorWG().enqueueBlocked(new BlobStoreUnpinMultiplePacket(unpined));

                _uids.clear();
                _ops.clear();
                clearBulkId();
            }
            if (termination)
                _terminated = true;
            _lastNotificationStamp++;
            _lock.notifyAll();
        }
    }

    private void onFlushError(Context context) {
        //revert all ops
        synchronized (_lock) {
            try {
                _terminated = true;
                if (_previousStates == null)
                    return;
                List<IEntryCacheInfo> unpined = null;
                //for each entry disconnect from the bulk + remove from memory if possible
                for (BulkOpHolder boh : _ops) {
                    if (boh.isDiscarded())
                        continue;

                    if (unpined == null)
                        unpined = new LinkedList<IEntryCacheInfo>();
                    unpined.add(boh.getBlobStoreRefEntryCacheInfo());
                    BlobStoreErrorBulkEntryInfo.setOnContext(context, _previousStates.get(boh.getBlobStoreRefEntryCacheInfo().getUID()));

                    switch (boh.getOperation()) {
                        case SpaceOperations.WRITE:
                            BlobStoreErrorsHandler.onFailedWrite(_cacheManager, context, boh.getBlobStoreRefEntryCacheInfo(), boh.getBlobStoreRefEntryCacheInfo().getEntryHolder());
                            break;
                        case SpaceOperations.UPDATE:
                            BlobStoreErrorsHandler.onFailedUpdate(_cacheManager, context, boh.getBlobStoreRefEntryCacheInfo(), boh.getBlobStoreRefEntryCacheInfo().getEntryHolder());
                            break;
                        case SpaceOperations.TAKE:
                            BlobStoreErrorsHandler.onFailedRemove(_cacheManager, context, boh.getBlobStoreRefEntryCacheInfo(), boh.getBlobStoreRefEntryCacheInfo().getEntryHolder());
                            break;
                    }
                    boh.getBlobStoreRefEntryCacheInfo().bulkUnRegister(_cacheManager);
                }
                if (unpined != null)
                    _cacheManager.getEngine().getProcessorWG().enqueueBlocked(new BlobStoreUnpinMultiplePacket(unpined));
            } catch (Throwable ex) {
                if (_logger.isLoggable(Level.SEVERE)) {
                    _logger.log(Level.SEVERE, "failure during handling flush error", ex);
                }
            } finally {
                if (_uids != null) {
                    _uids.clear();
                }
                if (_ops != null) {
                    _ops.clear();
                }
                clearBulkId();
                _lock.notifyAll();
            }
        }
    }


    public void waitForBulkFlush(BlobStoreRefEntryCacheInfo eci) {
        if (_terminated)
            return;

        synchronized (_lock) {
            if (_terminated)
                return;
            if (_exceptionOccured != null) {
                throw new BlobStoreException("bulk operation failed- a previous exception have been thrown " + _exceptionOccured);
            }
            if (_uids.get(eci.getUID()) == null)
                return;
            int mystamp = 0;
            try {
                mystamp = _lastNotificationStamp;
                long upto = System.currentTimeMillis() + BULK_MAX_WAIT_TIME;
                while (true) {
                    long towait = upto - System.currentTimeMillis();
                    if (towait > 0)
                        _lock.wait(towait);
                    else
                        break;

                    if (mystamp != _lastNotificationStamp)
                        break;
                }
                if (mystamp == _lastNotificationStamp)
                    throw new BlobStoreException("wait for bulk operation: max time expired, uid=" + eci.getUID());

            } catch (InterruptedException e) {
                // restore interrupt state just in case it does get here
                Thread.currentThread().interrupt();
                throw new BlobStoreException(e);
            }
        }
    }

    public Thread getOwnerThread() {
        return _ownerThread;
    }

    public boolean isTakeMultipleBulk() {
        return _takeMultipleBulk;
    }

    public Throwable getException() {
        return _exceptionOccured;
    }

    public void setExecption(Throwable t) {
        _logger.severe(getClass().getName() + " blobstore:set exception [in bulk] " + t);
        if (_exceptionOccured == null)
            _exceptionOccured = t;
    }

    public boolean wasEntryRemovedInChunk(String uid) {
        return (_removedEntries != null) ? _removedEntries.contains(uid) : false;

    }

    public BlobStoreErrorBulkEntryInfo getPerviousStateForEntry(String uid) {
        return _previousStates.get(uid);
    }

    private void generateBulkIdIfNeeded(boolean neededDirectPersistency) {
        if (_blobstoreBulkId == 0 && neededDirectPersistency && _cacheManager.useBlobStoreReplicationBackupBulks()) {
            _blobstoreBulkId = _bulkIdCounter.getAndIncrement();
        }
    }

    private void clearBulkId() {
        _blobstoreBulkId = 0;
    }


    private static class BulkOpHolder {
        private final int _operation;
        private final BlobStoreRefEntryCacheInfo _eci;
        private boolean _discarded;


        BulkOpHolder(int operation, BlobStoreRefEntryCacheInfo eci) {
            _operation = operation;
            _eci = eci;
        }

        int getOperation() {
            return _operation;
        }

        BlobStoreRefEntryCacheInfo getBlobStoreRefEntryCacheInfo() {
            return _eci;
        }

        boolean isDiscarded() {
            return _discarded;
        }

        void setDiscarded() {
            _discarded = true;
        }
    }

}
