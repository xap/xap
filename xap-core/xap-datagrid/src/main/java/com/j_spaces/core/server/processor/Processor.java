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

package com.j_spaces.core.server.processor;

import com.gigaspaces.client.protective.ProtectiveModeException;
import com.gigaspaces.events.GSEventRegistration;
import com.gigaspaces.events.NotifyActionType;
import com.gigaspaces.internal.client.spaceproxy.operations.WriteEntrySpaceOperationResult;
import com.gigaspaces.internal.exceptions.BatchQueryException;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.ChangeInternalException;
import com.gigaspaces.internal.server.space.FifoGroupsSearch;
import com.gigaspaces.internal.server.space.FifoSearch;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.events.NotifyContextsHolder;
import com.gigaspaces.internal.server.space.events.UpdateNotifyContextHolder;
import com.gigaspaces.internal.server.space.operations.WriteEntryResult;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.gigaspaces.logger.LogLevel;
import com.gigaspaces.lrmi.nio.IResponseContext;
import com.gigaspaces.lrmi.nio.ReplyPacket;
import com.gigaspaces.lrmi.nio.ResponseContext;
import com.gigaspaces.server.blobstore.BlobStoreException;
import com.j_spaces.core.AnswerHolder;
import com.j_spaces.core.AnswerPacket;
import com.j_spaces.core.Constants;
import com.j_spaces.core.EntryDeletedException;
import com.j_spaces.core.EntryTakenPacket;
import com.j_spaces.core.ExtendedAnswerHolder;
import com.j_spaces.core.FifoEntriesComparator;
import com.j_spaces.core.FifoException;
import com.j_spaces.core.NoMatchException;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.PendingFifoSearch;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.TemplateDeletedException;
import com.j_spaces.core.TransactionConflictException;
import com.j_spaces.core.TransactionNotActiveException;
import com.j_spaces.core.UpdateOrWriteContext;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.TerminatingFifoXtnsInfo;
import com.j_spaces.core.cache.XtnData;
import com.j_spaces.core.cache.blobStore.IBlobStoreRefCacheInfo;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.blobStore.IBlobStoreEntryHolder;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.client.*;
import com.j_spaces.core.cluster.ReplicationOperationType;
import com.j_spaces.core.fifo.FifoBackgroundDispatcher;
import com.j_spaces.core.fifo.FifoBackgroundRequest;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.core.sadapter.SelectType;
import com.j_spaces.core.transaction.CheckXtnStatusInTmBusPackect;
import com.j_spaces.kernel.IConsumerObject;
import com.j_spaces.kernel.locks.ILockObject;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.core.transaction.server.TransactionConstants;
import net.jini.id.Uuid;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main logic handler inside engine.
 */
@com.gigaspaces.api.InternalApi
public class Processor implements IConsumerObject<BusPacket<Processor>> {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_ENGINE);

    private final SpaceEngine _engine;
    private final CacheManager _cacheManager;
    private final FifoBackgroundDispatcher _fifoBackgroundDispatcher;
    //FIFO compare entry holders
    private final FifoEntriesComparator _comparator;
    //short lease retry count for ALL_IN_CACHE
    private final int _insertShortLeaseRetry;

    public Processor(SpaceEngine engine) {
        this._engine = engine;
        this._cacheManager = engine.getCacheManager();
        this._fifoBackgroundDispatcher = _cacheManager.getFifoBackgroundDispatcher();

        _comparator = new FifoEntriesComparator();

        _insertShortLeaseRetry = engine.getConfigReader().getIntSpaceProperty(
                Constants.Engine.ENGINE_INSERT_SHORT_LEASE_RETRY_PROP,
                Constants.Engine.ENGINE_INSERT_SHORT_LEASE_RETRY_DEFAULT);
        if (_insertShortLeaseRetry < 0)
            throw new RuntimeException("invalid INSERT_SHORT_LEASE_RETRY specified");
    }

    @Override
    public void cleanUp() {
    }

    /**
     * Main event loop. Handles Bus Packets.
     */
    @Override
    public void dispatch(BusPacket<Processor> packet) {
        try {
            if (_logger.isDebugEnabled())
                _logger.debug("Bus packet arrived: " + packet.getClass().getName());

            packet.execute(this);
        } catch (Exception ex) {
            if (_logger.isErrorEnabled())
                _logger.error(ex.toString(), ex);
        }
    }

    /**
     * Handles Direct write SA.
     *
     * if entryHolder.m_NoWriteLease == true, the return value will be String otherwise LeaseProxy
     **/
    public WriteEntryResult handleDirectWriteSA(Context context, IEntryHolder entry, IServerTypeDesc typeDesc, boolean fromReplication,
                                                boolean origin, boolean reInsertedEntry, boolean supplied_uid, boolean lockXtnsIfNeeded, int modifiers)
            throws TransactionException, UnusableEntryException, RemoteException, SAException {
        ILockObject entryLock = null;
        boolean originalMultiOpSyncReplIndicator = context.isSyncReplFromMultipleOperation();

        try {
            entryLock = getEntryLockObject(entry);

            //if its blob store verify memory shortage
            if (entry.isBlobStoreEntry())
                _cacheManager.getBlobStoreMemoryMonitor().onMemoryAllocation(((IBlobStoreEntryHolder) entry).getBlobStoreResidentPart().getStorageKey());

            // sync volatile variable
            _engine.setLastEntryTimestamp(entry.getSCN());
            boolean doNotNotifyListeners = reInsertedEntry;

            IEntryHolder original_eh = null;
            if (!reInsertedEntry)
                original_eh = entry.createCopy();

            context.setNotifyNewEntry(original_eh);
            //lock & insert
            if (lockXtnsIfNeeded && entry.getXidOriginated() != null) {
                // if the operation is multiple - the lock was already acquired.
                if (!context.isTransactionalMultipleOperation())
                    entry.getXidOriginated().lock();
                if (entry.getServerTypeDesc().isFifoSupported())
                    _engine.getTransactionHandler().getTxReadLock().lock();

                try {
                    XtnEntry xtnEntry = entry.getXidOriginated();
                    if (xtnEntry == null || !xtnEntry.m_Active)
                        throw new TransactionException("The transaction is not active: " + entry.getXidOriginatedTransaction());

                    insertToSpaceLoop(context, entry, typeDesc, fromReplication, origin, supplied_uid, entryLock, modifiers);

                } finally {
                    // if the operation is multiple - the lock was already unlocked.
                    if (!context.isTransactionalMultipleOperation())
                        entry.getXidOriginated().unlock();
                    if (entry.getServerTypeDesc().isFifoSupported())
                        _engine.getTransactionHandler().getTxReadLock().unlock();

                    //We may had a lease expired on the currently written entry, this was inserted to the replication backlog and we need
                    //to replicate it now
                    _engine.performReplication(context);
                }
            }// if (lockXtnsIfNeeded && entry.m_XidOriginated !
            else {
                insertToSpaceLoop(context, entry, typeDesc, fromReplication, origin, supplied_uid, entryLock, modifiers);
            }

            // sync volatile variable
            _engine.touchLastEntryTimestamp();

            /**
             * Only if at least one template exists in cache -
             * Inform to all waited templates about new written packet.
             **/
            if (_cacheManager.getTemplatesManager().anyNonNotifyTemplates() ||
                    (_cacheManager.getTemplatesManager().anyNotifyWriteTemplates()) && !context.isBackupOnly()) {
                EntryArrivedPacket ea = _engine.getEntryArrivedPacketsFactory().getPacket(context.getOperationID(), entry,
                        entry.getXidOriginatedTransaction(), !doNotNotifyListeners, original_eh, fromReplication);
                _engine.getProcessorWG().enqueueBlocked(ea);
            }

            return context.getWriteResult();
        } catch (Exception ex) {
            if (!originalMultiOpSyncReplIndicator && context.isSyncReplFromMultipleOperation()) {
                context.setSyncReplFromMultipleOperation(false);
                //lease expiration replication if inside
                _engine.performReplication(context);
            }
            if (ex instanceof SAException)
                throw (SAException) ex;
            if (ex instanceof TransactionException)
                throw (TransactionException) ex;
            if (ex instanceof UnusableEntryException)
                throw (UnusableEntryException) ex;
            if (ex instanceof RemoteException)
                throw (RemoteException) ex;
            if (ex instanceof RuntimeException)
                throw (RuntimeException) ex;
            throw new RuntimeException(ex);
        } finally {
            if (!originalMultiOpSyncReplIndicator && context.isSyncReplFromMultipleOperation())
                context.setSyncReplFromMultipleOperation(false);
            //FIFO
            context.resetRecentFifoObject();
            //FIFO------------------------------------------------

            if (entryLock != null)
                freeEntryLockObject(entryLock);
        }
    }

    private boolean needReadBeforeWriteToSpace(Context context) {
        return (_cacheManager.isTieredStorage() && context.getEntryTieredState() != TieredState.TIERED_COLD )|| (_cacheManager.isEvictableCachePolicy() && !_cacheManager.isMemorySpace());
    }

    private void insertToSpaceLoop(Context context, IEntryHolder entry, IServerTypeDesc typeDesc, boolean fromReplication, boolean origin,
                                   boolean supplied_uid, ILockObject entryLock, int modifiers) throws TransactionException, UnusableEntryException,
            RemoteException, SAException {
        int shortLeaseRetry = _insertShortLeaseRetry;

        while (true) {
            try {
                //insert the entry
                insertEntryToSpace(context, entry, typeDesc, fromReplication, origin,
                        supplied_uid, entryLock, modifiers);
                break;
            } catch (EntryAlreadyInSpaceException ex_) {
                //got EntryAlreadyInSpaceException . if we need to establish a new lock
                //(because the object itself is also the lock object)-
                //in order to remove the expitred entry- we will try it
                if (supplied_uid && _cacheManager.getLockManager().isPerLogicalSubjectLockObject(_cacheManager.isEvictableCachePolicy()
                        || _cacheManager.isTieredStorage()) && !_engine.isExpiredEntryStayInSpace(entry) && --shortLeaseRetry >= 0) {
                    if (_engine.isSyncReplicationEnabled() && _engine.getLeaseManager().replicateLeaseExpirationEventsForEntries())
                        context.setSyncReplFromMultipleOperation(true); //piggyback the lease expiration replication
                    if (leaseExpiredInInsertWithSameUid(context, entry, typeDesc,
                            null /* existingEntry*/, false /*alreadyLocked*/))
                        //retry to insert
                        continue;
                }
                throw ex_;
            }
        }//while(true)
    }


    private void insertEntryToSpace(Context context, IEntryHolder entry, IServerTypeDesc typeDesc, boolean fromReplication, boolean origin,
                                    boolean supplied_uid, ILockObject entryLock, int modifiers)
            throws TransactionException, UnusableEntryException,
            RemoteException, SAException {
        boolean alreadyIn = false;
        boolean ignoreUnpin = false;

        synchronized (entryLock) {
            try {
                if (supplied_uid && needReadBeforeWriteToSpace(context))
                //uid externally supplied we make a DB search -verify that entry is not alrady in
                {
                    IEntryHolder curEh = _cacheManager.getEntry(context, entry, !_cacheManager.isTieredStorage() /*tryInsertToCache*/, true /*lockeEntry*/,
                            (fromReplication &&
                                    _cacheManager.requiresEvictionReplicationProtection()) ||
                                    entry.isTransient() || UpdateModifiers.isMemoryOnlySearch(modifiers)/* memory only read */ || _cacheManager.isTieredStorage());
                    if (curEh != null && !_cacheManager.getLockManager().isPerLogicalSubjectLockObject(_cacheManager.isEvictableCachePolicy() || _cacheManager.isTieredStorage())) {//under same lock we can try and remove the existing entry if its lease expired
                        if (_engine.isSyncReplicationEnabled() && _engine.getLeaseManager().replicateLeaseExpirationEventsForEntries())
                            context.setSyncReplFromMultipleOperation(true); //piggyback the lease expiration replication

                        if (_engine.isExpiredEntryStayInSpace(entry) || !leaseExpiredInInsertWithSameUid(context, entry, typeDesc, curEh, true /*alreadyLocked*/)) {
                            alreadyIn = true;
                            throw new EntryAlreadyInSpaceException(entry.getUID(), entry.getClassName());
                        }
                    }
                }

                boolean shouldReplicate = false;
                if (_engine.isReplicated())
                    shouldReplicate = _engine.shouldReplicate(ReplicationOperationType.WRITE, typeDesc, false, fromReplication);

                if(Modifiers.contains(modifiers,Modifiers.BACKUP_ONLY)){
                    context.setBackupOnly();
                }
                try {
                    _cacheManager.insertEntry(context, entry, shouldReplicate, origin, supplied_uid);
                } catch (EntryAlreadyInSpaceException ex_) {
                    alreadyIn = true;
                    //try to remove the existing entry if expired-if we can use this lock
                    if (!_cacheManager.getLockManager().isPerLogicalSubjectLockObject(_cacheManager.isEvictableCachePolicy() || _cacheManager.isTieredStorage())) {
                        if (_engine.isSyncReplicationEnabled() && _engine.getLeaseManager().replicateLeaseExpirationEventsForEntries())
                            context.setSyncReplFromMultipleOperation(true); //piggyback thre lease expiration replication
                        if (_engine.isExpiredEntryStayInSpace(entry) || !leaseExpiredInInsertWithSameUid(context, entry, typeDesc,
                                null, true /*alreadyLocked*/))
                            throw ex_;
                        _cacheManager.insertEntry(context, entry, shouldReplicate, origin, (supplied_uid && _cacheManager.isMemorySpace()));
                        alreadyIn = false;

                    } else
                        throw ex_;
                }

                //make sure uid is erased from recent deletes in case of re-inserts
                if (supplied_uid && _cacheManager.useRecentDeletes()) {
                    if (_cacheManager.removeFromRecentDeletes(entry))
                        //insert UID of entry to hash of updated entries to protect against
                        //the case of an open iterator with the old entry inside
                        _cacheManager.insertToRecentUpdatesIfNeeded(entry, _cacheManager.requiresEvictionReplicationProtection() ? Long.MAX_VALUE : 0, entry.getXidOriginatedTransaction());

                }
                //FIFO do we need to activate FIFO search as well ?+++++++++++++++++++++++++++++++++++++++++++++++=
                FifoBackgroundRequest fb = context.getRecentFifoObject();
                if (fb != null) {//FIFO templates, insert finished, activate the fifo serach
                    fb.setTime(entry.getSCN());
                    //insert & activate a fifo search command
                    _fifoBackgroundDispatcher.positionAndActivateRequest(fb);

                    context.resetRecentFifoObject();
                }
                //FIFO------------------------------------------------

            } catch (BlobStoreException e) {
                ignoreUnpin = !context.isFromReplication() && !entry.isDeleted();
                throw e;
            } finally {
                if (!ignoreUnpin)
                    //while entry still locked
                    if (!alreadyIn && _cacheManager.mayNeedEntriesUnpinning() && entry.getXidOriginated() == null)
                        _cacheManager.unpinIfNeeded(context, entry, null /*template*/, null /* pEntry*/);
            }
        }//synchronized(entryLock)
    }

    //during insert we found an entry with same uid, if its lease is expired
    // remove it returns true if entry can be reinserted
    private boolean leaseExpiredInInsertWithSameUid(Context context, IEntryHolder entry, IServerTypeDesc typeDesc,
                                                    IEntryHolder existingEntry, boolean alreadyLocked)
            throws SAException {
        IEntryHolder curEh = existingEntry;
        ILockObject entryLock = null;

        if (curEh == null && (_cacheManager.getLockManager().isPerLogicalSubjectLockObject(_cacheManager.isTieredStorage() || _cacheManager.isEvictableCachePolicy()) || alreadyLocked)) {
            if (_cacheManager.isblobStoreDataSpace())
                curEh = _cacheManager.getEntryByUidFromPureCache(entry.getUID());
            else
                curEh = _cacheManager.getEntry(context, entry, true /*tryInsertToCache*/, alreadyLocked);
            if (curEh == null)
                return true;  //entry no longer exist
        }

        if (alreadyLocked)
            return removeEntryOnLeaseExpiration(context, typeDesc, curEh);
        //lock
        try {
            entryLock = (_cacheManager.getLockManager().isPerLogicalSubjectLockObject(_cacheManager.isTieredStorage() || _cacheManager.isEvictableCachePolicy()))
                    ? getEntryLockObject(curEh) : getEntryLockObject(entry);
            synchronized (entryLock) {
                if (curEh == null || _cacheManager.needReReadAfterEntryLock()) {//evictable- reget the entry under lock
                    IEntryHolder original = curEh;
                    curEh = _cacheManager.getEntry(context, curEh != null ? curEh : entry, true /*tryInsertToCache*/, true /*lockeEntry*/);
                    if (curEh == null)
                        return true;  //entry no longer exist
                    if (curEh.isBlobStoreEntry() && !curEh.isSameEntryInstance(original))
                        return false;
                }
                return removeEntryOnLeaseExpiration(context, typeDesc, curEh);
            }
        } finally {
            if (entryLock != null)
                freeEntryLockObject(entryLock);
        }
    }

    private boolean removeEntryOnLeaseExpiration(Context context,
                                                 IServerTypeDesc typeDesc, IEntryHolder curEh) throws SAException {
        OperationID opId = context.getOperationID();
        try {
            if (curEh.isExpired() && !_engine.getLeaseManager().isSlaveLeaseManagerForEntries() && (!_engine.getLeaseManager().isNoReapUnderXtnLeases() || !curEh.isEntryUnderWriteLockXtn()))
            //if its a case of expired lease - just remove  the entry
            {
                boolean considerExpirationReplication = !context.isFromReplication() && _engine.getLeaseManager().replicateLeaseExpirationEventsForEntries();
                if (!curEh.isDeleted()) {
                    if (considerExpirationReplication)
                        context.setOperationID(_engine.getLeaseManager().createOperationIDForLeaseExpirationEvent());
                    _engine.removeEntrySA(context, curEh, typeDesc,
                            false /*fromRepl*/, true /*origin*/,
                            SpaceEngine.EntryRemoveReasonCodes.LEASE_EXPIRED /*fromLeaseExpiration*/,
                            !considerExpirationReplication/*disableReplication*/, false /*disableProcessorCall*/, false /*disableSADelete*/);
                }
                return true;  //reinsert
            }
            //no need to remove
            if (_cacheManager.mayNeedEntriesUnpinning())
                _cacheManager.unpinIfNeeded(context, curEh, null /*template*/, null /* pEntry*/);
        } finally {
            if (context.getOperationID() != opId)
                context.setOperationID(opId);
        }

        return false;
    }

    public void handleUpdateOrWrite(IResponseContext respContext,
                                    UpdateOrWriteContext ctx,
                                    ReplyPacket<?> respPacket) {
        if (!ctx.hasConcentratingTemplate())
            ResponseContext.setExistingResponseContext(respContext);
        if (ctx.hasConcentratingTemplate() || respContext.isInvokedFromNewRouter())
            newHandleUpdateOrWrite(respContext, ctx);
        else
            oldHandleUpdateOrWrite(respContext, ctx, (ReplyPacket<AnswerPacket>) respPacket);
        ResponseContext.clearResponseContext();
    }

    private void oldHandleUpdateOrWrite(IResponseContext respContext,
                                        UpdateOrWriteContext ctx, ReplyPacket<AnswerPacket> respPacket) {
        Exception ex = null;
        AnswerPacket resp = respPacket.getResult();

        try {
            ctx.isUpdateOperation = false;
            ExtendedAnswerHolder holder = _engine.updateOrWrite(ctx,
                    respContext.isInvokedFromNewRouter());
            resp = holder != null ? holder.m_AnswerPacket : null;
        } catch (Exception e) {
            ex = e;
        }

        if (respContext.shouldSendResponse()) {
            respContext.sendResponse(resp, ex);
        }
    }

    private void newHandleUpdateOrWrite(IResponseContext respContext, UpdateOrWriteContext ctx) {
        Exception ex = null;
        AnswerPacket resp = null;

        try {
            ctx.isUpdateOperation = false;
            resp = _engine.updateOrWrite(ctx, respContext == null || respContext.isInvokedFromNewRouter()).m_AnswerPacket;
        } catch (Exception e) {
            ex = e;
        }

        if (!ctx.hasConcentratingTemplate() && respContext != null && respContext.shouldSendResponse()) {
            if (respContext.isInvokedFromNewRouter()) {
                WriteEntryResult result = resp != null ? resp.getWriteEntryResult() : null;
                respContext.sendResponse(new WriteEntrySpaceOperationResult(result, ex), null);
            } else {
                respContext.sendResponse(resp, ex);
            }
        }
    }

    /**
     * perform update given a template which is the updated entry.
     */
    public void handleDirectUpdateSA(Context context, final ITemplateHolder template) {
        ILockObject templateLock = null;

        try {
            boolean makeWaitForInfo = (template.getExpirationTime() != 0);
            /* template is already stable */
            IEntryHolder entry = null;
            try {
                if (makeWaitForInfo)
                    template.setInitialIfExistSearchActive();
                try {
                    entry = _engine.getMatchedEntryAndOperateSA(context, template,
                            makeWaitForInfo, false/*useSCN*/);
                } finally {
                    if (makeWaitForInfo)
                        template.resetInitialIfExistSearchActive();
                }
            } catch (TemplateDeletedException ex) {
                /* the template is deleted. Whoever deleted template notified receiver. */
                return;
            }
            if (entry != null || template.isDeleted()) // operation succeeded  or already returned an answer
            {
                return; /* operation succeeded */
            }
            if ((template.getExpirationTime() == 0)) {
                //NoEntryInSpaceException is handled in the engine
                context.setOperationAnswer(template, null, null);
                return;
            }

            if (templateLock == null)
                templateLock = getTemplateLockObject(template);
            synchronized (templateLock) {

                if (template.hasAnswer())
                    return; //answer already received, return

                if (!template.isHasWaitingFor()) { /* template  not waiting for anybody */
                    //NoEntryInSpaceException is handled in the engine
                    context.setOperationAnswer(template, null, null);
                    if (!template.isDeleted() && template.isInCache()) {
                        _cacheManager.removeTemplate(context, template, false, true /*origin*/, false);
                    }
                    return;
                }
            }

        } catch (Exception ex) {
            if (_logger.isErrorEnabled()) {
                _logger.error("Error handling update.", ex);
            }
            if (templateLock == null)
                templateLock = getTemplateLockObject(template);
            synchronized (templateLock) {
                if (!template.hasAnswer())
                    context.setOperationAnswer(template, null, ex);
                if (!template.isDeleted() && template.isInCache())
                    _cacheManager.removeTemplate(context, template, false, true /*origin*/, false);
            }
        } finally {
            if (templateLock != null)
                freeTemplateLockObject(templateLock);
        }
    }

    /**
     *
     * @param packet
     * @throws SAException
     * @throws EntryDeletedException
     */
    public void entryUpdatedSA(EntryUpdatedPacket packet) throws EntryDeletedException, SAException {
        Context context = null;
        IEntryHolder entry = packet.getEntryHolder();

        try {
            context = _cacheManager.getCacheContext();
            context.setOperationID(packet.getOperationID());
            context.setOperationVisibilityTime(packet.getCreationTime());
            ServerTransaction txn = packet.getTransaction();

            if (txn == null && !context.isBackupOnly() && (
                    _cacheManager.getTemplatesManager().anyNotifyUpdateTemplates()
                            || _cacheManager.getTemplatesManager().anyNotifyMatchedTemplates()
                            || _cacheManager.getTemplatesManager().anyNotifyRematchedTemplates())
                    )
                try {
                    context.setFromReplication(packet.isFromReplication());
                    if (txn == null) {
                        NotifyContextsHolder notifyContextsHolder = new UpdateNotifyContextHolder(packet.getOriginalEntryHolder(), packet.getNotifyEH()
                                , context.getOperationID(), packet.isNotifyMatched(), packet.isNotifyRematched());
                        _engine.getTemplateScanner().scanNotifyTemplates(notifyContextsHolder, context, null /* txn */, FifoSearch.NO);
                    }
                } finally {
                    // reset the fromReplication indication.
                    context.setFromReplication(false);
                }

            try {
                _engine.getTemplateScanner().scanNonNotifyTemplates(context, entry, txn, FifoSearch.NO);
            } catch (EntryDeletedException ex) {
                /* do nothing - no point in continuing to try other templates */
            }
        } finally {
            _cacheManager.freeCacheContext(context);
        }
    }

    /**
     * Handles overridden packets.
     */
    public void entryUnmatchedSA(EntryUnmatchedPacket packet) throws SAException, EntryDeletedException {

        Context context = null;

        try {
            context = _cacheManager.getCacheContext();
            context.setOperationID(packet.getOperationID());
            context.setOperationVisibilityTime(packet.getCreationTime());

            if (!_cacheManager.getTemplatesManager().anyNotifyUnmatchedTemplates())
                return;

            try {
                context.setFromReplication(packet.isFromReplication());
                NotifyContextsHolder notifyContextHolder = new NotifyContextsHolder(packet.getEntryHolder(), packet.getNewEntryHolder(), context.getOperationID(), NotifyActionType.NOTIFY_UNMATCHED);
                _engine.getTemplateScanner().scanNotifyTemplates(notifyContextHolder, context, null /*txn */, FifoSearch.NO);
            } finally {
                // reset the fromReplication indication.
                context.setFromReplication(false);
            }
        } finally {
            _cacheManager.freeCacheContext(context);
        }
    }

    /**
     * Handles EntryArrived packets.
     */
    public void entryArrivedSA(EntryArrivedPacket packet)
            throws SAException, EntryDeletedException {
        Context context = null;
        IEntryHolder entry = packet.getEntryHolder();

        try {
            context = _cacheManager.getCacheContext();
            context.setOperationID(packet.getOperationID());
            context.setOperationVisibilityTime(packet.getCreationTime());
            ServerTransaction txn = packet.getTransaction();

            if (packet.shouldNotifyListeners() && _cacheManager.getTemplatesManager().anyNotifyWriteTemplates() && txn == null) {
                try {
                    context.setFromReplication(packet.isFromReplication());

                    IEntryHolder entryValueToNotify = packet.getEntryValueToNotify();
                    NotifyContextsHolder notifyContextHolder = new NotifyContextsHolder(null, entryValueToNotify, context.getOperationID(), NotifyActionType.NOTIFY_WRITE);
                    _engine.getTemplateScanner().scanNotifyTemplates(notifyContextHolder, context, null /* txn */, FifoSearch.NO);
                } finally {
                    // reset the fromReplication indication.
                    context.setFromReplication(false);
                }
            }

            try {
                _engine.getTemplateScanner().scanNonNotifyTemplates(context, entry,
                        txn, FifoSearch.NO);
            } catch (EntryDeletedException ex) {
                /* do nothing - no point in continuing to try other templates */
            }
        } finally {
            _cacheManager.freeCacheContext(context);
            _engine.getEntryArrivedPacketsFactory().freePacket(packet);
        }
    }


    /**
     * Handles EntryTaked packets.
     */
    public void handleEntryTakenSA(EntryTakenPacket packet)
            throws Exception {
        Context context = null;
        IEntryHolder entry = packet.getEntryHolder();
        boolean anyNotityTakeTemplates = _cacheManager.getTemplatesManager().anyNotifyTakeTemplates();
        if (!anyNotityTakeTemplates)
            return;  // nothing to do- no notify-takes

        try {
            context = _cacheManager.getCacheContext();
            if(context.isBackupOnly()){
                return;
            }
            context.setFromReplication(packet.isFromReplication());
            context.setOperationID(packet.getOperationID());
            context.setOperationVisibilityTime(packet.getCreationTime());
            NotifyContextsHolder notifyContextHolder = new NotifyContextsHolder(packet.m_EntryValueToNotify, null, context.getOperationID(), NotifyActionType.NOTIFY_TAKE);
            _engine.getTemplateScanner().scanNotifyTemplates(notifyContextHolder, context, null /* txn */, FifoSearch.NO);
        } finally {
            _cacheManager.freeCacheContext(context);
        }
    }


    /**
     * Handles EntryExpired packets.
     */
    public void handleEntryExpiredSA(EntryExpiredBusPacket packet) throws Exception {
        handleEntryExpiredCoreSA(packet.getEntryHolder(), packet.getTransaction(), packet.isFromReplication());
    }

    /**
     * Handles EntryExpired packets.
     */
    public void handleEntryExpiredCoreSA(IEntryHolder entry, ServerTransaction txn, boolean fromReplication) throws Exception {
        boolean anyNotityExpiredTemplates = _cacheManager.getTemplatesManager().anyNotifyLeaseTemplates();
        if (!anyNotityExpiredTemplates)
            return;  // nothing to do- no notify-expired

        Context context = null;
        try {
            context = _cacheManager.getCacheContext();
            context.setFromReplication(fromReplication);
            NotifyContextsHolder notifyContextsHolder = new NotifyContextsHolder(null, entry, context.getOperationID(), NotifyActionType.NOTIFY_LEASE_EXPIRATION);
            _engine.getTemplateScanner().scanNotifyTemplates(notifyContextsHolder, context, null /* txn */, FifoSearch.NO);

        } finally {
            _cacheManager.freeCacheContext(context);
        }

    }

    public void handleDirectReadOrTakeSA(Context context, final ITemplateHolder template, boolean fromReplication, boolean origin) {
        handleDirectReadTakeOrIPUpdateSA(context, template, fromReplication, origin);
    }


    private void handleDirectReadTakeOrIPUpdateSA(Context context, final ITemplateHolder template, boolean fromReplication, boolean origin) {
        ILockObject templateLock = null;

        try {

            context.setFromReplication(fromReplication);
            context.setOrigin(origin);
            context.setTemplateInitialSearchThread();
            IEntryHolder entry = null;
            try {
                if (template.isFifoSearch()) {
                    template.setInitialFifoSearchActive();
                    //set the scan fifo xtn number
                    template.setFifoXtnNumberOnSearchStart(_cacheManager.getLatestTTransactionTerminationNum());
                }
                entry = _engine.getMatchedEntryAndOperateSA(context, template,
                        false/*makeWaitForInfo*/, false/*useSCN*/);

            } catch (TemplateDeletedException ex) {
                return; /* handled in commit and abort*/
            }


            if (entry == null) {
                if (template.getExpirationTime() == 0) {
                    context.setOperationAnswer(template, null, null);
                    return;
                }
                if (template.isExpired()) {
                    context.setOperationAnswer(template, null, null);
                    return;
                }
                /* handle inserting template into engine */
                if (template.isFifoSearch()) {//create a fifo block object in order to accumulate incoming
                    //entry events
                    template.setPendingFifoSearchObject(new PendingFifoSearch((_cacheManager.isMemorySpace() || _cacheManager.isResidentEntriesCachePolicy())));
                    template.resetFifoXtnNumberOnSearchStart();
                }

                if (template.getXidOriginated() != null) {
                    template.getXidOriginated().lock();

                    try {
                        XtnEntry xtnEntry = template.getXidOriginated();
                        if (xtnEntry == null || !xtnEntry.m_Active) {
                            context.setOperationAnswer(template, null,
                                    new TransactionException("The transaction is not active: " +
                                            template.getXidOriginatedTransaction()));
                            return;
                        }
                        if (templateLock == null)
                            templateLock = getTemplateLockObject(template);
                        synchronized (templateLock) {
                            _cacheManager.insertTemplate(context, template, false /*updateRedoLog*/);

                        }
                    } finally {
                        template.getXidOriginated().unlock();
                    }
                } else { /* template is not under Xtn */
                    if (templateLock == null)
                        templateLock = getTemplateLockObject(template);
                    synchronized (templateLock) {
                        _cacheManager.insertTemplate(context, template, false /*updateRedoLog*/);
                    }
                } /* finished handle inserting template into engine */

                try {
                    boolean needSecondSearch = false;

                    // sync volatile variable
                    if (_engine.getLastEntryTimestamp() >= template.getSCN())
                        needSecondSearch = true;

                    if (needSecondSearch) {
                        if (template.isFifoSearch())
                            //set the scan fifo xtn number
                            template.setFifoXtnNumberOnSearchStart(_cacheManager.getLatestTTransactionTerminationNum());
                        entry = _engine.getMatchedEntryAndOperateSA(context, template,
                                false/*makeWaitForInfo*/, false/*useSCN = false */);
                    }

                    if (template.isFifoSearch() && entry == null && template.isInCache() && !template.isDeleted() && template.getPendingFifoSearchObject() != null) {
                        //entries may be fifo-blocked on this template, enable it to be processed by recentFifoEntries
                        if (templateLock == null)
                            templateLock = getTemplateLockObject(template);
                        synchronized (templateLock) {
                            if (template.getPendingFifoSearchObject() != null && template.getPendingFifoSearchObject().anyRejectedEntries() && !template.isDeleted()) {
                                //NOTE-pending FIFO search is disabled by the RecentFifoEntries when the template is processed
                                FifoBackgroundRequest red = new FifoBackgroundRequest(
                                        context.getOperationID(),
                                        false/*isNotifyRequest*/,
                                        true/*isNonNotifyRequest*/,
                                        null,
                                        null,
                                        fromReplication, template);

                                _fifoBackgroundDispatcher.positionAndActivateRequest(red);

                            } else {
                                template.removePendingFifoSearchObject(true /*disableInitialSearch*/); //disable rejecting
                            }
                        }

                    }
                } catch (TemplateDeletedException ex) {
                    /* do nothing - whoever deleted this template notified receiver */
                }
            } else { /* entry != null */
                /* do nothing, because performTemplateOnEntrySA notified Receiver */
            }
        } catch (Exception ex) {
            LogLevel level = LogLevel.SEVERE;
            if (ex instanceof ProtectiveModeException)
                level = LogLevel.DEBUG;
            else if (ex instanceof ChangeInternalException) {
                ex = ((ChangeInternalException) ex).getInternalException();
                level = LogLevel.DEBUG;
            }
            // Log internal error...
            if (level.isEnabled(_logger)) {
                level.log(_logger, "Error handling read/take/change.", ex);
            }

            if (templateLock == null)
                templateLock = getTemplateLockObject(template);
            synchronized (templateLock) {
                if (!template.hasAnswer())
                    context.setOperationAnswer(template, null, ex);
                if (template.isInCache() && !template.isDeleted()) {
                    try {
                        _cacheManager.removeTemplate(context,
                                template,
                                false /* updateRedoLog */,
                                true /* origin */,
                                false);
                    } catch (Exception e) {
                        // Log internal error...
                        if (_logger.isErrorEnabled()) {
                            _logger.error(
                                    "Internal Error failed to removeTemplate: ",
                                    ex);
                        }
                    }
                }
            }
        } finally {
            if (templateLock != null)
                freeTemplateLockObject(templateLock);
        }
    }


    /**
     * @param template
     * @return
     */
    private ILockObject getTemplateLockObject(ITemplateHolder template) {
        return _cacheManager.getLockManager().getLockObject(template, false /*isEvictable*/);
    }

    private void freeTemplateLockObject(ILockObject lockObject) {
        _cacheManager.getLockManager().freeLockObject(lockObject);
    }

    public void handleDirectChangeSA(Context context, final ITemplateHolder template, boolean fromReplication, boolean origin) {
        handleDirectReadTakeOrChangeIESA(context, template, fromReplication, origin);
    }


    public void handleDirectReadIEOrTakeIESA(Context context, final ITemplateHolder template, boolean fromReplication, boolean origin) {
        handleDirectReadTakeOrChangeIESA(context, template, fromReplication, origin);
    }

    private void handleDirectReadTakeOrChangeIESA(Context context, final ITemplateHolder template, boolean fromReplication, boolean origin) {
        if (template.isChange())
            template.setIfExistForChange();

        ILockObject templateLock = null;

        try {

            context.setFromReplication(fromReplication);
            context.setOrigin(origin);
            context.setTemplateInitialSearchThread();
            long originalExpiration = template.getExpirationTime();
            if (originalExpiration != 0)
                template.setInitialIfExistSearchActive();
            IEntryHolder entry = null;
            try {
                if (template.isFifoSearch()) {
                    template.setInitialFifoSearchActive();
                    //set the scan fifo xtn number
                    template.setFifoXtnNumberOnSearchStart(_cacheManager.getLatestTTransactionTerminationNum());
                }
                //make it a non-blocking-read if possible- NBR cannot be set forif-exist with timeout
                if (originalExpiration != 0 && template.isReadOperation() && !template.isNonBlockingRead()) {
                    template.setExpirationTime(0);
                    template.setNonBlockingRead(_engine.isNonBlockingReadForOperation(template));
                }
                context.setPossibleIEBlockingMatch(false);
                entry = _engine.getMatchedEntryAndOperateSA(context, template,
                        false/*makeWaitForInfo*/, false/*useSCN*/);

            } catch (TemplateDeletedException ex) {
                return; /* cannot happen */
            }

            if (entry == null) {
                if (originalExpiration != template.getExpirationTime()) {
                    template.setExpirationTime(originalExpiration);
                    template.setNonBlockingRead(_engine.isNonBlockingReadForOperation(template));

                }
                if (template.getExpirationTime() == 0 || template.isExpired() || !context.isPossibleIEBlockingMatch()) {
                    if (!template.hasAnswer())
                        context.setOperationAnswer(template, null, null);
                    return;
                }
                if (template.isFifoSearch()) {//create a fifo block object in order to accumulate incoming
                    //entry events
                    template.setPendingFifoSearchObject(new PendingFifoSearch((_cacheManager.isMemorySpace() || _cacheManager.isResidentEntriesCachePolicy())));
                    template.resetFifoXtnNumberOnSearchStart();
                }


                try {
                    boolean needSecondSearch = _engine.getLastEntryTimestamp() >= template.getSCN() || context.isPossibleIEBlockingMatch();


                    if (needSecondSearch) {
                        template.setRejectedOpOriginalExceptionAndEntry(null, null);
                      /* handle inserting template into engine */
                        templateLock = getTemplateLockObject(template);
                        if (template.getXidOriginated() != null) {
                            template.getXidOriginated().lock();

                            try {
                                XtnEntry xtnEntry = template.getXidOriginated();
                                if (xtnEntry == null || !xtnEntry.m_Active) {
                                    context.setOperationAnswer(template, null,
                                            new TransactionException("The transaction is not active: " +
                                                    template.getXidOriginatedTransaction()));
                                    return;
                                }
                                synchronized (templateLock) {
                                    _cacheManager.insertTemplate(context, template, false /*updateRedoLog*/);

                                }
                            } finally {
                                template.getXidOriginated().unlock();
                            }
                        } else { /* template is not under Xtn */
                            synchronized (templateLock) {
                                _cacheManager.insertTemplate(context, template, false /*updateRedoLog*/);
                            }
                        } /* finished handle inserting template into engine */

                        if (template.isFifoSearch())
                            //set the scan fifo xtn number
                            template.setFifoXtnNumberOnSearchStart(_cacheManager.getLatestTTransactionTerminationNum());


                        entry = _engine.getMatchedEntryAndOperateSA(context, template,
                                true /*makeWaitForInfo*/, false/*useSCN = false */);
                    }


                    if (entry == null && template.isFifoSearch() && template.isInCache() && !template.isDeleted() && template.getPendingFifoSearchObject() != null) {
                        //entries may be fifo-blocked on this template, enable it to be processed by recentFifoEntries
                        PendingFifoSearch pfs = null;
                        synchronized (templateLock) {
                            if (template.getPendingFifoSearchObject().anyRejectedEntries() && !template.isDeleted()) {
                                //NOTE!!!!
                                //disabling the pending fifo search object is done by the RecentFifoEntries thread
                                pfs = template.getPendingFifoSearchObject();
                                FifoBackgroundRequest red = new FifoBackgroundRequest(
                                        context.getOperationID(),
                                        false/*isNotifyRequest*/,
                                        true/*isNonNotifyRequest*/,
                                        null,
                                        null,
                                        fromReplication,
                                        template);

                                _fifoBackgroundDispatcher.positionAndActivateRequest(red);

                            } else {
                                template.removePendingFifoSearchObject(true /*disableInitialSearch*/); //disable rejecting
                            }
                        }//synchronized(templateLock)
                        //wait for the rejected entries to be processed
                        if (pfs != null)
                            pfs.waitForNonActiveStatus();
                    }
                    //any need to keep on with this template ?
                    templateLock = getTemplateLockObject(template);
                    synchronized (templateLock) {
                        if (template.hasAnswer())
                            return; //answer already received, return
                        template.resetInitialIfExistSearchActive();
                        if (!template.isHasWaitingFor()) { /* template  not waiting for anybody */
                            boolean exceptionIfNoEntry = template.getUidToOperateBy() != null;

                            if (((template.getTemplateOperation() == SpaceOperations.READ_IE)
                                    || (template.getTemplateOperation() == SpaceOperations.TAKE_IE))
                                    && !ReadModifiers.isMatchByID(template.getOperationModifiers())) {
                                exceptionIfNoEntry = false;
                            }

                            if (exceptionIfNoEntry) {//designated entry not in space
                                EntryNotInSpaceException exv = new EntryNotInSpaceException(template.getUidToOperateBy(), _engine.getSpaceName(), false);
                                context.setOperationAnswer(template, null, exv);
                            } else {
                                context.setOperationAnswer(template, null, null);
                            }
                            if (template.isInCache() && !template.isDeleted())
                                _cacheManager.removeTemplate(context, template,
                                        false /*fromReplication*/, true /*origin*/, false);

                            return;
                        }
                        if (template.isExplicitInsertionToExpirationManager() && template.isInCache() && !template.isInExpirationManager())
                            _cacheManager.getTemplateExpirationManager().addTemplate(template);

                    }//synchronized (templateLock)
                } catch (TemplateDeletedException ex) {
                  /* do nothing - whoever deleted this template notified receiver */
                }
            } else { /* entry != null */
  			/* do nothing, because performTemplateOnEntrySA notified Receiver */
            }
        } catch (Exception ex) {
            LogLevel level = LogLevel.SEVERE;
            if (ex instanceof ProtectiveModeException)
                level = LogLevel.DEBUG;
            else if (ex instanceof ChangeInternalException) {
                ex = ((ChangeInternalException) ex).getInternalException();
                level = LogLevel.DEBUG;
            }
            // Log internal error...
            if (level.isEnabled(_logger)) {
                level.log(_logger, "Error handling read/take/change.", ex);
            }

            if (templateLock == null)
                templateLock = getTemplateLockObject(template);
            synchronized (templateLock) {
                if (!template.hasAnswer())
                    context.setOperationAnswer(template, null, ex);
                if (template.isInCache() && !template.isDeleted()) {
                    try {
                        _cacheManager.removeTemplate(context,
                                template,
                                false /* updateRedoLog */,
                                true /* origin */,
                                false);
                    } catch (Exception e) {
                        // Log internal error...
                        if (_logger.isErrorEnabled()) {
                            _logger.error(
                                    "Internal Error failed to removeTemplate: ",
                                    ex);
                        }
                    }
                }
            }
        } finally {
            if (templateLock != null)
                freeTemplateLockObject(templateLock);
        }
    }

    /* handle notify registration request */
    public void handleNotifyRegistration(final NotifyTemplateHolder template,
                                         boolean fromReplication, AnswerHolder answer, OperationID operationID) {
        Context context = null;
        ILockObject templateLock = null;

        try {
            context = _cacheManager.getCacheContext();
            context.setOperationID(operationID);
            templateLock = getTemplateLockObject(template);
            synchronized (templateLock) {
                handleNotifyCoreSA(context, template, fromReplication, answer);
            }
        } finally {
            try {
                _cacheManager.freeCacheContext(context);
            } catch (Exception ex1) {
            }
            if (templateLock != null)
                freeTemplateLockObject(templateLock);
        }
    }

    /**
     * Handles Notify Core.
     */
    private void handleNotifyCoreSA(Context context, NotifyTemplateHolder template,
                                    boolean fromReplication, AnswerHolder answer) {
        boolean shouldReplicate = false;

        if (_engine.isReplicated() && !_engine.getSpaceImpl().isBackup())
            shouldReplicate = _engine.shouldReplicate(ReplicationOperationType.NOTIFY, true /* is true because partial replication is not supported for notify  */, !template.isReplicateNotify(), fromReplication);

        _cacheManager.insertTemplate(context, template, shouldReplicate);

        Uuid spaceUID = _engine.getSpaceImpl().getUuid();
        // create Event Registration in answer and notify answer
        answer.setEventRegistration(new GSEventRegistration(template.getEventId(), null/* source */, context.getNotifyLease(),
                template.getSequenceNumber(), template.getUID(), spaceUID));
    }

    /**
     * Handles commit SA.
     */
    public void handleCommitSA(CommitBusPacket packet) throws SAException {
        Context context = null;
        ISAdapterIterator<ITemplateHolder> iter = null;
        ILockObject templateLock = null, entryLock = null;
        final XtnEntry xtnEntry = packet._xtnEntry;
        final XtnData pXtn = xtnEntry.getXtnData();

        try {
            context = _cacheManager.getCacheContext();
          
        /*----------------- handle Notify Templates: begin -----------------*/
            //  do nothing -
            //     done by the StorageAdapter.commit method which is called from Engine.commit()
        /*----------------- handle Notify Templates: end -----------------*/

        /*----------------- handle Read/Take Templates: begin -----------------*/
            try {
                iter = _cacheManager.makeUnderXtnTemplatesIter(context, xtnEntry);
                // iterate over iter
                if (iter != null) {
                    while (true) {
                        ITemplateHolder template = iter.next();
                        if (template == null)
                            break;
                        try {
                            templateLock = getTemplateLockObject(template);
                            synchronized (templateLock) {
                                if (template.isDeleted())
                                    continue;
                                if (template.isExpired(xtnEntry.m_CommitRollbackTimeStamp)) {
                                    context.setOperationAnswer(template, null, null);
                                    _cacheManager.removeTemplate(context, template,
                                            false /*fromReplication*/, true /*origin*/, false);
                                } else {
                                    context.setOperationAnswer(template,
                                            null, new UnknownTransactionException("Transaction was already committed/aborted."));
                                    _cacheManager.removeTemplate(context, template,
                                            false /*fromReplication*/, true /*origin*/, false);
                                }
                            } /* lock (templateLock) */
                        } /* try */ finally {
                            if (templateLock != null) {
                                freeTemplateLockObject(templateLock);
                                templateLock = null;
                            }
                        }
                    } /* for each template */
                } /* if iter != null */
            } /* try */ finally {
                if (iter != null)
                    iter.close();
            }
        /*----------------- handle Read/Take Templates: end -----------------*/

          /*---------------- handle entries need scan only for f-g templates*/

            _engine.getFifoGroupsHandler().handleNeedFgOnlyScanOnXtnEnd(context, xtnEntry);
          
          /*------------------------------------------------------------------*/
          
          
        /*----------------- handle need-notify Entries: begin -----------------*/
            try {
                context.setFromReplication(xtnEntry.isFromReplication());
                //if update supported- use the "snap image" of the contents
                // of entries that need to be notified- it is taken during
                //"pre-prepare" call
                iter = _cacheManager.makeUnderXtnEntriesIter(context,
                        xtnEntry, SelectType.NEED_NOTIFY_ENTRIES);
                if (iter != null) {
                    while (true) {
                        IEntryHolder entry = iter.next();
                        if (entry == null)
                            break;

                        //note- no need to lock entry its just a snapshot of the original entry
                        if (!_engine.getLeaseManager().isNoReapUnderXtnLeases() && entry.isExpired(_engine.getLeaseManager().getEffectiveEntryLeaseTime(xtnEntry.m_CommitRollbackTimeStamp)) && !_engine.isExpiredEntryStayInSpace(entry))
                            continue;

                        try {
                            // sync volatile variable
                            _engine.touchLastEntryTimestamp();

                            NotifyContextsHolder notifyContextHolder = null;
                            context.setOperationID(pXtn.getOperationID(entry.getUID()));
                            context.setOperationVisibilityTime(packet.getCreationTime());
                            IEntryHolder shadowEh = entry.getTxnEntryData() != null ? entry.getTxnEntryData().getOtherUpdateUnderXtnEntry() : null;

                            switch (entry.getWriteLockOperation()) {
                                case SpaceOperations.TAKE:
                                case SpaceOperations.TAKE_IE:
                                    if (!_cacheManager.getTemplatesManager().anyNotifyTakeTemplates())
                                        continue;
                                    notifyContextHolder = new NotifyContextsHolder(entry, null, context.getOperationID(), NotifyActionType.NOTIFY_TAKE);

                                    break;
                                case SpaceOperations.UPDATE: {
                                    boolean isMatched = _cacheManager.getTemplatesManager().anyNotifyMatchedTemplates();
                                    boolean isRematched = _cacheManager.getTemplatesManager().anyNotifyRematchedTemplates();
                                    if (!context.isBackupOnly() && (_cacheManager.getTemplatesManager().anyNotifyUpdateTemplates() || isRematched || isMatched)) {
                                        notifyContextHolder = new UpdateNotifyContextHolder(shadowEh, entry, context.getOperationID(), isMatched, isRematched);
                                    } else if (!_cacheManager.getTemplatesManager().anyNotifyUnmatchedTemplates())
                                        continue;
                                    break;
                                }
                                default:
                                    if (!_cacheManager.getTemplatesManager().anyNotifyWriteTemplates())
                                        continue;
                                    notifyContextHolder = new NotifyContextsHolder(null, entry, context.getOperationID(), NotifyActionType.NOTIFY_WRITE);
                            }

                            if (notifyContextHolder != null)
                                _engine.getTemplateScanner().scanNotifyTemplates(notifyContextHolder, context, null/*txn*/, FifoSearch.NO);

                            if (shadowEh != null && _cacheManager.getTemplatesManager().anyNotifyUnmatchedTemplates()) {
                                notifyContextHolder = new NotifyContextsHolder(shadowEh, entry, context.getOperationID(), NotifyActionType.NOTIFY_UNMATCHED);
                                _engine.getTemplateScanner().scanNotifyTemplates(notifyContextHolder, context, null/*txn*/, FifoSearch.NO);
                            }
                        } catch (EntryDeletedException ex) {
                        }
                    } /* while */
                } /* if iter != null */
            } /* try */ finally {
                if (iter != null)
                    iter.close();

                // reset the fromReplication indication.
                context.setFromReplication(false);

            }
        /*----------------- handle need-notify: end -----------------*/

        /*---------------- handle entries need scan only for f-g templates*/
            //done directly in SpaceEngine::commitSA in order to enable faster group freeing

            // _engine.getFifoGroupsHandler().handleNeedFgOnlyScanOnXtnEnd(context, xtnEntry);
          
        /*-----------------handle entries need scan only for f-g templates - end*/

        /*----------------- handle locked Entries: begin -----------------*/
            ISAdapterIterator entriesIter = null;
            try {
                entriesIter = _cacheManager.makeUnderXtnEntriesIter(context,
                        xtnEntry, SelectType.ALL_ENTRIES /*.LockedEntries*/, true /* returnPEntry*/);

                if (entriesIter != null) {
                    Collection<ITemplateHolder> wf = null;
                    ENTRY_LOOP:
                    while (true) {
                        IEntryCacheInfo entryCacheHolder = (IEntryCacheInfo) entriesIter.next();
                        if (entryCacheHolder == null)
                            break ENTRY_LOOP;
                        IEntryHolder entry = _cacheManager.getEntryFromCacheHolder(entryCacheHolder);

                        if (entry == null)
                            continue ENTRY_LOOP;
                        //if entry is taken , we need to check for fifo-group templates
                        if ((_cacheManager.isMemorySpace() || _cacheManager.isResidentEntriesCachePolicy()) && entry.isDeleted())
                            continue ENTRY_LOOP;

                        boolean entry_has_wf = false;
                        try {
                            entryLock = getEntryLockObject(entry);
                            synchronized (entryLock) {
                                IEntryHolder eh = null;
                                //NOTE: taken entries are handled in handleCommittedTakenEntries
                                if (!entry.isBlobStoreEntry())
                                    eh = _cacheManager.getEntry(context, entry.getUID(), null, null, true /*tryInsertToCache*/,
                                            true /*lockedEntry*/, true /*useOnlyCache*/);
                                else
                                    eh = _cacheManager.getEntry(context, entry, true /*tryInsertToCache*/, true /*lockedEntry*/, true /*useOnlyCache*/);

                                if (eh == null || eh.isDeleted())
                                    continue ENTRY_LOOP;
                                if (!entry.isSameEntryInstance(eh) && _cacheManager.getLockManager().isPerLogicalSubjectLockObject(_cacheManager.isTieredStorage() || _cacheManager.isEvictableCachePolicy()))
                                    continue ENTRY_LOOP;
                                entry = eh;

                                boolean updatedEntry = pXtn.isUpdatedEntry(entry);

                                _cacheManager.disconnectEntryFromXtn(context, entry, xtnEntry, true /*xtnEnd*/);

                                if (entry.isExpired(xtnEntry.m_CommitRollbackTimeStamp) && !entry.isEntryUnderWriteLockXtn() && !_engine.isExpiredEntryStayInSpace(entry)) {//recheck expired- space volatile touch
                                    if (entry.isExpired(_engine.getLeaseManager().getEffectiveEntryLeaseTime(xtnEntry.m_CommitRollbackTimeStamp))) {
                                        if (entry.isBlobStoreEntry())
                                            _cacheManager.getEntry(context, entry, true /*tryInsertToCache*/, true /*lockedEntry*/, true /*useOnlyCache*/);

                                        IServerTypeDesc typeDesc = _engine.getTypeManager().getServerTypeDesc(entry.getClassName());
                                        boolean considerExpirationReplication = !xtnEntry.isFromReplication() && _engine.getLeaseManager().replicateLeaseExpirationEventsForEntries();

                                        _engine.removeEntrySA(context, entry, typeDesc,
                                                false /*fromRepl*/, true /*origin*/,
                                                SpaceEngine.EntryRemoveReasonCodes.LEASE_EXPIRED /*fromLeaseExpiration*/,
                                                !considerExpirationReplication/*disableReplication*/, false /*disableProcessorCall*/, false /*disableSADelete*/);

                                        continue ENTRY_LOOP;
                                    }
                                }

                                //remove WF connections for committed-updated entries in which some WF
                                // templates may be irrelevant (no match anymore)
                                if (updatedEntry && entry.isHasWaitingFor())
                                    _engine.checkWFValidityAfterUpdate(context, entry);

                                if (entry.isHasWaitingFor()) {
                                    entry_has_wf = true;
                                    wf = entry.getCopyOfTemplatesWaitingForEntry();
                                }
                            } /* synchronized(entryLock) */
                        } finally {
                            if (entryLock != null)
                                freeEntryLockObject(entryLock);
                            entryLock = null;
                        }


                        if (entry_has_wf && wf != null) {
                            TEMPLATE_LOOP:
                            for (ITemplateHolder template : wf) {
                                if (template.isDeleted()) {
                                    //remove WF info of template-entry
                                    handleRemoveWaitingForInfoSA_EntryBased(context, entry, template);
                                    continue TEMPLATE_LOOP; /* to next template */
                                }

                                //FIFO +++++++++++++++++++++++++++++++++++++++++++++++++++

                                if (template.isFifoTemplate())
                                    //fifo entries handled in handleLockedFifoEntriesOnXtnEnd
                                    continue TEMPLATE_LOOP; /* to next template */
                                //FIFO ---------------------------------------------------


                                try {
                                    _engine.performTemplateOnEntrySA(context, template, entry,
                                            true/*makeWaitForInfo*/);
                                } catch (TransactionConflictException ex) {
                                    continue TEMPLATE_LOOP; /* to next template */
                                } catch (TemplateDeletedException ex) {
                                    continue TEMPLATE_LOOP; /* to next template */
                                } catch (TransactionNotActiveException ex) {
                                    continue TEMPLATE_LOOP; /* to next template */
                                } catch (NoMatchException ex) {
                                    continue TEMPLATE_LOOP; /* to next template */
                                } catch (FifoException ex) {
                                    continue TEMPLATE_LOOP; /* to next template */
                                } catch (EntryDeletedException ex) {
                                    continue ENTRY_LOOP; /* to next entry */
                                } catch (RuntimeException ex) {//
                                    after_xtn_activity_error(context, template, ex, true);
                                    continue ENTRY_LOOP; /* to next entry */
                                }
                            } /* TEMPLATE_LOOP */
                        }/* if entry has waiting for */

                        try {
                            // sync volatile variable
                            _engine.touchLastEntryTimestamp();

                            _engine.getTemplateScanner().scanNonNotifyTemplates(context, entry,
                                    null/*txn*/, FifoSearch.NO, FifoGroupsSearch.NO);

                        } catch (EntryDeletedException ex) {
                            continue ENTRY_LOOP;
                        }
                    } /* ENTRY_LOOP */
                } /* if entriesiter != null */
            } /* try */ finally {
                if (entriesIter != null)
                    entriesIter.close();
            }

         /*----------------- handle locked Entries: end -----------------*/

            _engine.getTransactionHandler().removeTransactionAfterEnd(xtnEntry);


        } /* try */ finally {
            _cacheManager.freeCacheContext(context);
        }
    }


    private ILockObject getEntryLockObject(IEntryHolder entry) {
        return
                _cacheManager.getLockManager().getLockObject(entry);
    }

    private void freeEntryLockObject(ILockObject entryLock) {
        _cacheManager.getLockManager().freeLockObject(entryLock);

    }


    private void after_xtn_activity_error(Context context, ITemplateHolder template, Exception ex, boolean afterCommit) {
        LogLevel level = LogLevel.SEVERE;
        if (ex instanceof ProtectiveModeException)
            level = LogLevel.DEBUG;
        else if (ex instanceof ChangeInternalException) {
            ex = ((ChangeInternalException) ex).getInternalException();
            level = LogLevel.DEBUG;
        }
        String op = afterCommit ? "commit" : "rollback";
        // Log internal error...
        if (level.isEnabled(_logger)) {
            level.log(_logger, "Error handling post " + op, ex);
        }

        Object templateLock = getTemplateLockObject(template);
        synchronized (templateLock) {
            if (!template.hasAnswer())
                context.setOperationAnswer(template, null, ex);
            if (template.isInCache() && !template.isDeleted()) {
                try {
                    _cacheManager.removeTemplate(context,
                            template,
                            false /* updateRedoLog */,
                            true /* origin */,
                            false);
                } catch (Exception e) {
                    // Log internal error...
                    if (_logger.isErrorEnabled()) {
                        _logger.error(
                                "Internal Error failed to removeTemplate: ",
                                ex);
                    }
                }
            }
        }


    }


    /**
     * this routine is called if the m_CrossXtnsFifoNotifications proprty is set if inserts the
     * entries to the recentFifo queue of notify this routine is called directly after commit and
     * not by backgroud thread return true if any entry considered
     */
    public boolean handleNotifyFifoInCommit(Context context, XtnEntry xtnEntry, boolean fifoNotifyForNonFifoEvents)
            throws SAException {
        boolean res = false;
        if (!_cacheManager.getTemplatesManager().anyNotifyFifoTakeTemplates() && !_cacheManager.getTemplatesManager().anyNotifyFifoUpdateTemplates() &&
                !_cacheManager.getTemplatesManager().anyNotifyFifoWriteTemplates())
            return res; //no fifo templates
        if (fifoNotifyForNonFifoEvents) {
            if (!_cacheManager.getTemplatesManager().anyNotifyFifoTakeTemplatesForNonFifoType() && !_cacheManager.getTemplatesManager().anyNotifyFifoUpdateTemplatesForNonFifoType() &&
                    !_cacheManager.getTemplatesManager().anyNotifyFifoWriteTemplatesForNonFifoType())
                return res;
        }
   	 /*----------------- handle need-notify Entries: begin -----------------*/
        ISAdapterIterator iter = null;
        IServerTypeDesc typeDesc = null;

        try {
            //if update supported- use the "snap image" of the contents
            // of entries that need to be notified- it is taken during
            //"pre-prepare" call
            iter = _cacheManager.makeUnderXtnEntriesIter(context, xtnEntry, SelectType.NEED_NOTIFY_ENTRIES);
            final XtnData pXtn = xtnEntry.getXtnData();

            if (iter != null) {
                while (true) {
                    IEntryHolder entry = (IEntryHolder) iter.next();
                    if (entry == null)
                        break;

                    //note- no need to lock entry its just a snapshot of the original entry
                    if (!_engine.getLeaseManager().isNoReapUnderXtnLeases() && entry.isExpired(_engine.getLeaseManager().getEffectiveEntryLeaseTime(xtnEntry.m_CommitRollbackTimeStamp)) && !_engine.isExpiredEntryStayInSpace(entry))
                        continue;

                    context.setOperationID(pXtn.getOperationID(entry.getUID()));

                    int operation;
                    switch (entry.getWriteLockOperation()) {
                        case SpaceOperations.TAKE:
                        case SpaceOperations.TAKE_IE:
                            if (!_cacheManager.getTemplatesManager().anyNotifyFifoTakeTemplates())
                                continue;
                            if (typeDesc == null || !typeDesc.getTypeName().equals(entry.getClassName()))
                                typeDesc = _engine.getTypeTableEntry(entry.getClassName());
                            if ((typeDesc.isFifoSupported() && fifoNotifyForNonFifoEvents) ||
                                    (!typeDesc.isFifoSupported() && !fifoNotifyForNonFifoEvents))
                                continue;

                            if (!typeDesc.isFifoSupported() && !_cacheManager.getTemplatesManager().anyNotifyFifoForNonFifoTypePerOperation(typeDesc, SpaceOperations.TAKE))
                                continue;
                            operation = SpaceOperations.TAKE;
                            break;
                        case SpaceOperations.UPDATE:
                            if (!_cacheManager.getTemplatesManager().anyNotifyFifoUpdateTemplates())
                                continue;
                            if (typeDesc == null || !typeDesc.getTypeName().equals(entry.getClassName()))
                                typeDesc = _engine.getTypeTableEntry(entry.getClassName());
                            if ((typeDesc.isFifoSupported() && fifoNotifyForNonFifoEvents) ||
                                    (!typeDesc.isFifoSupported() && !fifoNotifyForNonFifoEvents))
                                continue;
                            if (!typeDesc.isFifoSupported() && !_cacheManager.getTemplatesManager().anyNotifyFifoForNonFifoTypePerOperation(typeDesc, SpaceOperations.UPDATE))
                                continue;
                            operation = SpaceOperations.UPDATE;
                            break;
                        default:
                            if (!_cacheManager.getTemplatesManager().anyNotifyFifoWriteTemplates())
                                continue;
                            if (typeDesc == null || !typeDesc.getTypeName().equals(entry.getClassName()))
                                typeDesc = _engine.getTypeTableEntry(entry.getClassName());
                            if ((typeDesc.isFifoSupported() && fifoNotifyForNonFifoEvents) ||
                                    (!typeDesc.isFifoSupported() && !fifoNotifyForNonFifoEvents))
                                continue;
                            if (!typeDesc.isFifoSupported() && !_cacheManager.getTemplatesManager().anyNotifyFifoForNonFifoTypePerOperation(typeDesc, SpaceOperations.WRITE))
                                continue;
                            operation = SpaceOperations.WRITE;
                    }
                    IEntryHolder notifyeh = operation != SpaceOperations.TAKE ? entry.createCopy() : entry;
                    IEntryHolder shadowEh = entry.getTxnEntryData() != null ? entry.getTxnEntryData().getOtherUpdateUnderXtnEntry() : null;

                    FifoBackgroundRequest red = new FifoBackgroundRequest(
                            context.getOperationID(),
                            true/*isNotifyRequest*/,
                            false/*isNonNotifyRequest*/,
                            entry,
                            shadowEh,
                            xtnEntry.isFromReplication(),
                            operation,
                            null/*xtn*/,
                            notifyeh);
                    red.setXtnEnd();
                    red.setTime(xtnEntry.m_CommitRollbackTimeStamp);
                    if (fifoNotifyForNonFifoEvents)
                        red.setAllowFifoNotificationsForNonFifoEvents(xtnEntry.getAllowFifoNotificationsForNonFifoEntries());

                    _fifoBackgroundDispatcher.positionAndActivateRequest(red);
                    res = true;
                } /* while */
            } /* if iter != null */
        } /* try */ finally {
            if (iter != null)
                iter.close();
        }
        return res;
   	 /*----------------- handle need-notify: end -----------------*/

    }

    /**
     * when xtn terminated send the fifo entries to look for for fifo templates its is called during
     * commit/rb from the engine main routines
     */
    public void handleLockedFifoEntriesOnXtnEnd(Context context, XtnEntry xtnEntry, boolean fromRollback)
            throws SAException {
        ISAdapterIterator entriesIter = null;
        ILockObject entryLock = null;
        long fifoXtnNumber = _cacheManager.getFifoXtnNumber(xtnEntry);

        if (!xtnEntry.anyFifoEntriesUnderXtn())
            return;

        TreeMap<IEntryHolder, FifoBackgroundRequest> orderedXtnEntries =
                new TreeMap<IEntryHolder, FifoBackgroundRequest>(_comparator);

        try {
            entriesIter = _cacheManager.makeUnderXtnEntriesIter(context, xtnEntry,
                    SelectType.ALL_FIFO_ENTRIES /*.Locked fifo Entries*/, true /* returnPEntry*/);

            if (entriesIter != null) {
                final XtnData pXtn = xtnEntry.getXtnData();

                ENTRY_LOOP:
                while (true) {
                    IEntryCacheInfo entryCacheHolder = (IEntryCacheInfo) entriesIter.next();
                    if (entryCacheHolder == null)
                        break ENTRY_LOOP;
                    IEntryHolder entry = _cacheManager.getEntryFromCacheHolder(entryCacheHolder);

                    if (entry == null)
                        break ENTRY_LOOP;

                    context.setOperationID(pXtn.getOperationID(entry.getUID()));

                    try {
                        entryLock = getEntryLockObject(entry);
                        synchronized (entryLock) {
                            IEntryHolder original = entry;
                            entry = _cacheManager.getEntry(context, entry, true /*tryInsertToCache*/, true /*lockeEntry*/);
                            if (entry == null || !entry.isSameEntryInstance(original)) {
                                continue ENTRY_LOOP;
                            }

                            if (entry.isExpired(xtnEntry.m_CommitRollbackTimeStamp) && (!entry.isEntryUnderWriteLockXtn() || !_engine.getLeaseManager().isNoReapUnderXtnLeases()))
                                //second check- avoid touching volatile
                                if (!_engine.isExpiredEntryStayInSpace(entry) && entry.isExpired(_engine.getLeaseManager().getEffectiveEntryLeaseTime(xtnEntry.m_CommitRollbackTimeStamp)))
                                    continue ENTRY_LOOP;

                            boolean writeLock = false;
                            if (entry.getWriteLockOwner() == xtnEntry)
                                writeLock = true;
                            if (!fromRollback && writeLock && (
                                    (entry.getWriteLockOperation() == SpaceOperations.TAKE ||
                                            entry.getWriteLockOperation() == SpaceOperations.TAKE_IE)))
                                continue;

                            if (fromRollback && writeLock && entry.getWriteLockOperation() == SpaceOperations.WRITE)
                                continue;


                        } /* synchronized(entryLock) */
                    } finally {
                        if (entryLock != null)
                            freeEntryLockObject(entryLock);
                        entryLock = null;
                    }


                    // sync volatile variable
                    _engine.touchLastEntryTimestamp();

                    if (_cacheManager.getTemplatesManager().anyNonNotifyFifoTemplates()) {
                        FifoBackgroundRequest red = new FifoBackgroundRequest(
                                context.getOperationID(),
                                false/*isNotifyRequest*/,
                                true/*isNonNotifyRequest*/,
                                entry,
                                null /*originalEntry*/,
                                false /*fromReplication*/,
                                SpaceOperations.WRITE /*operation-dummy*/,
                                null /*xtn object*/,
                                null /*cloneEH*/);
                        red.setXtnEnd();

                        orderedXtnEntries.put(entry, red);
                    }
                } /* ENTRY_LOOP */
            } /* if entriesiter != null */

            //after accumulating & sorting- insert the entries to recentfifoentries
            //for processing
            Iterator<Map.Entry<IEntryHolder, FifoBackgroundRequest>> titer = orderedXtnEntries.entrySet().iterator();
            while (titer.hasNext()) {
                Map.Entry<IEntryHolder, FifoBackgroundRequest> me = titer.next();
                IEntryHolder eh = me.getKey();
                if (eh.isDeleted())
                    continue;
                FifoBackgroundRequest red = me.getValue();
                red.setFifoXtnNumber(fifoXtnNumber);

                _fifoBackgroundDispatcher.positionAndActivateRequest(red);
            }

        } /* try */ finally {
            if (entriesIter != null)
                entriesIter.close();
        }


    }

    /**
     * before xtn terminated update the last readLock/Writelock fifo xtn number for this entryin
     * order to have a coherent picture its is called during prepare/commit/rb from the engine main
     * routines before status change
     */
    public void handleLockedFifoEntriesBeforeXtnEnd(Context context, XtnEntry xtnEntry, boolean fromRollback)
            throws SAException {
        long fifoXtnNumber = _cacheManager.getFifoXtnNumber(xtnEntry);
        ILockObject entryLock = null;

        ISAdapterIterator entriesIter = null;
        if (!xtnEntry.anyFifoEntriesUnderXtn() || fifoXtnNumber == TerminatingFifoXtnsInfo.UNKNOWN_FIFO_XTN)
            return;


        try {
            entriesIter = _cacheManager.makeUnderXtnEntriesIter(context,
                    xtnEntry, SelectType.ALL_FIFO_ENTRIES /*.Locked fifo Entries*/, true /* returnPEntry*/);

            if (entriesIter != null) {
                while (true) {
                    IEntryCacheInfo entryCacheHolder = (IEntryCacheInfo) entriesIter.next();
                    if (entryCacheHolder == null)
                        break;
                    IEntryHolder entry = _cacheManager.getEntryFromCacheHolder(entryCacheHolder);

                    if (entry == null)
                        break;
                    // sync volatile variable
                    _engine.touchLastEntryTimestamp();
                    try {
                        entryLock = getEntryLockObject(entry);
                        synchronized (entryLock) {
                            boolean writeLock = false;
                            if (entry.getWriteLockOwner() == xtnEntry)
                                writeLock = true;
                            if (!fromRollback && writeLock && (
                                    (entry.getWriteLockOperation() == SpaceOperations.TAKE ||
                                            entry.getWriteLockOperation() == SpaceOperations.TAKE_IE)))
                                continue;

                            if (fromRollback && writeLock && entry.getWriteLockOperation() == SpaceOperations.WRITE)
                                continue;

                            //update the fifo xtn number per entry
                            boolean entryWritingXtn = writeLock && entry.getWriteLockOperation() == SpaceOperations.WRITE;

                            _cacheManager.updateFifoXtnInfoForEntry(entry, fifoXtnNumber, writeLock, entryWritingXtn);


                        } /* synchronized(entryLock) */
                        continue;
                    } finally {
                        if (entryLock != null)
                            freeEntryLockObject(entryLock);
                        entryLock = null;
                    }


                } /* while */
            } /* if entriesiter != null */

        } /* try */ finally {
            if (entriesIter != null)
                entriesIter.close();
        }

    }


    //FIFO ----------------------------------------------------


    /**
     * handle taken entries under commit . NOTE: this method is called in prepare-commit stage in
     * order to evict taken entries. taken entries are not handled in post-commit stage
     * (habdleCommitSA) in order to enabled rewritting entries with same UIDS transaction table is
     * locked when this method is called
     */
    public void handleCommittedTakenEntries(Context context, final XtnEntry xtnEntry)
            throws SAException {
        ISAdapterIterator<IEntryHolder> entriesIter = null;
        ILockObject entryLock = null;
        try {
            entriesIter = _cacheManager.makeUnderXtnEntriesIter(context,
                    xtnEntry, SelectType.TAKEN_ENTRIES /*.LockedEntries*/, false /* returnPEntry*/);

            if (entriesIter != null) {
                final XtnData pXtn = xtnEntry.getXtnData();
                while (true) {
                    entryLock = null;
                    IEntryHolder entry = entriesIter.next();
                    if (entry == null)
                        break;
                    if (entry.getWriteLockTransaction() == null ||
                            (entry.getWriteLockOperation() != SpaceOperations.TAKE &&
                                    entry.getWriteLockOperation() != SpaceOperations.TAKE_IE))
                        continue;

                    try {
                        entryLock = getEntryLockObject(entry);
                        synchronized (entryLock) {
                            IEntryHolder original = entry;
                            if (entry.getWriteLockTransaction() != null && entry.getWriteLockOwner() == xtnEntry &&
                                    (entry.getWriteLockOperation() == SpaceOperations.TAKE ||
                                            entry.getWriteLockOperation() == SpaceOperations.TAKE_IE)) {
                                IServerTypeDesc tte = _engine.getTypeManager().getServerTypeDesc(entry.getClassName());
                                context.setOperationID(pXtn.getOperationID(entry.getUID()));
                                _engine.removeEntrySA(context, entry, tte, xtnEntry.isFromReplication() /*fromReplication*/,
                                        true /*origin*/, SpaceEngine.EntryRemoveReasonCodes.TAKE,
                                        true/* disable replication */, false /* disable processor call */,
                                        true /*disableSADelete*/);
                            }
                        } /* synchronized(entryLock) */
                        continue;
                    } finally {
                        if (entryLock != null)
                            freeEntryLockObject(entryLock);
                        entryLock = null;
                    }
                } /* ENTRY_LOOP */
            } /* if entriesiter != null */
        } /* try */ finally {
            if (entriesIter != null)
                entriesIter.close();
        }
    }

    /**
     * Handles rollback SA.
     */
    public void handleRollbackSA(RollbackBusPacket packet) throws SAException {
        Context context = null;
        ISAdapterIterator iter = null, entriesIter = null;
        ILockObject templateLock = null, entryLock = null;
        final XtnEntry xtnEntry = packet._xtnEntry;
        final XtnData pXtn = xtnEntry.getXtnData();

        try {
            context = _cacheManager.getCacheContext();

      /*----------------- handle Notify Templates: begin -----------------*/
            //  do nothing -
            //     done by the StorageAdapter.commit method which is called from Engine.commit()
      /*----------------- handle Notify Templates: end -----------------*/

      /*----------------- handle Read/Take Templates: begin -----------------*/
            try {
                iter = _cacheManager.makeUnderXtnTemplatesIter(context, xtnEntry);
                if (iter != null) {
                    while (true) {
                        ITemplateHolder template = (ITemplateHolder) iter.next();
                        if (template == null)
                            break;

                        try {
                            templateLock = getTemplateLockObject(template);
                            synchronized (templateLock) {
                                if (template.isDeleted())
                                    continue;
                                if (template.isExpired(xtnEntry.m_CommitRollbackTimeStamp))
                                    context.setOperationAnswer(template, null, null);
                                else
                                    context.setOperationAnswer(template, null, new TransactionException("Transaction not active : " + xtnEntry.m_Transaction));
                                _cacheManager.removeTemplate(context, template,
                                        false /*fromReplication*/, true /*origin*/, false);
                            } /* lock (templateLock) */
                        } /* try */ finally {
                            if (templateLock != null)
                                freeTemplateLockObject(templateLock);
                        }
                    } /* for each template */
                } /* if iter != null */
            } /* try */ finally {
                if (iter != null)
                    iter.close();
            }
          /*----------------- handle Read/Take Templates: end -----------------*/

          /*----------------- handle new Entries: begin -----------------*/
        /*----------------- handle new Entries: end -----------------*/

            //handling of new entries is done directly (from rollback) on
            //  handleNewRolledbackEntries

         /*---------------- handle entries need scan only for f-g templates*/
            //done directly in SpaceEngine :: rollbackSA
            // _engine.getFifoGroupsHandler().handleNeedFgOnlyScanOnXtnEnd(context, xtnEntry);
           
         /*-----------------handle entries need scan only for f-g templates - end*/

         /*---------------- handle entries need scan only for f-g templates*/

            _engine.getFifoGroupsHandler().handleNeedFgOnlyScanOnXtnEnd(context, xtnEntry);
         
         /*------------------------------------------------------------------*/
         
         
         
         
         /*----------------- handle locked Entries: begin -----------------*/
            try {
                entriesIter = _cacheManager.makeUnderXtnEntriesIter(context,
                        xtnEntry, SelectType.ALL_ENTRIES, true);
                if (entriesIter != null) {
                    Collection<ITemplateHolder> wf = null;
                    ENTRY_LOOP:
                    while (true) {
                        IEntryCacheInfo entryCacheHolder = (IEntryCacheInfo) entriesIter.next();
                        if (entryCacheHolder == null)
                            break ENTRY_LOOP;

                        IEntryHolder entry = _cacheManager.getEntryFromCacheHolder(entryCacheHolder);

                        if (entry == null)
                            continue ENTRY_LOOP;
                        if ((_cacheManager.isMemorySpace() || _cacheManager.isResidentEntriesCachePolicy()) && entry.isDeleted())
                            continue ENTRY_LOOP;
                        boolean entry_has_wf = false;
                        try {
                            entryLock = getEntryLockObject(entry);
                            synchronized (entryLock) {
                                IEntryHolder eh = _cacheManager.getEntry(context, entry, true /*tryInsertToCache*/, true /*lockedEntry*/, true /*useOnlyCache*/);
                                if (eh == null || eh.isDeleted())
                                    continue ENTRY_LOOP;
                                if (!entry.isSameEntryInstance(eh) && _cacheManager.getLockManager().isPerLogicalSubjectLockObject(_cacheManager.isTieredStorage() || _cacheManager.isEvictableCachePolicy()))
                                    continue ENTRY_LOOP;
                                entry = eh;
                                boolean updatedEntry = pXtn.isUpdatedEntry(entry);
                                _cacheManager.disconnectEntryFromXtn(context, entry, xtnEntry, true /*xtnEnd*/);
                                if (entry.isExpired(xtnEntry.m_CommitRollbackTimeStamp) && !entry.isEntryUnderWriteLockXtn()) {//recheck- spare touching a volatile
                                    if (!_engine.isExpiredEntryStayInSpace(entry) && entry.isExpired(_engine.getLeaseManager().getEffectiveEntryLeaseTime(xtnEntry.m_CommitRollbackTimeStamp))) {
                                        if (entry.isBlobStoreEntry())
                                            _cacheManager.getEntry(context, entry, true /*tryInsertToCache*/, true /*lockedEntry*/, true /*useOnlyCache*/);

                                        IServerTypeDesc typeDesc = _engine.getTypeManager().getServerTypeDesc(entry.getClassName());
                                        _engine.removeEntrySA(context, entry, typeDesc,
                                                false /*fromRepl*/, true /*origin*/,
                                                SpaceEngine.EntryRemoveReasonCodes.LEASE_EXPIRED /*fromLeaseExpiration*/,
                                                false/*disableReplication*/, false /*disableProcessorCall*/, false /*disableSADelete*/);
                                        continue ENTRY_LOOP;
                                    }
                                }

                                //remove WF connections for rolled-updated entries in which some WF
                                // templates may be irrelevant (no match anymore)
                                if (updatedEntry && entry.isHasWaitingFor())
                                    _engine.checkWFValidityAfterUpdate(context, entry);

                                if (entry.isHasWaitingFor()) {
                                    entry_has_wf = true;
                                    wf = entry.getCopyOfTemplatesWaitingForEntry();
                                }
                            } /* synchronized(entryLock) */
                        } finally {
                            if (entryLock != null)
                                freeEntryLockObject(entryLock);
                            entryLock = null;
                        }

                        if (entry_has_wf && wf != null) {
                            TEMPLATE_LOOP:
                            for (ITemplateHolder template : wf) {
                                if (template.isDeleted()) {
                                    //remove WF info of template-entry
                                    handleRemoveWaitingForInfoSA_EntryBased(context, entry, template);
                                    continue TEMPLATE_LOOP; /* to next template */
                                }

                                //FIFO +++++++++++++++++++++++++++++++++++++++++++++++++++
                                if (template.isFifoTemplate())
                                    //fifo entries handled in handleLockedFifoEntriesOnXtnEnd
                                    continue TEMPLATE_LOOP; /* to next template */
                                //FIFO ---------------------------------------------------

                                try {
                                    _engine.performTemplateOnEntrySA(context, template, entry,
                                            true/*makeWaitForInfo*/);
                                } catch (TransactionConflictException ex) {
                                    continue TEMPLATE_LOOP; /* to next template */
                                } catch (TemplateDeletedException ex) {
                                    continue TEMPLATE_LOOP; /* to next template */
                                } catch (NoMatchException ex) {
                                    continue TEMPLATE_LOOP; /* to next template */
                                } catch (FifoException ex) {
                                    continue TEMPLATE_LOOP; /* to next template */
                                } catch (TransactionNotActiveException ex) {
                                    continue TEMPLATE_LOOP; /* to next template */
                                } catch (EntryDeletedException ex) {
                                    continue ENTRY_LOOP; /* to next entry */
                                } catch (RuntimeException ex) {//
                                    after_xtn_activity_error(context, template, ex, false);
                                    continue ENTRY_LOOP; /* to next entry */
                                }
                            } /* TEMPLATE_LOOP */
                        } /* if entry has waiting for */

                        try {
                            // sync volatile variable
                            _engine.touchLastEntryTimestamp();
                            _engine.getTemplateScanner().scanNonNotifyTemplates(context, entry,
                                    null/*txn*/, FifoSearch.NO, FifoGroupsSearch.NO);
                        } catch (EntryDeletedException ex) {
                            continue ENTRY_LOOP;
                        }
                    } /* ENTRY_LOOP */
                } /* if entriesIter != null */
            } /* try */ finally {
                if (entriesIter != null)
                    entriesIter.close();
            }

        /*----------------- handle locked Entries: end -----------------*/

            _engine.getTransactionHandler().removeTransactionAfterEnd(xtnEntry);
        } /* try */ finally {
            _cacheManager.freeCacheContext(context);
        }
    }


    /**
     * handle new entries  under rollback . clean those entries. it is called directly in order to
     * clean user-defined UIDS
     */
    public void handleNewRolledbackEntries(Context context, final XtnEntry xtnEntry)
            throws SAException {
        ISAdapterIterator iter = null;
        try {
            iter = _cacheManager.makeUnderXtnEntriesIter(context, xtnEntry, SelectType.NEW_ENTRIES);
            if (iter != null) {
                final XtnData pXtn = xtnEntry.getXtnData();
                while (true) {
                    IEntryHolder entry = (IEntryHolder) iter.next();
                    if (entry == null)
                        break;

                    ILockObject entryLock = null;
                    try {
                        entryLock = getEntryLockObject(entry);
                        synchronized (entryLock) {
                            boolean fromLeaseExpiration = !_engine.getLeaseManager().isNoReapUnderXtnLeases() && entry.isExpired(_engine.getLeaseManager().getEffectiveEntryLeaseTime(xtnEntry.m_CommitRollbackTimeStamp)) && !_engine.isExpiredEntryStayInSpace(entry);
                            context.setOperationID(pXtn.getOperationID(entry.getUID()));
                            _engine.removeEntrySA(context, entry, false /*fromReplication*/,
                                    true /*origin*/, false /*ofReplClass*/, fromLeaseExpiration ? SpaceEngine.EntryRemoveReasonCodes.LEASE_EXPIRED : SpaceEngine.EntryRemoveReasonCodes.TAKE /*fromLeaseExpiration*/,
                                    true /* disableReplication*/, false /* disableProcessorCall*/, true /* disableSADelete*/);
                        } /* synchronized(entryLock) */
                    } finally {
                        if (entryLock != null)
                            freeEntryLockObject(entryLock);
                        entryLock = null;
                    }
                } /* while */
            } /* if iter != null */
        } /* try */ finally {
            if (iter != null)
                iter.close();
        }
    }

    /**
     * Handles the remove of waiting for info of a entry .
     */
    public void handleRemoveWaitingForInfoSA(RemoveWaitingForInfoSABusPacket packet)
            throws SAException {
        Context context = null;

        try {
            context = _cacheManager.getCacheContext();
            context.setOperationID(packet.getOperationID());
            if (packet.getEntryHolder() != null) {
                handleRemoveWaitingForInfoSA_EntryBased(context, packet.getEntryHolder(), packet.getTemplate());
            } else {
                //template based
                handleRemoveWaitingForInfoSA_TemplateBased(context, packet.getTemplate());
            }
        } finally {
            _cacheManager.freeCacheContext(context);
        }
    }

    /**
     * Handles the remove of waiting for info of a entry via all its connected templates .
     */
    private void handleRemoveWaitingForInfoSA_EntryBased(Context context, IEntryHolder entry, ITemplateHolder template)
            throws SAException {
        ILockObject entryLock = null;

        //check if fifo group templates are waiting for this entry- "this" means a group here
        _engine.getFifoGroupsHandler().handleRemoveWaitingForInfoSAFifoGroups_EntryBased(context, entry,
                template);

        boolean need_unpin = false;
        try {
            entryLock = getEntryLockObject(entry);
            synchronized (entryLock) {
                try {
                    if (entry.isBlobStoreEntry()) {
                        need_unpin = true;
                        entry = ((IBlobStoreEntryHolder) entry).getLatestEntryVersion(_cacheManager, true/*attach*/, context);
                    }

                    if (template != null) {//specific template, handle it and return
                        handleRemoveWaitingForInfoSA_Template(context, entry, template);
                    } else {
                        //create WF iterator
                        Collection<ITemplateHolder> wf = entry.getCopyOfTemplatesWaitingForEntry();
                        if (wf == null)
                            return;

                        for (ITemplateHolder th : wf) {
                            handleRemoveWaitingForInfoSA_Template(context, entry, th);
                        } /* while (true) */
                    }
                } finally {
                    if (need_unpin)
                        _cacheManager.unpinIfNeeded(context, entry, null, null);
                }
            } /* synchronized (entryLock) */
        } /* try */ finally {
            if (entryLock != null)
                freeEntryLockObject(entryLock);
        }
    }


    /**
     * Handles the remove of waiting for info of a template via all its connected entries . this
     * routine is called for a deleted template in order to remove its WF info
     */
    private void handleRemoveWaitingForInfoSA_TemplateBased(Context context, ITemplateHolder template)
            throws SAException {
        //create WF iterator
        ILockObject entryLock = null;
        Collection<IEntryHolder> clonedWaitingFor;
        ILockObject templateLock = getTemplateLockObject(template);


        try {
            synchronized (templateLock) {
                clonedWaitingFor = template.getEntriesWaitingForTemplate() != null ? new ArrayList<IEntryHolder>(template.getEntriesWaitingForTemplate()) : null;
                if (clonedWaitingFor == null)
                    return;
            }
        } finally {
            freeTemplateLockObject(templateLock);
        }

        for (IEntryHolder entry : clonedWaitingFor) {
            boolean need_unpin = false;
            try {
                entryLock = getEntryLockObject(entry);
                synchronized (entryLock) {
                    try {
                        if (entry.isBlobStoreEntry()) {
                            need_unpin = true;
                            entry = ((IBlobStoreEntryHolder) entry).getLatestEntryVersion(_cacheManager, true/*attach*/, context);
                        }
                        handleRemoveWaitingForInfoSA_Template(context, entry, template);
                    } finally {
                        if (need_unpin)
                            _cacheManager.unpinIfNeeded(context, entry, null, null);
                    }
                } /* synchronized (entryLock) */
            } //try
            finally {
                if (entryLock != null) {
                    freeEntryLockObject(entryLock);
                    entryLock = null;
                }
            }
        } /* while (true) */
    }


    /**
     * Handles the remove of waiting for info of a template in an entry.
     */
    private void handleRemoveWaitingForInfoSA_Template(Context context, final IEntryHolder entry, final ITemplateHolder template)
            throws SAException {
        ILockObject templateLock = getTemplateLockObject(template);
        try {
            synchronized (templateLock) {
                //remove the WF connection entry-template
                _cacheManager.removeWaitingForInfo(context, entry, template, true /*unpinIfPossible*/);

                if (template.isDeleted())
                    //whoever deleted template has already notified receiver
                    return;
                if (template.isHasWaitingFor())
                    //template still has a  WF connections
                    return;
                if (!template.isInitialIfExistSearchActive()) { //initial search terminated- can return a no-success

                    boolean exceptionIfNoEntry = template.getUidToOperateBy() != null;

                    if (((template.getTemplateOperation() == SpaceOperations.READ_IE)
                            || (template.getTemplateOperation() == SpaceOperations.TAKE_IE))
                            && !ReadModifiers.isMatchByID(template.getOperationModifiers())) {
                        exceptionIfNoEntry = false;
                    }

                    if (exceptionIfNoEntry) {//designated entry not in space
                        EntryNotInSpaceException exv = new EntryNotInSpaceException(template.getUidToOperateBy(), _engine.getSpaceName(), false);
                        context.setOperationAnswer(template, null, exv);
                    } else {
                        context.setOperationAnswer(template, null, null);
                    }
                    _cacheManager.removeTemplate(context, template,
                            false /*fromReplication*/, true /*origin*/, false);
                }
            } /* synchronized (templateLock) */
        } /* try */ finally {
            if (templateLock != null)
                freeTemplateLockObject(templateLock);
        } /* finally */

    }


    public void handleDirectMultipleReadTakeSA(Context context, final ITemplateHolder template) {
        ILockObject templateLock = null;

        try {

            context.setFromReplication(false);
            context.setOrigin(true);
            context.setTemplateInitialSearchThread();
            try {
                if (template.isFifoSearch()) {
                    template.setInitialFifoSearchActive();
                    //set the scan fifo xtn number
                    template.setFifoXtnNumberOnSearchStart(_cacheManager.getLatestTTransactionTerminationNum());
                }
                executeBatchSearchAndProcessing(context, template, false /*makeWaitForInfo*/);
                if (template.isFifoGroupPoll() && !context.isAnyFifoGroupIndex())
                    throw new IllegalArgumentException("fifo grouping specified but no fifo grouping property defined type=" + template.getServerTypeDesc().getTypeName());

            } catch (TemplateDeletedException ex) {
                /* do nothing - whoever deleted this template notified receiver */
                return;
            }
            if (template.getBatchOperationContext().reachedMinEntries() || template.hasAnswer()) {
                if (!template.hasAnswer())
                    context.setOperationAnswer(template, null, null);
                return;
            } else {
                if (template.getExpirationTime() == 0 || template.isExpired()) {
                    context.setOperationAnswer(template, null, null);
                    return;
                }
                if (template.getMultipleUids() != null) {//multiple uids not supported in blocking op
                    context.setOperationAnswer(template, null, null);
                    return;
                }
                if (template.isInitiatedEvictionOperation()) {//evict not supported in blocking op
                    context.setOperationAnswer(template, null, null);
                    return;
                }
                /* handle inserting template into engine */
                if (template.isFifoSearch()) {//create a fifo block object in order to accumulate incoming
                    //entry events
                    template.setPendingFifoSearchObject(new PendingFifoSearch((_cacheManager.isMemorySpace() || _cacheManager.isResidentEntriesCachePolicy())));
                    template.resetFifoXtnNumberOnSearchStart();
                }

                if (template.getXidOriginated() != null) {
                    template.getXidOriginated().lock();

                    try {
                        XtnEntry xtnEntry = template.getXidOriginated();
                        if (xtnEntry == null || !xtnEntry.m_Active) {
                            context.setOperationAnswer(template, null,
                                    new TransactionException("The transaction is not active: " +
                                            template.getXidOriginatedTransaction()));
                            return;
                        }
                        if (templateLock == null)
                            templateLock = getTemplateLockObject(template);
                        synchronized (templateLock) {
                            _cacheManager.insertTemplate(context, template, false /*updateRedoLog*/);

                        }
                    } finally {
                        template.getXidOriginated().unlock();
                    }
                } else { /* template is not under Xtn */
                    if (templateLock == null)
                        templateLock = getTemplateLockObject(template);
                    synchronized (templateLock) {
                        _cacheManager.insertTemplate(context, template, false /*updateRedoLog*/);
                    }
                } /* finished handle inserting template into engine */

                try {
                    // sync volatile variable
                    boolean needSecondSearch = _engine.getLastEntryTimestamp() >= template.getSCN();

                    if (needSecondSearch) {
                        if (template.isFifoSearch())
                            //set the scan fifo xtn number
                            template.setFifoXtnNumberOnSearchStart(_cacheManager.getLatestTTransactionTerminationNum());

                        executeBatchSearchAndProcessing(context, template, false  /*makeWaitForInfo*/);
                    }

                    if (template.isFifoSearch() && template.isInCache() && !template.isDeleted() && template.getPendingFifoSearchObject() != null) {
                        //entries may be fifo-blocked on this template, enable it to be processed by recentFifoEntries
                        if (templateLock == null)
                            templateLock = getTemplateLockObject(template);
                        synchronized (templateLock) {
                            if (!template.canFinishBatchOperation() && template.getPendingFifoSearchObject() != null && template.getPendingFifoSearchObject().anyRejectedEntries() && !template.isDeleted()) {
                                //NOTE-pending FIFO search is disabled by the RecentFifoEntries when the template is processed
                                FifoBackgroundRequest red = new FifoBackgroundRequest(
                                        context.getOperationID(),
                                        false/*isNotifyRequest*/,
                                        true/*isNonNotifyRequest*/,
                                        null,
                                        null,
                                        false /*fromReplication*/, template);

                                _fifoBackgroundDispatcher.positionAndActivateRequest(red);

                            } else {
                                template.removePendingFifoSearchObject(true /*disableInitialSearch*/); //disable rejecting
                            }
                        }

                    }
                } catch (TemplateDeletedException ex) {
                    /* do nothing - whoever deleted this template notified receiver */
                }
            }
        } catch (Exception ex) {
            LogLevel level = LogLevel.SEVERE;
            if (ex instanceof ProtectiveModeException)
                level = LogLevel.DEBUG;
            else if (ex instanceof ChangeInternalException) {
                ex = ((ChangeInternalException) ex).getInternalException();
                level = LogLevel.DEBUG;
            } else if (ex instanceof BatchQueryException) {
                level = LogLevel.TRACE;
            }
            // Log internal error...
            if (level.isEnabled(_logger)) {
                level.log(_logger, "Error handling read/take/change. multiple", ex);
            }

            if (templateLock == null)
                templateLock = getTemplateLockObject(template);
            synchronized (templateLock) {
                if (!template.hasAnswer())
                    context.setOperationAnswer(template, null, ex);
                if (template.isInCache() && !template.isDeleted()) {
                    try {
                        _cacheManager.removeTemplate(context,
                                template,
                                false /* updateRedoLog */,
                                true /* origin */,
                                false);
                    } catch (Exception e) {
                        // Log internal error...
                        if (_logger.isErrorEnabled()) {
                            _logger.error(
                                    "Internal Error failed to removeTemplate: ",
                                    ex);
                        }
                    }
                }
            }
        } finally {
            if (templateLock != null)
                freeTemplateLockObject(templateLock);
        }
    }


    private void executeBatchSearchAndProcessing(Context context, final ITemplateHolder template, boolean makeWaitForInfo)
            throws TemplateDeletedException, SAException, TransactionException {
        XtnEntry txnEntry = template.getXidOriginated();
        if (_engine.isSyncReplicationEnabled() && template.isTakeOperation())
            context.setSyncReplFromMultipleOperation(true);


        try {
            //for take operations under transaction acquire transaction lock
            if (txnEntry != null && template.isTakeOperation()) {
                txnEntry.lock();

                context.setTransactionalMultipleOperation(true);
            }

            if (template.getBatchOperationContext().needToProcessExecption()) {
                try {
                    _engine.executeOnMatchingEntries(context, template, makeWaitForInfo);
                } catch (Throwable t) {
                    template.getBatchOperationContext().onException(t);
                }
            } else {//trow directly up
                _engine.executeOnMatchingEntries(context, template, makeWaitForInfo);
            }
        } finally {
            context.setTransactionalMultipleOperation(false);
            if (txnEntry != null && template.isTakeOperation())
                txnEntry.unlock();
            if (context.isActiveBlobStoreBulk()) {
                //if off-heap in case of bulk we first need to flush to the blob-store
                context.getBlobStoreBulkInfo().bulk_flush(context, false/*only_if_chunk_reached*/, true);
            }
            if (_engine.isSyncReplicationEnabled() && template.isTakeOperation())
                context.setSyncReplFromMultipleOperation(false);

            _engine.performReplication(context);
        }

    }

    //isexist batch ops
    public void handleDirectMultipleReadIEOrTakeIESA(Context context, final ITemplateHolder template) {
        handleDirectMultipleReadTakeOrChangeIESA(context, template);
    }

    public void handleDirectMultipleChangeSA(Context context, final ITemplateHolder template) {
        handleDirectMultipleReadTakeOrChangeIESA(context, template);
    }

    private void handleDirectMultipleReadTakeOrChangeIESA(Context context, final ITemplateHolder template) {
        if (template.isChange())
            template.setIfExistForChange();

        ILockObject templateLock = null;

        try {

            context.setFromReplication(false);
            context.setOrigin(true);
            context.setTemplateInitialSearchThread();
            long originalExpiration = template.getExpirationTime();
            if (originalExpiration != 0)
                template.setInitialIfExistSearchActive();
            try {
                if (template.isFifoSearch()) {
                    template.setInitialFifoSearchActive();
                    //set the scan fifo xtn number
                    template.setFifoXtnNumberOnSearchStart(_cacheManager.getLatestTTransactionTerminationNum());
                }
                //make it a non-blocking-read if possible- NBR cannot be set forif-exist with timeout
                if (originalExpiration != 0 && template.isReadOperation() && !template.isNonBlockingRead()) {
                    template.setExpirationTime(0);
                    template.setNonBlockingRead(_engine.isNonBlockingReadForOperation(template));
                }
                context.setPossibleIEBlockingMatch(false);
                executeBatchSearchAndProcessing(context, template, false /*makeWaitForInfo*/);
                if (template.isFifoGroupPoll() && !context.isAnyFifoGroupIndex())
                    throw new IllegalArgumentException("fifo grouping specified but no fifo grouping property defined type=" + template.getServerTypeDesc().getTypeName());
            } catch (TemplateDeletedException ex) {
                return; /* cannot happen */
            }

            if (!template.getBatchOperationContext().reachedMaxEntries()) {
                if (originalExpiration != template.getExpirationTime()) {
                    template.setExpirationTime(originalExpiration);
                    template.setNonBlockingRead(_engine.isNonBlockingReadForOperation(template));

                }
                if (template.getExpirationTime() == 0 || template.isExpired() || !context.isPossibleIEBlockingMatch() || template.getBatchOperationContext().reachedMinEntries()) {
                    if (!template.hasAnswer())
                        context.setOperationAnswer(template, null, null);
                    return;
                }
                if (template.getMultipleUids() != null) {//multiple uids not supported in blocking op
                    context.setOperationAnswer(template, null, null);
                    return;
                }
                if (template.isInitiatedEvictionOperation()) {//evict not supported in blocking op
                    context.setOperationAnswer(template, null, null);
                    return;
                }
                if (template.isFifoSearch()) {//create a fifo block object in order to accumulate incoming
                    //entry events
                    template.setPendingFifoSearchObject(new PendingFifoSearch((_cacheManager.isMemorySpace() || _cacheManager.isResidentEntriesCachePolicy())));
                    template.resetFifoXtnNumberOnSearchStart();
                }


                try {
                    boolean needSecondSearch = _engine.getLastEntryTimestamp() >= template.getSCN() || context.isPossibleIEBlockingMatch();


                    if (needSecondSearch) {
                        if (template.isChange())
                            ((ExtendedAnswerHolder) (template.getAnswerHolder())).resetRejectedEntriesInfo();
    					/* handle inserting template into engine */
                        templateLock = getTemplateLockObject(template);
                        if (template.getXidOriginated() != null) {
                            template.getXidOriginated().lock();

                            try {
                                XtnEntry xtnEntry = template.getXidOriginated();
                                if (xtnEntry == null || !xtnEntry.m_Active) {
                                    context.setOperationAnswer(template, null,
                                            new TransactionException("The transaction is not active: " +
                                                    template.getXidOriginatedTransaction()));
                                    return;
                                }
                                synchronized (templateLock) {
                                    _cacheManager.insertTemplate(context, template, false /*updateRedoLog*/);
                                }
                            } finally {
                                template.getXidOriginated().unlock();
                            }
                        } else { /* template is not under Xtn */
                            synchronized (templateLock) {
                                _cacheManager.insertTemplate(context, template, false /*updateRedoLog*/);
                            }
                        } /* finished handle inserting template into engine */

                        if (template.isFifoSearch())
                            //set the scan fifo xtn number
                            template.setFifoXtnNumberOnSearchStart(_cacheManager.getLatestTTransactionTerminationNum());


                        executeBatchSearchAndProcessing(context, template, true  /*makeWaitForInfo*/);
                    }


                    if (template.isFifoSearch() && template.isInCache() && !template.isDeleted() && template.getPendingFifoSearchObject() != null) {
                        //entries may be fifo-blocked on this template, enable it to be processed by recentFifoEntries
                        PendingFifoSearch pfs = null;
                        synchronized (templateLock) {
                            if (!template.canFinishBatchOperation() && template.getPendingFifoSearchObject().anyRejectedEntries() && !template.isDeleted()) {
                                //NOTE!!!!
                                //disabling the pending fifo search object is done by the RecentFifoEntries thread
                                pfs = template.getPendingFifoSearchObject();
                                FifoBackgroundRequest red = new FifoBackgroundRequest(
                                        context.getOperationID(),
                                        false/*isNotifyRequest*/,
                                        true/*isNonNotifyRequest*/,
                                        null,
                                        null,
                                        false /*fromReplication*/,
                                        template);

                                _fifoBackgroundDispatcher.positionAndActivateRequest(red);

                            } else {
                                template.removePendingFifoSearchObject(true /*disableInitialSearch*/); //disable rejecting
                            }
                        }//synchronized(templateLock)
                        //wait for the rejected entries to be processed
                        if (pfs != null)
                            pfs.waitForNonActiveStatus();
                    }
                    //any need to keep on with this template ?
                    templateLock = getTemplateLockObject(template);
                    synchronized (templateLock) {
                        if (template.hasAnswer())
                            return; //answer already received, return
                        template.resetInitialIfExistSearchActive();
                        if (!template.isHasWaitingFor()) { /* template  not waiting for anybody */
                            boolean exceptionIfNoEntry = template.getUidToOperateBy() != null;

                            if (((template.getTemplateOperation() == SpaceOperations.READ_IE)
                                    || (template.getTemplateOperation() == SpaceOperations.TAKE_IE))
                                    && !ReadModifiers.isMatchByID(template.getOperationModifiers())) {
                                exceptionIfNoEntry = false;
                            }

                            if (exceptionIfNoEntry) {//designated entry not in space
                                EntryNotInSpaceException exv = new EntryNotInSpaceException(template.getUidToOperateBy(), _engine.getSpaceName(), false);
                                context.setOperationAnswer(template, null, exv);
                            } else {
                                context.setOperationAnswer(template, null, null);
                            }
                            if (template.isInCache() && !template.isDeleted())
                                _cacheManager.removeTemplate(context, template,
                                        false /*fromReplication*/, true /*origin*/, false);

                            return;
                        }
                        if (template.isExplicitInsertionToExpirationManager() && template.isInCache() && !template.isInExpirationManager())
                            _cacheManager.getTemplateExpirationManager().addTemplate(template);

                    }//synchronized (templateLock)
                } catch (TemplateDeletedException ex) {
    				/* do nothing - whoever deleted this template notified receiver */
                }
            } else { /* entry != null */
    			/* do nothing, because performTemplateOnEntrySA notified Receiver */
            }
        } catch (Exception ex) {
            LogLevel level = LogLevel.SEVERE;
            if (ex instanceof ProtectiveModeException)
                level = LogLevel.DEBUG;
            else if (ex instanceof ChangeInternalException) {
                ex = ((ChangeInternalException) ex).getInternalException();
                level = LogLevel.DEBUG;
            }
            // Log internal error...
            if (level.isEnabled(_logger)) {
                level.log(_logger, "Error handling read/take/change.", ex);
            }

            if (templateLock == null)
                templateLock = getTemplateLockObject(template);
            synchronized (templateLock) {
                if (!template.hasAnswer())
                    context.setOperationAnswer(template, null, ex);
                if (template.isInCache() && !template.isDeleted()) {
                    try {
                        _cacheManager.removeTemplate(context,
                                template,
                                false /* updateRedoLog */,
                                true /* origin */,
                                false);
                    } catch (Exception e) {
                        // Log internal error...
                        if (_logger.isErrorEnabled()) {
                            _logger.error(
                                    "Internal Error failed to removeTemplate: ",
                                    ex);
                        }
                    }
                }
            }
        } finally {
            if (templateLock != null)
                freeTemplateLockObject(templateLock);
        }
    }


    /*
        try to unpin a batch of candidates- called after bulk op flushing
     */
    void processBlobStoreUnpinCandidatesFromBulk(List<IEntryCacheInfo> candidates) {
        Context context = null;
        try {
            context = _cacheManager.getCacheContext();

            for (IEntryCacheInfo e : candidates) {
                IBlobStoreRefCacheInfo oh = (IBlobStoreRefCacheInfo) e;
                if (e.isDeleted())
                    continue;
                IEntryHolder entry = oh.getEntryHolderIfInMemory();
                if (entry == null)
                    continue;

                ILockObject entryLock = null;
                try {
                    entryLock = getEntryLockObject(entry);
                    synchronized (entryLock) {
                        if (e.isDeleted() || !e.isPinned())
                            continue;
                        entry = oh.getEntryHolderIfInMemory();
                        _cacheManager.unpinIfNeeded(context, entry, null, e);
                    } /* synchronized(entryLock) */
                } finally {
                    if (entryLock != null)
                        freeEntryLockObject(entryLock);
                    entryLock = null;
                }
            }
        } finally {
            if (context != null)
                _cacheManager.freeCacheContext(context);
        }

    }


    /**
     * Handles check xtn state in TM .
     */
    public void handleCheckXtnStatusInTm(CheckXtnStatusInTmBusPackect packet) {
        boolean aliveInTm = false;
        int state;
        try {
            state = packet.getTx().getState();
            aliveInTm = state != TransactionConstants.ABORTED;
        } catch (Exception ex) {
        }

        //notify waiter
        synchronized (packet.getNotifyObj()) {
            packet.setNotAbortedLiveTxn(aliveInTm);
            packet.setHasAnswer(true);
            packet.getNotifyObj().notify();
        }
    }


}
