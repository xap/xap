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

package com.j_spaces.core.fifo;

import com.gigaspaces.events.NotifyActionType;
import com.gigaspaces.internal.server.space.FifoSearch;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.events.NotifyContextsHolder;
import com.gigaspaces.internal.server.space.events.UpdateNotifyContextHolder;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.j_spaces.core.EntryDeletedException;
import com.j_spaces.core.FifoException;
import com.j_spaces.core.NoMatchException;
import com.j_spaces.core.PendingFifoSearch;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.TemplateDeletedException;
import com.j_spaces.core.TransactionConflictException;
import com.j_spaces.core.TransactionNotActiveException;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.locks.ILockObject;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 8.0.4
 */
public abstract class FifoWorkerThread extends GSThread {
    protected static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_FIFO);

    protected final int _threadNum;
    protected final boolean _isNotify;
    private final SpaceEngine _engine;
    protected final CacheManager _cacheManager;
    protected final Object _shutDown;

    protected Context _context;
    protected boolean _closed;
    protected boolean _closeThread;


    protected FifoWorkerThread(int num, boolean isNotify, String workerType,
                               CacheManager cacheManager, SpaceEngine engine) {
        super("[" + engine.getFullSpaceName() + "] " + workerType + "#" + num + "_Notify=" + isNotify);
        _threadNum = num;
        _isNotify = isNotify;
        _shutDown = new Object();
        _engine = engine;
        _cacheManager = cacheManager;
    }

    public abstract void close();

    public abstract void positionRequest(FifoBackgroundRequest rd);

    public abstract void activateFifoThread();

    public abstract void positionAndActivateRequest(FifoBackgroundRequest rd);

    protected NotifyActionType getNotifyType(int spaceOperation) {
        if (!_cacheManager.getTemplatesManager().anyFifoTemplates())
            return null;

        switch (spaceOperation) {
            case SpaceOperations.WRITE:
                return _cacheManager.getTemplatesManager().anyNotifyFifoWriteTemplates(_threadNum) ? NotifyActionType.NOTIFY_WRITE : null;
            case SpaceOperations.UPDATE:
                return _cacheManager.getTemplatesManager().anyNotifyFifoUpdateTemplates(_threadNum) ? NotifyActionType.NOTIFY_UPDATE : null;
            case SpaceOperations.TAKE:
            case SpaceOperations.TAKE_IE:
                return _cacheManager.getTemplatesManager().anyNotifyFifoTakeTemplates(_threadNum) ? NotifyActionType.NOTIFY_TAKE : null;
            case SpaceOperations.LEASE_EXPIRATION:
                return _cacheManager.getTemplatesManager().anyNotifyFifoLeaseExpirationTemplates(_threadNum) ? NotifyActionType.NOTIFY_LEASE_EXPIRATION : null;
            default:
                return null;
        }
    }

    protected void handleRequest(FifoBackgroundRequest rd) {
        // reset replication state
        if (_context != null)
            _context.setFromReplication(false);

        try {
            if (_isNotify)
                handleNotifyRequest(rd);
            else
                handleNonNotifyRequest(rd);
        } catch (EntryDeletedException e) {

        } catch (SAException e) {
            if (_logger.isErrorEnabled())
                _logger.error("FifoWorkerThread caught SAException notify=" + _isNotify + "  ", e);
        } catch (Exception ex) {
            if (_logger.isErrorEnabled())
                _logger.error("FifoWorkerThread caught unhandeled exception notify=" + _isNotify + "  ", ex);
        }
    }

    protected abstract void handleNotifyRequest(FifoBackgroundRequest rd)
            throws SAException, EntryDeletedException;

    protected void handleNotifyRequest(FifoBackgroundRequest rd, NotifyActionType notifyType)
            throws EntryDeletedException, SAException {
        if (_context == null) {
            _context = _cacheManager.getCacheContext();
            _context.setFifoThreadNumber(_threadNum);
        }

        _context.setFromReplication(rd.isFromReplication());
        _context.setOperationID(rd.getOperationID());
        _context.setOperationVisibilityTime(rd.getTime());

        IEntryHolder notify_eh = rd.getCloneEntry() != null ? rd.getCloneEntry() : rd.getEntry().createCopy();

        NotifyContextsHolder notifyContextsHolder = null;
        if (rd.getXtn() == null)
        // if txn!=null, in scanNotifyTemplates all templates will be rejected.
        // (after commit will be executed the Processor.handleNotifyFifoInCommit will create a FifoBackgroundRequest with txn=null,
        // 	here rd.getXtn() will be null and then scanNotifyTemplates will send the suitable notifications).
        {
            if (notifyType.equals(NotifyActionType.NOTIFY_UPDATE))
                notifyContextsHolder = new UpdateNotifyContextHolder(rd.getOriginalEntry(), notify_eh, _context.getOperationID(), _cacheManager.getTemplatesManager().anyNotifyMatchedTemplates()
                        , _cacheManager.getTemplatesManager().anyNotifyRematchedTemplates());
            else if (notifyType.equals(NotifyActionType.NOTIFY_TAKE))
                notifyContextsHolder = new NotifyContextsHolder(notify_eh, null, _context.getOperationID(), notifyType);
            else
                notifyContextsHolder = new NotifyContextsHolder(rd.getOriginalEntry(), notify_eh, _context.getOperationID(), notifyType);
            _engine.getTemplateScanner().scanNotifyTemplates(notifyContextsHolder, _context, null /* txn */, FifoSearch.YES);

            // handle the UNMATCHED case when notifications on the original entry should be send
            if (rd.getOriginalEntry() != null && _cacheManager.getTemplatesManager().anyNotifyUnmatchedTemplates()) {
                notifyContextsHolder = new NotifyContextsHolder(rd.getOriginalEntry(), notify_eh, _context.getOperationID(), NotifyActionType.NOTIFY_UNMATCHED);
                _engine.getTemplateScanner().scanNotifyTemplates(notifyContextsHolder, _context, null /* txn */, FifoSearch.YES);
            }
        }
    }

    protected void performTemplateOnEntryNonNotify(IEntryHolder eh, ITemplateHolder template, FifoBackgroundRequest rd)
            throws SAException, EntryDeletedException {
        _context.setRecentFifoObject(rd);
        _context.setOperationID(rd.getOperationID());
        _context.setLastRawMatchSnapshot(null);

        try {
            _engine.performTemplateOnEntrySA(_context, template, eh,
                    template.isIfExist()/*makeWaitForInfo*/);
        } catch (TransactionConflictException ex) {
        } catch (TemplateDeletedException ex) {
        } catch (TransactionNotActiveException ex) {
        } catch (NoMatchException ex) {
        } catch (FifoException ex) {
        }
    }

    /**
     * perform general non-notify
     */
    protected void performGeneralEntryNonNotify(IEntryHolder eh, FifoBackgroundRequest rd)
            throws SAException, EntryDeletedException {
        _context.setRecentFifoObject(rd);
        _context.setOperationID(rd.getOperationID());
        _engine.getTemplateScanner().scanNonNotifyTemplates(_context,
                eh, rd.getXtn(), FifoSearch.YES);
    }

    protected void processRejectedRequestsToPerform(FifoBackgroundRequest rd, List<FifoBackgroundRequest> rejectedRequestsToPerform)
            throws SAException, EntryDeletedException {
        if (rejectedRequestsToPerform != null) {
            for (FifoBackgroundRequest delayedRd : rejectedRequestsToPerform) {
                if (rd.getTemplate() != null) {
                    if (rd.getTemplate().isDeleted())
                        return;
                    performTemplateOnEntryNonNotify(delayedRd.getEntry(), rd.getTemplate(), delayedRd);
                } else
                    performGeneralEntryNonNotify(delayedRd.getEntry(), delayedRd);
            }
        } else {
            if (rd.getTemplate() != null) {
                if (rd.getTemplate().isDeleted())
                    return;
                performTemplateOnEntryNonNotify(rd.getEntry(), rd.getTemplate(), rd);
            } else
                performGeneralEntryNonNotify(rd.getEntry(), rd);
        }
    }

    protected void handleNonNotifyRequest(FifoBackgroundRequest rd)
            throws SAException, EntryDeletedException {
        if (!_cacheManager.getTemplatesManager().anyNonNotifyFifoTemplates(_threadNum) && rd.getTemplate() == null)
            return;

        if (_context == null) {
            _context = _cacheManager.getCacheContext();
            _context.setFifoThreadNumber(_threadNum);
        }

        PendingFifoSearch pfs = null;
        ArrayList<FifoBackgroundRequest> rejectedRequestsToPerform = null;

        try {
            if (rd.getTemplate() != null && rd.getTemplate().isDeleted())
                return;//(not relevant)
            //if this is a blocking fifo from initial search-get its entries & process
            if (rd.getTemplate() != null && rd.getTemplate().getPendingFifoSearchObject() != null) {
                ILockObject templateLock = null;
                //lock the template
                try {
                    templateLock = _cacheManager.getLockManager().getLockObject(rd.getTemplate(), false/*isEvictable*/);
                    synchronized (templateLock) {
                        if (rd.getTemplate().isDeleted())
                            return;
                        pfs = rd.getTemplate().getPendingFifoSearchObject();
                        rejectedRequestsToPerform = pfs.getRejectedEntries();
                        rd.getTemplate().removePendingFifoSearchObject(true /*disableinitialsearch*/);
                    }
                } finally {
                    if (templateLock != null)
                        _cacheManager.getLockManager().freeLockObject(templateLock);
                }
            }

            processRejectedRequestsToPerform(rd, rejectedRequestsToPerform);
        } finally {
            if (pfs != null)
                pfs.notifyNonActiveIfNeedTo();//in case any one waiting for this to terminate
        }
    }
}
