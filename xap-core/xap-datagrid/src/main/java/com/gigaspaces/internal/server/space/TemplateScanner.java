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

package com.gigaspaces.internal.server.space;

import com.gigaspaces.events.NotifyActionType;
import com.gigaspaces.internal.query.RegexCache;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.events.NotifyContext;
import com.gigaspaces.internal.server.space.events.NotifyContextsHolder;
import com.gigaspaces.internal.server.space.events.SpaceDataEventManager;
import com.gigaspaces.internal.server.space.events.UpdateNotifyContextHolder;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.server.storage.ITransactionalEntryData;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.j_spaces.core.EntryDeletedException;
import com.j_spaces.core.FifoException;
import com.j_spaces.core.NoMatchException;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.TemplateDeletedException;
import com.j_spaces.core.TransactionConflictException;
import com.j_spaces.core.TransactionNotActiveException;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.cache.*;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.ICollection;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.IStoredListIterator;

import com.j_spaces.kernel.list.ScanSingleListIterator;
import net.jini.core.transaction.server.ServerTransaction;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Niv Ingberg
 * @since 8.0.4
 */
@com.gigaspaces.api.InternalApi
public class TemplateScanner {
    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_SPACE_TEMPLATE_SCANNER);

    private final SpaceEngine _engine;
    private final SpaceTypeManager _typeManager;
    private final CacheManager _cacheManager;
    private final SpaceDataEventManager _dataEventManager;
    private final RegexCache _regexCache;

    public TemplateScanner(SpaceTypeManager typeManager, CacheManager cacheManager,
                           SpaceDataEventManager dataEventManager, SpaceEngine engine) {
        this._typeManager = typeManager;
        this._cacheManager = cacheManager;
        this._dataEventManager = dataEventManager;
        this._regexCache = new RegexCache(engine.getConfigReader());
        this._engine = engine;
    }

    //TODO test with template null
    //TODO test with template with index that is not spaceId
    public void cancelAllNonNotifyTemplates(Exception ex) {
        Map<String, IServerTypeDesc> safeTypeTable = _typeManager.getSafeTypeTable();
        if (safeTypeTable == null) return;
        for (IServerTypeDesc serverTypeDesc : safeTypeTable.values()) {
            TypeData typeData = _cacheManager.getTypeData(serverTypeDesc);
            if (typeData == null) continue;

            //ReadTakeUidTemplates
            cancelTemplates(typeData.getReadTakeUidTemplates(), ex);

            //ReadTakeTemplates
            cancelTemplates(typeData.getReadTakeTemplates(), ex);
            //ReadTakeExtendedTemplates
            cancelTemplates(typeData.getReadTakeExtendedTemplates(), ex);

            //IndexedTemplates
            if (typeData.getIndexes() != null) {
                for (TypeDataIndex typeDataIndex : typeData.getIndexes()) {
                    //ReadTakeTemplates
                    ConcurrentHashMap<Object, IStoredList<TemplateCacheInfo>[]> rtTemplates = typeDataIndex._RTTemplates;
                    if (rtTemplates != null) {
                        for (IStoredList<TemplateCacheInfo>[] iStoredLists : rtTemplates.values()) {
                            for (IStoredList<TemplateCacheInfo> iStoredList : iStoredLists) {
                                cancelTemplates(iStoredList, ex);
                            }
                        }
                    }

                    //ReadTakeNullTemplates
                    cancelTemplates(typeDataIndex._RTNullTemplates, ex);

                    //ReadTake GT,LT,NE Index Templates
                    if (typeDataIndex.getM_RT_GT_Index() != null) {
                        cancelTemplates(typeDataIndex.getM_RT_GT_Index().getOrderedStore(), ex);
                    }
                    if (typeDataIndex.getM_RT_LT_Index() != null) {
                        cancelTemplates(typeDataIndex.getM_RT_LT_Index().getOrderedStore(), ex);
                    }
                    if (typeDataIndex.getM_RT_NE_Index() != null) {
                        cancelTemplates(typeDataIndex.getM_RT_NE_Index().getOrderedStore(), ex);
                    }
                }
            }

        }
    }

    private void cancelTemplates(Map<?, IStoredList<TemplateCacheInfo>> map, Exception ex) {
        if (map == null) return;
        for (IStoredList<TemplateCacheInfo> storedList : map.values()) {
            cancelTemplates(storedList, ex);
        }
    }


    private void cancelTemplates(IStoredList<TemplateCacheInfo> templates, Exception ex) {
        if (templates == null) return;

        if (templates.isEmpty()) return;

        ScanSingleListIterator<TemplateCacheInfo> iterator = new ScanSingleListIterator<TemplateCacheInfo>(templates, false);
        //TODO consider using iterator.reuse();
        try {
            while (iterator.hasNext()) {
                ITemplateHolder templateHolder = iterator.next().getSubject().m_TemplateHolder;
                TemplateExpirationManager.cancelTemplateHolder(templateHolder, ex, _cacheManager, false, null);

            }
        } finally {
            iterator.releaseScan();
        }
    }



    public RegexCache getRegexCache() {
        return _regexCache;
    }

    /**
     * Search for templates that match the specified entry and perform each such template.
     */
    public void scanNonNotifyTemplates(Context context, IEntryHolder entry,
                                       ServerTransaction txn, FifoSearch fifoSearch)
            throws EntryDeletedException, SAException {
        scanNonNotifyTemplates(context, entry,
                txn, fifoSearch, fifoSearch == FifoSearch.YES ? FifoGroupsSearch.NO : FifoGroupsSearch.INCLUSIVE /*fifoGroupTemplatesOnly*/);
    }

    /**
     * Search for templates that match the specified entry and perform each such template.
     */
    public void scanNonNotifyTemplates(Context context, IEntryHolder entry,
                                       ServerTransaction txn, FifoSearch fifoSearch, FifoGroupsSearch fifoGroupsSearch)
            throws EntryDeletedException, SAException {
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "Starting scan of non-notify templates: EntryUid=" + entry.getUID() + ", fifoSearch=" + fifoSearch);

        context.setExhaustedFifoGroupsTemplatesSearch(false);
        scanTemplates(context, entry, txn, fifoSearch, null /*notifyContextsHolder*/, fifoGroupsSearch);

        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "Finished scan of non-notify templates: EntryUid=" + entry.getUID() + ", fifoSearch=" + fifoSearch);
    }

    public void scanNotifyTemplates(NotifyContextsHolder notifyContextsHolder, Context context, ServerTransaction txn, FifoSearch fifoSearch)
            throws EntryDeletedException, SAException {
        String entryUid = notifyContextsHolder.getNotifyEntry().getUID();
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "Starting scan of notify templates: EntryUid=" + entryUid +
                    ", fifoSearch=" + fifoSearch +
                    ", notifyTypes=" + notifyContextsHolder.getNotifyTypes());


        scanTemplates(context, notifyContextsHolder.getNotifyEntry(), txn, fifoSearch, notifyContextsHolder, FifoGroupsSearch.NO);

        for (NotifyContext notifyContext : notifyContextsHolder.getNotifyContexts()) {
            //	marks that all the templates for this entry was already found
            notifyContext.setFinishedTemplateSearch(true);
            _dataEventManager.finishTemplatesSearch(notifyContext);

            if (_logger.isLoggable(Level.FINE))
                _logger.log(Level.FINE, "Finished scan of notify templates: EntryUid=" + entryUid +
                        ", fifoSearch=" + fifoSearch +
                        ", notifyType=" + notifyContext.getNotifyType().toString());
        }

    }

    private void scanTemplates(Context context, IEntryHolder entry,
                               ServerTransaction txn, FifoSearch fifoSearch, NotifyContextsHolder notifyContextsHolder, FifoGroupsSearch fifoGroupsSearch)
            throws EntryDeletedException, SAException {
        EntryDeletedException exp = null;
        final MatchTarget matchTarget = notifyContextsHolder != null ? MatchTarget.NOTIFY : MatchTarget.READ_TAKE;
        final IServerTypeDesc entryTypeDesc = _typeManager.getServerTypeDesc(entry.getClassName());
        try {
            for (IServerTypeDesc typeDesc : entryTypeDesc.getSuperTypes()) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.log(Level.FINE, "Scanning templates of type [" + typeDesc.getTypeName() + "]: entryUid=" + entry.getUID() + ", fifoSearch=" + fifoSearch);
                if (typeDesc.isInactive())
                    continue;
                if (context.isExhaustedFifoGroupsTemplatesSearch() && fifoGroupsSearch == FifoGroupsSearch.EXCLUSIVE)
                    return;
                final Object searchResult = _cacheManager.findTemplatesByIndex(context, typeDesc, entry, matchTarget);
                if (searchResult == null)
                    continue;
                try {
                    scanTemplatesByIndexSearchResult(context, entry, txn, fifoSearch, notifyContextsHolder, searchResult, fifoGroupsSearch);
                } catch (EntryDeletedException ex) {
                    if (notifyContextsHolder != null || fifoGroupsSearch == FifoGroupsSearch.NO)
                        throw ex;
                    exp = ex;
                    fifoGroupsSearch = FifoGroupsSearch.EXCLUSIVE;
                }
            }
        } finally {
            if (exp != null)
                throw exp;

        }
    }

    private void scanTemplatesByIndexSearchResult(Context context, IEntryHolder entry,
                                                  ServerTransaction txn, FifoSearch fifoSearch, NotifyContextsHolder notifyContextsHolder, Object searchResult, FifoGroupsSearch fifoGroupsSearch)
            throws EntryDeletedException, SAException {
        EntryDeletedException exp = null;
        try {
            if (searchResult instanceof IStoredList)
                scanTemplatesByIndex(context, entry, txn, fifoSearch, notifyContextsHolder, (ICollection<TemplateCacheInfo>) searchResult, fifoGroupsSearch);
            else if (searchResult instanceof IExtendedIndexIterator) {
                IExtendedIndexIterator<TemplateCacheInfo> ois = (IExtendedIndexIterator<TemplateCacheInfo>) searchResult;
                try {
                    while (ois.hasNext()) {
                        if (fifoGroupsSearch == FifoGroupsSearch.EXCLUSIVE && context.isExhaustedFifoGroupsTemplatesSearch())
                            return;
                        try {
                            scanTemplatesByIndexSearchResult(context, entry, txn, fifoSearch, notifyContextsHolder, ois.next(), fifoGroupsSearch);
                        } catch (EntryDeletedException ex) {
                            if (fifoGroupsSearch == FifoGroupsSearch.NO)
                                throw ex;
                            exp = ex;
                            fifoGroupsSearch = FifoGroupsSearch.EXCLUSIVE;
                        }
                    }
                } finally {
                    ois.releaseScan();
                }
            } else if (searchResult instanceof Object[]) {
                for (Object result : (Object[]) searchResult) {
                    try {
                        scanTemplatesByIndexSearchResult(context, entry, txn, fifoSearch, notifyContextsHolder, result, fifoGroupsSearch);
                    } catch (EntryDeletedException ex) {
                        if (fifoGroupsSearch == FifoGroupsSearch.NO)
                            throw ex;
                        exp = ex;
                        fifoGroupsSearch = FifoGroupsSearch.EXCLUSIVE;
                    }
                }
            } else if (searchResult instanceof List) {
                for (Object result : (List<?>) searchResult) {
                    if (fifoGroupsSearch == FifoGroupsSearch.EXCLUSIVE && context.isExhaustedFifoGroupsTemplatesSearch())
                        return;
                    try {
                        scanTemplatesByIndexSearchResult(context, entry, txn, fifoSearch, notifyContextsHolder, result, fifoGroupsSearch);
                    } catch (EntryDeletedException ex) {
                        if (fifoGroupsSearch == FifoGroupsSearch.NO)
                            throw ex;
                        exp = ex;
                        fifoGroupsSearch = FifoGroupsSearch.EXCLUSIVE;
                    }
                }
            } else
                throw new IllegalArgumentException("Unsupported search result class: " + searchResult.getClass().getName());

        } finally {
            if (exp != null)
                throw exp;
        }
    }

    private void scanTemplatesByIndex(Context context, IEntryHolder entry, ServerTransaction txn,
                                      FifoSearch fifoSearch, NotifyContextsHolder notifyContextsHolder, ICollection<TemplateCacheInfo> sl, FifoGroupsSearch fifoGroupsSearch)
            throws EntryDeletedException, SAException {
        if (sl.isEmpty())
            return;
        EntryDeletedException exp = null;
        IStoredListIterator<TemplateCacheInfo> slh = null;
        try {
            for (slh = sl.establishListScan(false); slh != null; slh = sl.isMultiObjectCollection() ? sl.next(slh) : null) {
                TemplateCacheInfo cacheHolder = slh.getSubject();
                if (cacheHolder == null)
                    continue;
                ITemplateHolder template = cacheHolder.m_TemplateHolder;
                if (template == null)
                    continue;
                if (!template.isFifoGroupPoll() && fifoGroupsSearch == FifoGroupsSearch.EXCLUSIVE)
                    continue;
                if (template.isFifoGroupPoll() && fifoGroupsSearch == FifoGroupsSearch.NO)
                    continue;
                if (context.isExhaustedFifoGroupsTemplatesSearch()) {
                    if (fifoGroupsSearch == FifoGroupsSearch.EXCLUSIVE)
                        return;
                    if (template.isFifoGroupPoll())
                        continue;
                }

                if (template.isNotifyTemplate()) {
                    notifyIfMatching((NotifyTemplateHolder) template, notifyContextsHolder, context, fifoSearch, txn);
                } else {
                    try {
                        scanNonNotifyTemplate(template, entry, context, fifoSearch);
                    } catch (EntryDeletedException ex) {
                        if (fifoGroupsSearch == FifoGroupsSearch.NO)
                            throw ex;
                        exp = ex;
                        fifoGroupsSearch = FifoGroupsSearch.EXCLUSIVE;
                    }
                }
            }
        } finally {
            if (slh != null)
                slh.release();
            if (exp != null)
                throw exp;
        }
    }

    private void notifyIfMatching(NotifyTemplateHolder template, NotifyContextsHolder notifyContextsHolder,
                                  Context context, FifoSearch fifoSearch, ServerTransaction txn) {
        if (_logger.isLoggable(Level.FINER))
            _logger.log(Level.FINER, "Scanning template #" + template.getEventId() +
                    " (uid=" + template.getUID() +
                    "): entryUid=" + notifyContextsHolder.getNotifyEntry().getUID()
                    + ", fifoSearch=" + fifoSearch);

        synchronized (template) {
            if (template.quickReject(context, fifoSearch, txn, notifyContextsHolder))
                return;

            if (template.isMaybeUnderXtn()) {
                XtnEntry xtnEntry = _engine.getTransaction(template.getXidOriginatedTransaction());
                if (xtnEntry == null || !xtnEntry.m_Active)
                    return;
            }

            if (!template.isMatchByID()  // backward used only if matching is done by uid only.
                    && !notifyMatch(context, notifyContextsHolder.getNotifyEntry(), template))
                return;

            NotifyContext notifyContext = null;
            NotifyActionType notifyType = notifyContextsHolder.getNotifyType();
            // unmatched notification
            if (notifyType.equals(NotifyActionType.NOTIFY_UNMATCHED)) {
                if (template.isMatchByID()
                        || notifyMatch(context, notifyContextsHolder.getNewEntry(), template))
                    return;
            }
            // can include NOTIFY_UPDATE, NOTIFY_REMATCHED or NOTIFY_MATCHED
            else if (notifyType.equals(NotifyActionType.NOTIFY_UPDATE)) {
                boolean contatinsRematchedNotifyType = template.containsNotifyType(NotifyActionType.NOTIFY_REMATCHED_UPDATE);
                boolean contatinsMatchedNotifyType = template.containsNotifyType(NotifyActionType.NOTIFY_MATCHED_UPDATE);

                // check if matched or re-matched notifications exist in the notify template.
                if (contatinsRematchedNotifyType || contatinsMatchedNotifyType) {
                    UpdateNotifyContextHolder updateHolder = (UpdateNotifyContextHolder) notifyContextsHolder;

                    if (notifyMatch(context, notifyContextsHolder.getOriginalEntry(), template)) {
                        // the original entry is also matched, check if this is a NOTIFY_REMATCHED.
                        if (contatinsRematchedNotifyType)
                            notifyContext = updateHolder.getRematchedNotifyContext();
                    } else if (contatinsMatchedNotifyType)
                        // the original entry is not matched - this is the first time the new entry is matched, check if it is a NOTIFY_MATCHED.
                        notifyContext = updateHolder.getMatchedNotifyContext();

                    if (notifyContext == null && !template.containsNotifyType(NotifyActionType.NOTIFY_UPDATE))
                        // no need to send notification for this (updated) entry
                        return;
                }
            }


            if (_logger.isLoggable(Level.FINER))
                _logger.log(Level.FINER, "Match found: template #" + template.getEventId() +
                        " (uid=" + template.getUID() +
                        "), entryUid=" + notifyContextsHolder.getNotifyEntry().getUID()
                        + ", fifoSearch=" + fifoSearch);

            //This will be the result notify context if notifyContextsHolder doesn't contain the matched or re-matched notification types.
            if (notifyContext == null)
                notifyContext = notifyContextsHolder.getNotifyContext();
            _dataEventManager.notifyTemplate(template,
                    notifyType.equals(NotifyActionType.NOTIFY_UNMATCHED) ? notifyContext.getReferenceEntry() : notifyContextsHolder.getNotifyEntry(),
                    notifyContextsHolder.getOriginalEntry(), notifyContext, context);
        }
    }

    private void scanNonNotifyTemplate(ITemplateHolder template, IEntryHolder entry,
                                       Context context, FifoSearch fifoSearch)
            throws EntryDeletedException, SAException {
        if (_logger.isLoggable(Level.FINER))
            _logger.log(Level.FINER, "Scanning template " + template.getUID() + ": entryUid=" + entry.getUID()
                    + ", fifoSearch=" + fifoSearch);
        if (template.quickReject(context, fifoSearch))
            return;

        try {
            if (template.isMatchByID() && template.isNonBlockingRead()) {
                ITransactionalEntryData ed = entry.getTxnEntryData();
                context.setLastRawMatchSnapshot(ed);
                context.setLastMatchResult(ed.getOtherUpdateUnderXtnEntry() != null ? MatchResult.MASTER_AND_SHADOW : MatchResult.MASTER);
                context.setUnstableEntry(entry.isUnstable());
            }
            if (!template.isMatchByID()  // backward used only if matching is done by uid only.
                    && !match(context, entry, template))
                return;

            boolean ifExist =
                    template.getTemplateOperation() == SpaceOperations.READ_IE ||
                            template.getTemplateOperation() == SpaceOperations.TAKE_IE ||
                            template.getTemplateOperation() == SpaceOperations.UPDATE;

            if (template.isFifoGroupPoll()) {
                if (!context.isExhaustedFifoGroupsTemplatesSearch())
                    _engine.getFifoGroupsHandler().scanNonNotifyTemplate(template, entry,
                            context, ifExist);
                return;
            }

            _engine.performTemplateOnEntrySA(context, template, entry,
                    ifExist/*makeWaitingFor*/);
        } catch (TemplateDeletedException e) {
        } catch (NoMatchException e) {
        } catch (FifoException e) {
        } catch (TransactionConflictException e) {
        } catch (TransactionNotActiveException e) {
        }
    }

    /**
     * Checks if the specified template matches the specified entry. Assumes template is a
     * superclass of entry (in the wider sense). Assumes template and entry are not deleted, expired
     * .
     *
     * @param entry     the entry to check
     * @param template  the template to check
     * @param skipIndex if != -1, the method assumes the template field at skipIndex matches the
     *                  entry field at skipIndex, and hence does not check this index.
     */
    public boolean match(Context context, IEntryHolder entry, ITemplateHolder template,
                         int skipAlreadyMatchedFixedPropertyIndex, String skipAlreadyMatchedIndexPath, boolean safeEntry) {
        MatchResult res = template.match(_cacheManager, entry, skipAlreadyMatchedFixedPropertyIndex, skipAlreadyMatchedIndexPath, safeEntry, context, _regexCache);
        return res != MatchResult.NONE;
    }

    public boolean match(Context context, IEntryHolder entry, ITemplateHolder template) {
        MatchResult res = template.match(_cacheManager, entry, -1, null, false, context, _regexCache);
        return res != MatchResult.NONE;
    }

    public boolean notifyMatch(Context context, IEntryHolder entry, ITemplateHolder template) {
        MatchResult res = template.match(_cacheManager, entry, -1, null, false, context, _regexCache);
        return res == MatchResult.MASTER || res == MatchResult.MASTER_AND_SHADOW;
    }

}
