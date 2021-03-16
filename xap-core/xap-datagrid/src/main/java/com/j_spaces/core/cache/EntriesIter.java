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

import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.ObjectIntegerMap;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.MatchResult;
import com.gigaspaces.internal.server.space.tiered_storage.MultiTypedRDBMSISIterator;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.server.storage.ITransactionalEntryData;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.XtnStatus;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.blobStore.IBlobStoreRefCacheInfo;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.list.IScanListIterator;
import com.j_spaces.kernel.list.ScanSingleListIterator;

import java.util.HashSet;
import java.util.Random;

/**
 * Iterator for entries.
 */
@com.gigaspaces.api.InternalApi
public class EntriesIter extends SAIterBase implements ISAdapterIterator<IEntryHolder> {
    private boolean _notRefreshCache;
    private IServerTypeDesc[] _types;
    private int _currentClass;
    private int _actualClass;
    private IScanListIterator<IEntryCacheInfo> _entries;
    final private ITemplateHolder _templateHolder;
    final private long _SCNFilter;
    final private long _leaseFilter;
    private IEntryCacheInfo _currentEntryCacheInfo;
    private IEntryHolder _currentEntryHolder;
    private boolean _doneWithCache;
    private HashSet<String> _entriesReturned;
    private ISAdapterIterator<IEntryHolder> _saIter;
    private boolean _firstFromCache = true;
    //if true bring entries only from memory and not the DB
    //NOTE-meaningless in FIFO. ignored
    final private boolean _memoryOnly;
    final private boolean _transientOnly;

    //FIFO-related +++++++++++++++++++++++++++++++++++++++++++
    private IEntryHolder _entryFromCache;
    private IEntryHolder _entryFromSA;
    private boolean _first = true;
    private ITypeDesc _typeDesc;
    private ObjectIntegerMap<String> _fifoClassesOrder;
    final private IServerTypeDesc _templateServerTypeDesc;
    //ignore entry holder- used in off heap in order not to bring EH to memory for count if possible
    private boolean _returnEntryCacheInfoOnly;
    private boolean _tryToUsePureIndexesBlobStore;

    //for by-uid or by-uids count
    private String[] _uids;
    private int      _nextUidPos;

    private static final Random randomGenerator = new Random();

    public EntriesIter(Context context, ITemplateHolder template, IServerTypeDesc serverTypeDesc,
                       CacheManager cacheManager, long SCNFilter, long leaseFilter,
                       boolean memoryOnly, boolean transientOnly)
            throws SAException {
        super(context, cacheManager);

        _templateServerTypeDesc = serverTypeDesc;
        _doneWithCache = false;  // start with cache, proceed with SA

        if(_cacheManager.isTieredStorage()){
            if(context.getTemplateTieredState() == null){
                context.setTemplateTieredState(_cacheManager.getEngine().getTieredStorageManager().guessTemplateTier(template));
            }

            if(context.getTemplateTieredState() == TemplateMatchTier.MATCH_HOT || memoryOnly || template.isMemoryOnlySearch()){
                _memoryOnly = true;
            } else {
                _memoryOnly = memoryOnly;
                if(context.getTemplateTieredState() == TemplateMatchTier.MATCH_COLD){
                    _doneWithCache = true;
                }


                if(context.getTemplateTieredState() == TemplateMatchTier.MATCH_HOT_AND_COLD){
                    _entriesReturned = new HashSet<>();
                }

            }

            _notRefreshCache = true;

        } else {
            _memoryOnly = template.isMemoryOnlySearch() || memoryOnly;
        }

        _transientOnly = transientOnly;
        _templateHolder = template;
        _SCNFilter = SCNFilter;
        _leaseFilter = _cacheManager.isBlobStoreCachePolicy() ? 0 : leaseFilter;

        if (!template.isEmptyTemplate()) {
            _typeDesc = _cacheManager.getTypeManager().getTypeDesc(template.getClassName());
        }

        if (template.getUidToOperateBy() != null || template.getMultipleUids() != null)
        {
            if (template.getMultipleUids() != null)
            {
                _uids = template.getMultipleUids();
            }
            else
            {
                _uids = new String[1];
                _uids[0] = template.getUidToOperateBy() ;
            }
            if (_cacheManager.isBlobStoreCachePolicy() && template.getCustomQuery() == null && template.isEmptyTemplate() )
                setBringCacheInfoOnly(true);

            return;
        }

		/* [@author moran]
         * TODO#1 currently m_Classes and m_ClassEnumeration act as the same source
		 * pair. One for String the other ints, but both correspond to the same
		 * data. m_Classes should be removed, but as part of the current scalability
		 * effort, m_ClassEnumeration has been introduced. Note that to fully
		 * remove it, IStorageAdapter needs to support class-numeration and also
		 * SALeaseManager needs to support numeric-cells and not String-cells of
		 * classname. 
		 */
        _types = serverTypeDesc.getAssignableTypes();

        //FIFO++++++++++++++++++++++++++++++++++++++++++=
        //fifo not for all classes- return only relevant classes from SA
        if (_templateHolder.isFifoTemplate()) {
            int numOfFifoClasses = 0;
            boolean[] isFifo = new boolean[_types.length];

			/* [@author moran]
             * TODO#2 should be removed once m_Classes is deprecated
			 * and adapted to use the fragment of code below:
			 * PType p = m_CacheManager.m_Root.m_ClassEnumeration.get( m_ClassEnumeration[i] ); //get by class key
			 * if (p!=null && p.m_FifoSupport)
			 * {
			 *     isFifo[i] = true;
			 *     numOfFifoClasses++;
			 * }
			 * 
			 * instead of relying on m_FifoClassNames Set.
			 */
            for (int i = 0; i < _types.length; i++) {
                if (!_cacheManager.getTypeManager().isFifoType(_types[i]))
                    continue;
                isFifo[i] = true;
                numOfFifoClasses++;
            }//for
            if (numOfFifoClasses == 0) {
                _types = null;//    no fifo-enabled class, no match
                return;
            }
            if (numOfFifoClasses < _types.length) {
                IServerTypeDesc[] onlyFifoTypes = new IServerTypeDesc[numOfFifoClasses];
                int j = 0;
                for (int i = 0; i < _types.length; i++)
                    if (isFifo[i])
                        onlyFifoTypes[j++] = _types[i];

                _types = onlyFifoTypes;
            }
            //set number per  class in order to enable cache/persistent comparison
            _fifoClassesOrder = CollectionsFactory.getInstance().createObjectIntegerMap();
            for (int k = 0; k < _types.length; k++)
                _fifoClassesOrder.put(_types[k].getTypeName(), k);
        }//if (m_TemplateHolder.m_FifoTemplate
        //FIFO-------------------------------------------=

        if (_cacheManager.isEvictableCachePolicy() && !_cacheManager.isMemorySpace())
            _entriesReturned = new HashSet<String>();

        /**
         * perform anti-starvation random scan
         */
        if (_types.length > 1) {
            int size = _types.length - 1;
            // random start
            if (!_templateHolder.isFifoTemplate())
                _actualClass = getMathRandom(size);
        }

        if(_cacheManager.isTieredStorage() && _doneWithCache){
            //TODO - tiered storage - handle null template
            _saIter = new MultiTypedRDBMSISIterator(_cacheManager.getEngine().getTieredStorageManager().getInternalStorage(), _context, _types, _templateHolder);
        }

    }


    private int getMathRandom(int size) {
        return randomGenerator.nextInt(size);
    }

    public IEntryHolder next()
            throws SAException {

        if(_cacheManager.isTieredStorage() && _context.getTemplateTieredState() == TemplateMatchTier.MATCH_COLD && _memoryOnly){
            return null;
        }

        if (_uids != null)
            return next_by_uid();


        //FIFO++++++++++++++++++++++++++++++++++++++++++=
        if (_types == null)
            return null;
        boolean memoryOverTimebasedeviction = _memoryOnly && _cacheManager.isTimeBasedEvictionStrategy();
        if (_templateHolder.isFifoTemplate() && _cacheManager.isEvictableCachePolicy() && !_cacheManager.isMemorySpace() && !memoryOverTimebasedeviction) {
            return next_fifo();
        }
        //FIFO-------------------------------------------=
        while (!_doneWithCache) {
            checkIfNext();
            if (_currentEntryCacheInfo == null) {
                // finished with cache, starting with SA
                _doneWithCache = true;
                if (_cacheManager.isEvictableCachePolicy() &&
                        !_cacheManager.isMemorySpace() && !_memoryOnly && !_transientOnly) {

                    _saIter = _cacheManager.getStorageAdapter().makeEntriesIter(_templateHolder,
                            _SCNFilter, _leaseFilter, _types);
                }

                if(_cacheManager.isTieredStorage() && _context.getTemplateTieredState() != TemplateMatchTier.MATCH_HOT && _saIter == null && !_memoryOnly){
                    //TODO - tiered storage - handle multiple types
                    _saIter = new MultiTypedRDBMSISIterator(_cacheManager.getEngine().getTieredStorageManager().getInternalStorage(), _context, _types, _templateHolder);
                }

                return saIterNext();
            }

            //iterate over cache
            if (_cacheManager.isEvictableCachePolicy() && !_cacheManager.isMemorySpace() &&
                    !_currentEntryHolder.isTransient() && !_memoryOnly) {
                if (!(_entriesReturned.add(_currentEntryCacheInfo.getUID())))
                    continue; //already returned

            }
            return _currentEntryHolder;
        } //while not done with cache

        return saIterNext();
    }

    public IEntryHolder next_by_uid()
            throws SAException
    {
        _currentEntryCacheInfo = null;
        _currentEntryHolder = null;
        while (_nextUidPos < _uids.length)
        {
            //NOTE: get by uids is irrelevant for DB located entries
            String uid = _uids[_nextUidPos++];
            IEntryCacheInfo pEntry = null;
            IEntryHolder  eh = null;
            pEntry = _cacheManager.getPEntryByUid(uid);
            if (pEntry == null || invalidEntryCacheInfo(pEntry))
                continue;
            if (isBringCacheInfoOnly()) {
                eh = ((IBlobStoreRefCacheInfo) pEntry).getEntryHolderIfInMemory();
                if (eh != null && !match(eh))
                    continue;
            }
            else
            {
                eh = pEntry.getEntryHolder(_cacheManager,_context);
                if (invalidEntry(pEntry, eh) || !match(eh))
                    continue;
            }
            _currentEntryCacheInfo = pEntry;
            _currentEntryHolder = eh;
            return _currentEntryHolder;
        }
        return null;
    }



    public IEntryCacheInfo nextEntryCacheInfo()
            throws SAException {
        next();
        return _currentEntryCacheInfo;
    }

    public Object nextObject()
            throws SAException {
        next();
        if (isBringCacheInfoOnly())
            return _currentEntryCacheInfo;
        else
            return _currentEntryHolder;
    }

    public IEntryCacheInfo getCurrentEntryCacheInfo() {
        return _currentEntryCacheInfo;
    }

    public IEntryHolder getCurrentEntryHolder() {
        return _currentEntryHolder;
    }

    /**
     * next in fifo order
     */
    private IEntryHolder next_fifo()
            throws SAException {
        while (true) {
            if (_first) {//first iteration, open iterator to SA
                _first = false;

                _saIter = _cacheManager.getStorageAdapter().makeEntriesIter(_templateHolder,
                        _SCNFilter, _leaseFilter, _types);

            }
            if (_saIter != null && _entryFromSA == null) {
                _entryFromSA = saIterNext();
                if (_entryFromSA == null) {
                    _saIter.close();
                    _saIter = null;
                }
            }//if (m_SAIter != null && m_EHfromSA == null)

            if (!_doneWithCache && _entryFromCache == null) {
                checkIfNext();
                if (_currentEntryCacheInfo == null)
                    _doneWithCache = true;
                else
                    _entryFromCache = _currentEntryHolder;
            }//if (!m_DoneWithCache && m_EHfromCache == null)

            //which entry to return ?
            if (_entryFromCache == null && _entryFromSA == null)
                return null;

            IEntryHolder res = null;
            boolean returnSA = false;
            if (_entryFromCache != null && _entryFromSA != null) {
                if (_entryFromCache.getClassName().equals(_entryFromSA.getClassName())) {
                    if (_entryFromSA.getSCN() < _entryFromCache.getSCN() ||
                            (_entryFromSA.getSCN() == _entryFromCache.getSCN() && _entryFromSA.getOrder() < _entryFromCache.getOrder()))
                        returnSA = true;
                } else {
                    int classSa = _fifoClassesOrder.get(_entryFromSA.getClassName());
                    int classCache = _fifoClassesOrder.get(_entryFromCache.getClassName());
                    returnSA = classSa < classCache;
                }
            } else {
                if (_entryFromSA != null)
                    returnSA = true;
            }

            if (returnSA) {
                res = _entryFromSA;
                _entryFromSA = null;
            } else {
                res = _entryFromCache;
                _entryFromCache = null;
            }
            if (res != null) {
                if (!returnSA) { //from memory return only transient + entries under active XTN with write or update op'
                    if (!res.isTransient()) {
                        if (!res.isMaybeUnderXtn()) {
                            res = null;
                            continue;
                        }
                        res = _cacheManager.getEntryByUidFromPureCache(res.getUID());
                        if (res == null || !res.isMaybeUnderXtn()) {
                            res = null;
                            continue;
                        }
                        ITransactionalEntryData ed = res.getTxnEntryData();
                        XtnEntry xtnEntry = ed.getWriteLockOwner();
                        if (xtnEntry == null || xtnEntry.getStatus() != XtnStatus.BEGUN ||
                                (ed.getWriteLockOperation() != SpaceOperations.WRITE && ed.getWriteLockOperation() != SpaceOperations.UPDATE)) {
                            res = null;
                            continue;
                        }
                    }
                }
                //check if the entry returned from DB resides in memory in a new copy, if yes return it
                if (returnSA) {
                    IEntryHolder updated_res = _cacheManager.getEntryByUidFromPureCache(res.getUID());
                    if (updated_res != null)
                        res = updated_res;
                }

                if (!_entriesReturned.add(res.getUID())) {
                    res = null;
                    continue;
                }
            }
            _context.setRawmatchResult(null, MatchResult.NONE, null, null);
            return res;
        }
    }


    @Override
    public void close() throws SAException {
        if (_entries != null)
            _entries.releaseScan();

        if (_saIter != null)
            _saIter.close();

        super.close();
    }

    /**
     * checks if there is a valid next element and sets the m_CurrentClass, m_Entries, m_Pos and
     * m_CurrentEntry fields accordingly.
     */
    private void checkIfNext() {
        // first, we check if there are any classes in list
        if (_currentClass >= _types.length) {
            _currentEntryCacheInfo = null;
            _currentEntryHolder = null;
            return;
        }


        if (!moreEntriesForCurrentClass()) {
            if (!_firstFromCache) {
                _currentClass++;
                _actualClass++;
            }//if (!m_FirstFromCache)
            else {
                _firstFromCache = false;
            }
        }


        // second, we assign the current m_Entries SL if it is null
        if (!moreEntriesForCurrentClass()) {
            for (; _currentClass < _types.length; _currentClass++, _actualClass++) {
                if (_actualClass >= _types.length)
                    _actualClass = 0;

                TypeData pType = _cacheManager.getTypeData(_types[_actualClass]);

                if (pType != null) {
                    getEntries(_types[_actualClass]);
                    if (!moreEntriesForCurrentClass())
                        continue;
                    break;
                }
            }

            if (!moreEntriesForCurrentClass()) {
                _currentEntryCacheInfo = null;
                _currentEntryHolder = null;
                return;
            }
        }

        // third, we check if there are more relevant objects for the current class.
        // if there are not, we skip to the next class and the next... until
        // we find a relevant object or we visited them all.
        if (isRelevantEntry())
            return;

        // if we got here we didn't find relevant objects in the current m_Entries
        // SL, so we search next classes

        for (_currentClass++, _actualClass++; _currentClass < _types.length; _currentClass++, _actualClass++) {
            if (_actualClass >= _types.length)
                _actualClass = 0;

            //[@since 1.47] old logic using classTable to extract PType
            //String currentClass = (String) m_Classes[m_ActualClass];
            //pType = (PType) m_CacheManager.getM_Root().m_ClassTable.get(currentClass);
            TypeData pType = _cacheManager.getTypeData(_types[_actualClass]);

            if (pType == null)
                continue;
            if (_templateHolder.isChangeById() && !_types[_actualClass].getTypeName().equals(_templateHolder.getServerTypeDesc()))
                continue;
            ;  //in-place-update by id no inheritance

            getEntries(_types[_actualClass]);
            if (!moreEntriesForCurrentClass())
                continue;

            if (isRelevantEntry())
                return;

        } /* for (m_CurrentClass++, ...) */
        // if we got here there are no relevant objects to return
        _currentEntryCacheInfo = null;
        _currentEntryHolder = null;
    }

    private boolean match(IEntryHolder eh) {
        return _cacheManager.getEngine().getTemplateScanner().match(_context, eh, _templateHolder);
    }

    private void getEntries(IServerTypeDesc entryTypeDesc) {
        Object res = _cacheManager.getMatchingMemoryEntriesForScanning(_context, entryTypeDesc, _templateHolder, _templateServerTypeDesc);
        if (_context.isBlobStoreTryNonPersistentOp()) //can we skip access to ssd/blobStore ?
        {
            if (_context.isBlobStoreUsePureIndexesAccess() && _context.isUsingIntersectedListForScanning()) {
                setBringCacheInfoOnly(true);
                _tryToUsePureIndexesBlobStore = true;
            } else {//is it all nulls template ?
                boolean allNullsBlobStore = !_templateHolder.isSqlQuery() && _templateHolder.getXidOriginated() == null && _templateHolder.getUidToOperateBy() == null;
                if (allNullsBlobStore && _templateHolder.getEntryData().getFixedPropertiesValues() != null && _templateHolder.getEntryData().getFixedPropertiesValues().length > 0) {
                    for (Object property : _templateHolder.getEntryData().getFixedPropertiesValues()) {
                        if (property != null) {
                            allNullsBlobStore = false;
                            break;
                        }
                    }
                }
                setBringCacheInfoOnly(allNullsBlobStore);
            }
        }

        if (res != null && res instanceof IEntryCacheInfo)
            _entries = new ScanSingleListIterator((IStoredList) res, _templateHolder.isFifoTemplate(), _templateHolder.isServerIterator());
        else
            _entries = (IScanListIterator) res;
    }


    //more entries to bring for current class?
    private boolean moreEntriesForCurrentClass() {
        return (_entries != null);
    }


    private IEntryHolder saIterNext()
            throws SAException {
        if (_saIter == null)
            return null;

        while (true) {
            IEntryHolder entryHolder = _saIter.next();
            if (entryHolder == null) {
                _currentEntryCacheInfo = null;
                _currentEntryHolder = null;
                return null;
            }

            if (_entriesReturned != null && _entriesReturned.contains(entryHolder.getUID()))
                continue;

            //Verify that entry read
            //from the DB donot belong to another partition
            if (_cacheManager.getEngine().isPartitionedSpace()) {
                if (_typeDesc == null || !_typeDesc.getTypeName().equals(entryHolder.getClassName()))
                    _typeDesc = _cacheManager.getTypeManager().getTypeDesc(entryHolder.getClassName());

                if (!_cacheManager.getEngine().isEntryFromPartition(entryHolder))
                    continue;
            }

            IEntryCacheInfo pEntry = null;
            if (!_notRefreshCache) {
                if (_templateHolder.isChangeById() && !entryHolder.getServerTypeDesc().getTypeName().equals(_templateHolder.getServerTypeDesc().getTypeName()))
                    continue;
                ;  //in-place-update by id no inheritance
                //memory monitoring before inserting to cache- its write type op.
                _cacheManager.getEngine().getMemoryManager().monitorMemoryUsage(true);
                if ((pEntry = _cacheManager.insertEntryToCache(_context, entryHolder, false /*newEntry*/, null /*pType*/, false/*pin*/, CacheManager.InitialLoadOrigin.NON /*fromInitialLoad*/)) == null)
                    //insert to cache failed- entry was deleted meanwhile
                    continue;
                IEntryHolder eh = pEntry.getEntryHolder(_cacheManager);
                if (eh != entryHolder && !match(eh))
                    continue;

                entryHolder = eh; //in case a newer version in cache pinned after update

            }
            _context.setRawmatchResult(null, MatchResult.NONE, null, null);
            _currentEntryHolder = entryHolder;
            return entryHolder;
        }
    }

    public void setNotRefreshCache() {
        _notRefreshCache = true;
    }

    public void setBringCacheInfoOnly(boolean val) {
        _returnEntryCacheInfoOnly = val;
    }

    public boolean isBringCacheInfoOnly() {
        return _returnEntryCacheInfoOnly;
    }

    private boolean isRelevantEntry() {
        // third, we check if there are more relevant objects for the current class.
        // if there are not, we skip to the next class and the next... until
        // we find a relevant object or we visited them all.
        if (_entries != null) {
            try {
                while (_entries.hasNext()) {
                    IEntryCacheInfo pEntry = _entries.next();
                    if (pEntry == null)
                        continue;
                    if (_tryToUsePureIndexesBlobStore && !_context.isBlobStoreUsePureIndexesAccess() && isBringCacheInfoOnly())
                        setBringCacheInfoOnly(false);  //no intersection possible due to long vectors
                    if (_returnEntryCacheInfoOnly && pEntry.isBlobStoreEntry() && _SCNFilter == 0 && _leaseFilter == 0) {
                        _currentEntryHolder = null;
                        _currentEntryCacheInfo = null;
                        if (!invalidEntryCacheInfo(pEntry)) {
                            _currentEntryCacheInfo = pEntry;
                            _currentEntryHolder = ((IBlobStoreRefCacheInfo) pEntry).getEntryHolderIfInMemory();
                            if (_currentEntryHolder != null && !match(_currentEntryHolder)) {
                                _currentEntryHolder = null;
                                _currentEntryCacheInfo = null;
                                continue;
                            }
                            return true;
                        } else
                            continue;
                    }
                    if (pEntry.isBlobStoreEntry()) {
                        if (pEntry.isDeleted() || !pEntry.preMatch(_context, _templateHolder))
                            continue;
                    }

                    IEntryHolder eh = pEntry.getEntryHolder(_cacheManager);
                    if (!invalidEntry(pEntry, eh) && match(eh)) {
                        _currentEntryCacheInfo = pEntry;
                        _currentEntryHolder = eh;
                        return true;
                    }
                }
            } catch (SAException ex) {
            }//can never happen in a memory space
        }
        _entries = null;
        return false;

    }


    /**
     * @return <code>false</code> if PEntry not valid
     */
    private boolean invalidEntry(IEntryCacheInfo pEntry, IEntryHolder eh) {
        if (pEntry == null)
            return true;
        if (_memoryOnly && !_cacheManager.isResidentCacheEntry(pEntry))
            return true;
        return
                (_cacheManager.isEvictableCachePolicy() && pEntry.isRemoving()) ||
                        (_SCNFilter != 0 && eh.getSCN() < _SCNFilter) ||
                        (_leaseFilter != 0 && eh.isExpired(_leaseFilter) && !_cacheManager.getLeaseManager().isSlaveLeaseManagerForEntries() && !_cacheManager.getEngine().isExpiredEntryStayInSpace(eh)) ||
                        (_transientOnly && !eh.isTransient());
    }

    private boolean invalidEntryCacheInfo(IEntryCacheInfo pEntry) {
        return pEntry.isDeleted();
    }
}