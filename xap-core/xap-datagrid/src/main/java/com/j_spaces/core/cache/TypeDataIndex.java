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

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.query.IQueryIndexScanner;
import com.gigaspaces.internal.server.space.MatchTarget;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ShadowEntryHolder;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.collections.economy.EconomyConcurrentHashMap;
import com.gigaspaces.internal.utils.collections.economy.HashEntryHandlerSpaceEntry;
import com.gigaspaces.internal.utils.collections.economy.IEconomyConcurrentMap;
import com.gigaspaces.metadata.index.ISpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.Constants;
import com.j_spaces.core.cache.fifoGroup.FifoGroupsMainIndexExtention;
import com.j_spaces.core.cache.fifoGroup.IFifoGroupsIndexExtention;
import com.j_spaces.core.client.DuplicateIndexValueException;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.StoredListFactory;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.list.IScanListIterator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

@com.gigaspaces.api.InternalApi
public class TypeDataIndex<K> {
    private final Logger _logger;

    //a dummy ref for failed index used under xtn
    public static IEntryCacheInfo _DummyOI = EntryCacheInfoFactory.createEntryCacheInfo(null);

    private static final boolean _indexesBackrefsForBlobStoreData = true;

    //the percentage of unique values- above it we try "put" of raw value first
    private static final int UNIQUE_VALUE_TRY_THRESHOLD = 40;


    private final int _position;
    private final SpaceIndexType _indexType;

    private final ISpaceIndex _indexDefinition;

    //in case index is not unique
    private final ConcurrentMap<Object, IStoredList<IEntryCacheInfo>> _nonUniqueEntriesStore;
    //in case index is unique
    private final ConcurrentMap<Object, IEntryCacheInfo> _uniqueEntriesStore;


    //basic indexes for templates
    public final ConcurrentHashMap<Object, IStoredList<TemplateCacheInfo>[]> _RTTemplates;
    public final ConcurrentHashMap<Object, IStoredList<TemplateCacheInfo>[]> _NTemplates;


    private final IStoredList<IEntryCacheInfo> _nullEntries;

    public final IStoredList<TemplateCacheInfo> _RTNullTemplates;
    public final IStoredList<TemplateCacheInfo> _NNullTemplates;

    //concurrent implementation
    private final IExtendedEntriesIndex<K, IEntryCacheInfo> _concurrentExtendedIndex;
    private final IExtendedEntriesIndex<K, IEntryCacheInfo> _concurrentExtendedFifoGroupsIndex;

    protected boolean m_AreAnyLogicallDelitions;


    //extendex indexes for templates for GT/GE conditions
    private final IExtendedIndex<K, TemplateCacheInfo> m_Notify_GT_Index;
    private final TemplatesExtendedIndexHandler<K> m_RT_GT_Index;
    //extendex indexes for templates for LT/LE conditions
    private final IExtendedIndex<K, TemplateCacheInfo> m_Notify_LT_Index;
    private final TemplatesExtendedIndexHandler<K> m_RT_LT_Index;

    //extended index for NE condition
    private final IExtendedIndex<K, TemplateCacheInfo> m_Notify_NE_Index;
    private final TemplatesExtendedIndexHandler<K> m_RT_NE_Index;


    //non volatile-non protected
    private int _estimatedNumNonNullValues;
    private int _estimatedUniqueNonNullValues;

    //currently concurrent SL is used for null-value entries only
    private final boolean _useConcurrentSl;

    private final boolean _useEconomyHashMap;

    private Class<?> _valueType;

    //creation version of this index
    private final int _indexCreationNumber;

    private final boolean _considerValueClone;
    private final boolean _cloneableIndexValue;
    //is index value type fixed & known ?
    private final boolean _valueTypeKnown;

    private final ISpaceIndex.FifoGroupsIndexTypes _fifoGroupsIndexType;
    //if this index is a segment in compound f-g index- point to it
    private TypeDataIndex _compoundFifoGroupsIndex;

    protected IFifoGroupsIndexExtention<K> _fifoGroupsIndexExtention;

    private final ThreadLocal<IValueCloner> _valueCloner = new ThreadLocal<IValueCloner>();

    private final CacheManager _cacheManager;

    private final boolean _unique;

    //thin extended index ==> only skip-list no hash map
    private final boolean _thinExtendedIndex;

    private static final Set<String> _immutableTypes = initImmutableTypes();

    public TypeDataIndex(CacheManager cacheManager, ISpaceIndex index, int pos, boolean useEconomyHashmap, int indexCreationNumber) {
        this(
                cacheManager, index, pos, useEconomyHashmap, indexCreationNumber, null, ISpaceIndex.FifoGroupsIndexTypes.NONE);
    }

    public TypeDataIndex(CacheManager cacheManager, ISpaceIndex index, int pos, boolean useEconomyHashmap, int indexCreationNumber, ISpaceIndex.FifoGroupsIndexTypes fifoGroupsIndexType) {
        this(cacheManager, index, pos, useEconomyHashmap, indexCreationNumber, null, fifoGroupsIndexType);
    }

    public TypeDataIndex(CacheManager cacheManager, ISpaceIndex index, int pos, boolean useEconomyHashmap, int indexCreationNumber, Class<?> valueClass, ISpaceIndex.FifoGroupsIndexTypes fifoGroupsIndexType) {
        this._logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE + "." + cacheManager.getEngine().getSpaceImpl().getNodeName());
        _cacheManager = cacheManager;
        _useEconomyHashMap = useEconomyHashmap;
        _indexCreationNumber = indexCreationNumber;

        this._position = pos;
        this._indexType = index.getIndexType();
        _thinExtendedIndex = _indexType == SpaceIndexType.ORDERED ;
        _indexDefinition = index;
        _unique = index.isUnique();
        int numOfCHMSegents = Integer.getInteger(SystemProperties.CACHE_MANAGER_HASHMAP_SEGMENTS, SystemProperties.CACHE_MANAGER_HASHMAP_SEGMENTS_DEFAULT);

        if (!_thinExtendedIndex) {
            if (_useEconomyHashMap) {
                this._uniqueEntriesStore = index.isUnique() ? new EconomyConcurrentHashMap<Object, IEntryCacheInfo>(16, 0.75f, numOfCHMSegents, new HashEntryHandlerSpaceEntry(pos)) : null;
                this._nonUniqueEntriesStore = new EconomyConcurrentHashMap<Object, IStoredList<IEntryCacheInfo>>(16, 0.75f, numOfCHMSegents, new HashEntryHandlerSpaceEntry<Object>(pos));
            } else {
                this._uniqueEntriesStore = index.isUnique() ? new ConcurrentHashMap<Object, IEntryCacheInfo>(16, 0.75f, numOfCHMSegents) : null;
                this._nonUniqueEntriesStore = new ConcurrentHashMap<Object, IStoredList<IEntryCacheInfo>>(16, 0.75f, numOfCHMSegents);
            }
        } else {
            this._uniqueEntriesStore = null;
            this._nonUniqueEntriesStore = null;
        }

        int numOfCoresToUseSL = Integer.getInteger(SystemProperties.ENGINE_CORES_TOUSE_CONCURRENT_SL, SystemProperties.ENGINE_CORES_TOUSE_CONCURRENT_SL_DEFAULT);
        _useConcurrentSl = Runtime.getRuntime().availableProcessors() >= numOfCoresToUseSL;

        _RTTemplates = new ConcurrentHashMap<Object, IStoredList<TemplateCacheInfo>[]>();
        _NTemplates = new ConcurrentHashMap<Object, IStoredList<TemplateCacheInfo>[]>();

        if (_useConcurrentSl) {
            _nullEntries = StoredListFactory.createConcurrentSegmentedList(true/* supportFifoPerSegment*/,1 /* inputNumOfSegments*/,true /* padded*/);
            _RTNullTemplates = StoredListFactory.createConcurrentSegmentedList(true/* supportFifoPerSegment*/,1 /* inputNumOfSegments*/,true /* padded*/);
            _NNullTemplates = StoredListFactory.createConcurrentSegmentedList(true/* supportFifoPerSegment*/,1 /* inputNumOfSegments*/,true /* padded*/);
        } else {
            _nullEntries = StoredListFactory.createRandomScanList(true);
            _RTNullTemplates = StoredListFactory.createList(true);
            _NNullTemplates = StoredListFactory.createList(true);
        }

        if (_indexType.isOrdered()) {
            _concurrentExtendedIndex = new ExtendedIndexHandler<K>(this);

            m_Notify_GT_Index = new TemplatesExtendedIndexHandler<K>(this);
            m_RT_GT_Index = new TemplatesExtendedIndexHandler<K>(this);
            m_Notify_LT_Index = new TemplatesExtendedIndexHandler<K>(this);
            m_RT_LT_Index = new TemplatesExtendedIndexHandler<K>(this);
            m_Notify_NE_Index = new TemplatesExtendedIndexHandler<K>(this);
            m_RT_NE_Index = new TemplatesExtendedIndexHandler<K>(this);

        } else {
            _concurrentExtendedIndex = null;
            m_Notify_GT_Index = null;
            m_RT_GT_Index = null;
            m_Notify_LT_Index = null;
            m_RT_LT_Index = null;
            m_Notify_NE_Index = null;
            m_RT_NE_Index = null;
        }
        _fifoGroupsIndexType = fifoGroupsIndexType;
        if (isFifoGroupsMainIndex())
            _fifoGroupsIndexExtention = new FifoGroupsMainIndexExtention<K>(cacheManager, this);


        if (_concurrentExtendedIndex != null && (isFifoGroupsMainIndex() || fifoGroupsIndexType == ISpaceIndex.FifoGroupsIndexTypes.COMPOUND))
            _concurrentExtendedFifoGroupsIndex = new FifoGroupsExtendedIndexHandler(this, _concurrentExtendedIndex, fifoGroupsIndexType);
        else
            _concurrentExtendedFifoGroupsIndex = null;

        String val = System.getProperty(SystemProperties.CACHE_MANAGER_EMBEDDED_INDEX_PROTECTION);
        boolean embeddedIndexProtection = new Boolean(val != null ? val : SystemProperties.CACHE_MANAGER_EMBEDDED_INDEX_PROTECTION_DEFAULT);
        if (!embeddedIndexProtection) {
            _considerValueClone = false;
            _cloneableIndexValue = false;
            _valueTypeKnown = false;
        } else {
            if (valueClass == null || valueClass.getName().equals("java.lang.Object")) {//
                _valueTypeKnown = false;
                _considerValueClone = true;
                _cloneableIndexValue = false;
            } else {//check the type
                _valueTypeKnown = true;
                //check if immutable
                if (TypeDataIndex.isImmutableIndexValue(valueClass)) {
                    _considerValueClone = false;
                    _cloneableIndexValue = false;
                } else {
                    _considerValueClone = true;
                    _cloneableIndexValue = TypeDataIndex.isCloneableIndexValue(valueClass);
                }
            }
        }
    }

    public boolean isExtendedIndex() {
        return _concurrentExtendedIndex != null;
    }

    public IExtendedEntriesIndex<K, IEntryCacheInfo> getExtendedIndex() {
        return _concurrentExtendedIndex;
    }

    public IExtendedEntriesIndex<K, IEntryCacheInfo> getExtendedFGIndex() {
        return _concurrentExtendedFifoGroupsIndex;
    }

    boolean considerValueClone() {
        return _considerValueClone;
    }

    public CacheManager getCacheManager() {
        return _cacheManager;
    }

    //how many backrefs does an index  occupy for a value
    /* (non-Javadoc)
     * @see com.j_spaces.core.cache.TypeDataIndex#numOfEntryIndexBackRefs(java.lang.Object)
     */
    public int numOfEntryIndexBackRefs(Object fieldValue) {
        return (!isExtendedIndex() || fieldValue == null) ? 1 : 2;
    }


    /* (non-Javadoc)
    * @see com.j_spaces.core.cache.TypeDataIndex#isUniqueIndex()
    */
    public boolean isUniqueIndex() {
        return _unique;
    }


    public ISpaceIndex getIndexDefinition() {
        return _indexDefinition;

    }

    private boolean isThinExtendedIndex() {
        return _thinExtendedIndex;
    }

    public boolean isFifoGroupsMainIndex() {
        return _fifoGroupsIndexType == ISpaceIndex.FifoGroupsIndexTypes.MAIN;
    }

    public ISpaceIndex.FifoGroupsIndexTypes getFifoGroupsIndexType() {
        return _fifoGroupsIndexType;
    }

    public static boolean isIndexesBackRefsForBlobStoreData() {
        return _indexesBackrefsForBlobStoreData;
    }

    public boolean disableIndexUsageForOperation(TypeData typeData, int inputIndexCreationNumber) {
        return (inputIndexCreationNumber < getIndexCreationNumber() || typeData.disableIdIndexForEntries(this));
    }

    /* (non-Javadoc)
    * @see com.j_spaces.core.cache.TypeDataIndex#markIndexValue(boolean)
    */
    public void markIndexValue(boolean unique) {
        if (unique)
            _estimatedUniqueNonNullValues++;
        _estimatedNumNonNullValues++;

        if (_estimatedNumNonNullValues < 0) {
            _estimatedNumNonNullValues = 0;
            _estimatedUniqueNonNullValues = 0;
        }
    }

    /* (non-Javadoc)
    * @see com.j_spaces.core.cache.TypeDataIndex#assumeUniqueValue()
    */
    public boolean assumeUniqueValue() {
        int estimatedNumNonNullValues = _estimatedNumNonNullValues;
        int estimatedUniqueNonNullValues = _estimatedUniqueNonNullValues;
        return estimatedNumNonNullValues == 0 ? true
                : ((estimatedUniqueNonNullValues / estimatedNumNonNullValues) * 100 > UNIQUE_VALUE_TRY_THRESHOLD);
    }

    /* (non-Javadoc)
    * @see com.j_spaces.core.cache.TypeDataIndex#getIndexValue(com.gigaspaces.server.IServerEntry)
    */
    public Object getIndexValue(ServerEntry entry) {
        return entry.getFixedPropertyValue(_position);
    }


    public IStoredList<IEntryCacheInfo> getIndexEntries(Object indexValue) {
        if (!isThinExtendedIndex()) {
            if (isUniqueIndex())
                return getUniqueEntriesStore().get(indexValue);

            return getNonUniqueEntriesStore().get(indexValue);
        } else
            return getConcurrentExtendedIndex().getIndexEntries((K) indexValue);
    }


    /**
     * @return the entries
     */
    public ConcurrentMap<Object, IStoredList<IEntryCacheInfo>> getNonUniqueEntriesStore() {
        return isThinExtendedIndex() ? _concurrentExtendedIndex.getNonUniqueEntriesStore() : _nonUniqueEntriesStore;
    }


    /**
     * @return the uniqueIndex
     */
    public ConcurrentMap<Object, IEntryCacheInfo> getUniqueEntriesStore() {
        return isThinExtendedIndex() ? _concurrentExtendedIndex.getUniqueEntriesStore() : _uniqueEntriesStore;
    }


    /**
     * @return the nullEntries
     */
    public IStoredList<IEntryCacheInfo> getNullEntries() {
        return _nullEntries;
    }


    public IExtendedEntriesIndex<K, IEntryCacheInfo> getConcurrentExtendedIndex() {
        return _concurrentExtendedIndex;
    }

    public IExtendedIndex<K, IEntryCacheInfo> getExtendedFifoGroupsIndexForScanning() {
        return _concurrentExtendedFifoGroupsIndex;
    }

    public IExtendedIndexScanPositioner<K, IEntryCacheInfo> getExtendedIndexForScanning() {
        return _concurrentExtendedIndex;
    }


    /**
     * insert entry indexed field from cache.
     */
    public void insertEntryIndexedField(IEntryCacheInfo pEntry, K fieldValue, TypeData pType) {
        if (_fifoGroupsIndexExtention == null)
            insertEntryIndexedField_impl(pEntry, fieldValue, pType, pEntry.getBackRefs());
        else
            _fifoGroupsIndexExtention.insertEntryIndexedField(pEntry, fieldValue, pType);

    }

    public void insertEntryIndexedField(IEntryCacheInfo pEntry, K fieldValue, TypeData pType, ArrayList<IObjectInfo<IEntryCacheInfo>> insertBackRefs) {
        if (_fifoGroupsIndexExtention == null)
            insertEntryIndexedField_impl(pEntry, fieldValue, pType, insertBackRefs);
        else
            _fifoGroupsIndexExtention.insertEntryIndexedField(pEntry, fieldValue, pType, insertBackRefs);
    }


    public void insertEntryIndexedField_impl(IEntryCacheInfo pEntry, K fieldValue, TypeData pType, ArrayList<IObjectInfo<IEntryCacheInfo>> backRefs) {
        if (backRefs == null && pEntry.indexesBackRefsKept())
            throw new RuntimeException("null backrefs but backrefs have to be kept!!!!");
        IObjectInfo oi = null;
        // check if field's value is null - if so store it in null entries
        boolean uniqueValue = true;
        boolean alreadyCloned = false;

        if (fieldValue == null) {
            oi = _nullEntries.add(pEntry);
            if (backRefs != null)
                backRefs.add(oi);
        } else // there is a non-null value, store it in entries
        {
            updateValueType(fieldValue);

            if (isUniqueIndex()) {
                while (true) {
                    IEntryCacheInfo other = getUniqueEntriesStore().putIfAbsent(fieldValue, pEntry);
                    if (other == null) {
                        oi = pEntry;
                        break;
                    }
                    //unique index supported for ID field with autogenerate=false  and for defined index                    
                    if (other.isRemovingOrRemoved() || other.isDeleted()) {//removing entry - help out
                        getUniqueEntriesStore().remove(fieldValue, other); //help remove
                    } else {
                        DuplicateIndexValueException ex = new DuplicateIndexValueException(pEntry.getUID(), pEntry.getEntryHolder(_cacheManager).getClassName(), _indexDefinition.getName(), fieldValue, other.getUID());
                        if (_logger.isLoggable(Level.SEVERE))
                            _logger.log(Level.SEVERE, "Duplicate value encountered on unique index insertion ", ex);
                        //if case of failure we need to remove the partally inserted entry
                        //relevant for write + update + update under txn, done in the calling code
                        throw ex;
                    }
                }
            } else {
                IStoredList<IEntryCacheInfo> newSL = null;
                IObjectInfo myoi = null, otheroi = null;
                IStoredList<IEntryCacheInfo> currentSL = null;
                boolean first = true;
                while (true) {
                    if (first) {
                        first = false;
                        if (!assumeUniqueValue())
                            currentSL = getNonUniqueEntriesStore().get(fieldValue);
                    }
                    if (currentSL == null) {
                        if (_considerValueClone && !alreadyCloned) {
                            fieldValue = (K) cloneIndexValue(fieldValue, pEntry.getEntryHolder(_cacheManager));
                            alreadyCloned = true;
                        }
                        currentSL = getNonUniqueEntriesStore().putIfAbsent(fieldValue, pEntry);
                        if (currentSL == null) {
                            oi = pEntry;
                            if (_fifoGroupsIndexExtention != null)
                                _fifoGroupsIndexExtention.addToValuesList(fieldValue, pEntry);
                            break;
                        }
                    }
                    if (currentSL.isMultiObjectCollection()) {//a real SL
                        oi = currentSL.add(pEntry);
                        // may have been invalidated by PersistentGC
                        if (oi == null) {
                            //help remove entry for key only if currently mapped to given value
                            getNonUniqueEntriesStore().remove(fieldValue, currentSL);
                            currentSL = null;
                            continue;
                        } else {
                            uniqueValue = false;
                            break; // OK - done.
                        }
                    }
                    //a single object is stored, create a SL and add it
                    if (newSL == null) {
                        if (_useEconomyHashMap)
                            newSL = StoredListFactory.createConcurrentSegmentedList(false /*segmented*/, pType.isAllowFifoIndexScans(), fieldValue);
                        else
                            newSL = StoredListFactory.createConcurrentList(pType.isAllowFifoIndexScans());
                    }
                    otheroi = newSL.addUnlocked(currentSL.getObjectFromHead());
                    myoi = newSL.addUnlocked(pEntry);

                    if (!getNonUniqueEntriesStore().replace(fieldValue, currentSL, newSL)) {
                        newSL.removeUnlocked(otheroi);
                        newSL.removeUnlocked(myoi);
                        myoi = null;
                        currentSL = null;
                    } else {
                        uniqueValue = false;
                        oi = myoi;
                        if (_fifoGroupsIndexExtention != null) {
                            _fifoGroupsIndexExtention.addToValuesList(fieldValue, newSL);
                            _fifoGroupsIndexExtention.removeFromValuesList(fieldValue, currentSL);
                        }

                        break; // OK - done.
                    }
                }
            }

            if (!isUniqueIndex())
                markIndexValue(uniqueValue);
            if (backRefs != null)
                backRefs.add(isUniqueIndex() ? pEntry : oi);


            if (isExtendedIndex()) {

                try {
                    if (!isThinExtendedIndex())
                        oi = _concurrentExtendedIndex.insertEntryIndexedField(pEntry, fieldValue, pType, alreadyCloned);
                    if (backRefs != null)
                        backRefs.add(oi); //if thin we insert same backref not to break existing code
                } catch (RuntimeException ex) {
                    if (!pEntry.getEntryHolder(_cacheManager).hasShadow()) {
                        //its not an update under xtn- so clean failure here
                        ArrayList<IObjectInfo<IEntryCacheInfo>> failedBackRefs = pEntry.getBackRefs();
                        removeNotNullIndex(pEntry.getEntryHolder(_cacheManager), failedBackRefs,
                                fieldValue, failedBackRefs != null ? failedBackRefs.size() - 1 : 0, pEntry, failedBackRefs != null ? failedBackRefs.get(failedBackRefs.size() - 1) : null, true);

                    } else
                        backRefs.add(_DummyOI);

                    throw ex;
                }
            }
        } /* else - there is a non-null value */
    }

    K cloneIndexValue(K fieldValue, IEntryHolder entryHolder) {
        Class<?> clzz = !_valueTypeKnown ? fieldValue.getClass() : getValueType();
        if (!_valueTypeKnown && TypeDataIndex.isImmutableIndexValue(clzz))
            return fieldValue;
        if (_valueCloner.get() == null)
            _valueCloner.set(new DefaultValueCloner());

        K res = (K) _valueCloner.get().cloneValue(fieldValue, _cloneableIndexValue, clzz, entryHolder.getUID(), entryHolder.getClassName());

        //perfom an health check
        if (res != fieldValue && (fieldValue.hashCode() != res.hashCode() || !fieldValue.equals(res)))
            throw new RuntimeException("Entry Class: " + entryHolder.getClassName() +
                    " - Wrong hashCode() or equals() implementation of " +
                    fieldValue.getClass() + " class field");

        return res;

    }

    protected boolean isConsiderValueClone() {
        return _considerValueClone;
    }

    /**
     * @param eh
     * @param fieldValue
     * @param pEntry
     * @param oi
     */
    private void removeNonUniqueIndexedField(IEntryHolder eh, K fieldValue,
                                             IEntryCacheInfo pEntry, IObjectInfo oi) {
        while (true) {
            ConcurrentMap<Object, IStoredList<IEntryCacheInfo>> store = getNonUniqueEntriesStore();
            IStoredList<IEntryCacheInfo> entries = store.get(fieldValue);
            if (entries == null /*&& isObjectSerialization*/)
                throw new RuntimeException("Entry Class: " + eh.getClassName() +
                        " - Wrong hashCode() or equals() implementation of " +
                        fieldValue.getClass() + " class field, or field value changed while entry stored in space.");

            if (entries.isMultiObjectCollection()) {//a true SL
                IObjectInfo myoi = oi;
                if (myoi == pEntry) {
                    myoi = entries.getHead();
                    if (myoi.getSubject() != pEntry)
                        throw new RuntimeException("Entry Class: " + eh.getClassName() +
                                " - Single-entry to multiple wrong OI ,  " +
                                fieldValue.getClass() + " class field .");
                }
                if (oi != null) {
                    entries.remove(myoi);
                } else {
                    boolean res = entries.removeByObject(pEntry);
                    if (!res)
                        throw new RuntimeException("Entry Class: " + eh.getClassName() +
                                " - removeByObject on SL returned false ,  " +
                                fieldValue.getClass() + " class field.");
                }
                //GS-7384 Remove index list in this thread instead of leaving it to persistent gc
                if (entries.invalidate()) {
                    store.remove(fieldValue, entries);
                    if (_fifoGroupsIndexExtention != null)
                        _fifoGroupsIndexExtention.removeFromValuesList(fieldValue, entries);

                }
                break;
            }
            if (entries != pEntry) {
                throw new RuntimeException("Entry Class: " + eh.getClassName() +
                        " - Single-entry Wrong hashCode() or equals() implementation of " +
                        fieldValue.getClass() + " class field, or field value changed while entry stored in space..");
            }
            //single value- remove me
            if (store.remove(fieldValue, pEntry)) {
                if (_fifoGroupsIndexExtention != null)
                    _fifoGroupsIndexExtention.removeFromValuesList(fieldValue, pEntry);
                break;
            }
        }
    }

    public int removeEntryIndexedField_main(IEntryHolder eh, ArrayList<IObjectInfo<IEntryCacheInfo>> deletedBackRefs,
                                            K fieldValue, int refpos, boolean removeIndexedValue, IEntryCacheInfo pEntry) {
        int res =
                removeEntryIndexedField(eh, deletedBackRefs,
                        fieldValue, refpos, removeIndexedValue, pEntry);
        if (isExtendedIndex()) {
            getExtendedIndex().onRemove(pEntry);
            if (getExtendedFGIndex() != null)
                getExtendedFGIndex().onRemove(pEntry);
        }
        return res;
    }


    /**
     * remove entry indexed field from cache. returns the current pos in the ref array
     * removeIndexedValue- true to remove from data structures, false to just remove from backrefs
     */
    public int removeEntryIndexedField(IEntryHolder eh, ArrayList<IObjectInfo<IEntryCacheInfo>> deletedBackRefs,
                                       K fieldValue, int refpos, boolean removeIndexedValue, IEntryCacheInfo pEntry) {
        if (_fifoGroupsIndexExtention == null || !removeIndexedValue || fieldValue == null)
            return
                    removeEntryIndexedField_impl(eh, deletedBackRefs,
                            fieldValue, refpos, removeIndexedValue, pEntry);
        else
            return
                    _fifoGroupsIndexExtention.removeEntryIndexedField(eh, deletedBackRefs,
                            fieldValue, refpos, removeIndexedValue, pEntry);
    }

    public int removeEntryIndexedField_impl(IEntryHolder eh, ArrayList<IObjectInfo<IEntryCacheInfo>> deletedBackRefs,
                                            K fieldValue, int refpos, boolean removeIndexedValue, IEntryCacheInfo pEntry) {
        IObjectInfo oi = null;
        if (deletedBackRefs != null)
            oi = deletedBackRefs.set(refpos, null);

        if (oi == null && pEntry.indexesBackRefsKept())
            throw new RuntimeException("oi is null but backrefs have to be kept!!!!");

        refpos++;
        if (oi == _DummyOI)
            removeIndexedValue = false;

        if (!removeIndexedValue) {
            if (fieldValue != null && isExtendedIndex()) {
                if (deletedBackRefs != null)
                    oi = deletedBackRefs.set(refpos, null);
                refpos++;
            }
            return refpos;

        }

        // check if field's value is null -
        if (fieldValue == null) {
            if (oi != null) {
                _nullEntries.remove(oi);
            } else {
                boolean res = _nullEntries.removeByObject(pEntry);
                if (!res)
                    throw new RuntimeException("Entry Class: " + eh.getClassName() +
                            " - removeByObject on NULLS SL returned false");
            }
        } else // there is a non-null value
        {
            removeNotNullIndex(eh,
                    deletedBackRefs,
                    fieldValue,
                    refpos,
                    pEntry,
                    oi, false /*fromFailure*/);
            if (isExtendedIndex())
                refpos++;

        } /* else - there is a non-null value */

        return refpos;
    }


    /**
     * @param eh
     * @param deletedBackRefs
     * @param fieldValue
     * @param refpos
     * @param pEntry
     * @param oi
     * @return
     */
    private void removeNotNullIndex(IEntryHolder eh,
                                    ArrayList<IObjectInfo<IEntryCacheInfo>> deletedBackRefs,
                                    K fieldValue, final int refpos, IEntryCacheInfo pEntry,
                                    IObjectInfo oi, boolean fromFailure) {
        if (isUniqueIndex() /*&& oi == pEntry TBD open-up when unique index is a general feature*/) {
            removeUniqueIndexedField(fieldValue, pEntry);
        } else {
            removeNonUniqueIndexedField(eh,
                    fieldValue,
                    pEntry,
                    oi);
        }

        if (isExtendedIndex() && !fromFailure) {
            //extended indexing
            if (deletedBackRefs != null)
                oi = deletedBackRefs.set(refpos, null);
            if (oi != _DummyOI && !isThinExtendedIndex())
                _concurrentExtendedIndex.removeEntryIndexedField(eh, fieldValue, pEntry, oi);
        }
    }
    
    
    
    
    /*
     * index (one or more) where added to the type
     * reindex the entry accordingly
     * 
     * NOTE=========   dropping indexes is currently not supported
     * 
     */

    static void reindexEntry(CacheManager cacheManager, IEntryCacheInfo entryCacheInfo, TypeData typeData) {
        if (!typeData.hasIndexes())
            return;   //nothing to do

        if (entryCacheInfo.getLatestIndexCreationNumber() >= typeData.getLastIndexPendingCreationNumber() && entryCacheInfo.getLatestIndexCreationNumber() >= typeData.getLastIndexCreationNumber())
            return;   //nothing to do

        if (entryCacheInfo.getEntryHolder(cacheManager).hasShadow(true /*safeEntry*/)) {
            reindexEntryAndShadow(cacheManager, entryCacheInfo, typeData);
            return;
        }
        ArrayList<IObjectInfo<IEntryCacheInfo>> newBackRefs = entryCacheInfo.indexesBackRefsKept() ? new ArrayList<IObjectInfo<IEntryCacheInfo>>(typeData.numberOfBackRefs()) : null;
        ArrayList<IObjectInfo<IEntryCacheInfo>> curBackRefs = entryCacheInfo.getBackRefs();
        int originalPos = 0;
        int newIndexCreationNumber = entryCacheInfo.getLatestIndexCreationNumber();
        entryCacheInfo.setBackRefs(newBackRefs);
        if (entryCacheInfo.indexesBackRefsKept())
            newBackRefs.add(curBackRefs.get(originalPos++)); //whole entries list
        // add entry to indexes
        IEntryData entryData = entryCacheInfo.getEntryHolder(cacheManager).getEntryData();
        final TypeDataIndex[] indexes = typeData.getIndexes();
        for (TypeDataIndex index : indexes) {
            newIndexCreationNumber = Math.max(newIndexCreationNumber, index.getIndexCreationNumber());
            if (typeData.disableIdIndexForEntries(index))
                continue;
            if (index.getIndexCreationNumber() <= entryCacheInfo.getLatestIndexCreationNumber()) {
                if (entryCacheInfo.indexesBackRefsKept()) {
                    newBackRefs.add(curBackRefs.get(originalPos++));
                    if (index.isExtendedIndex() && index.getIndexValue(entryData) != null)
                        newBackRefs.add(curBackRefs.get(originalPos++));
                }
            } else {//new index
                index.insertEntryIndexedField(entryCacheInfo, index.getIndexValue(entryData), typeData);
            }
        } /* for (int pos...) */

        entryCacheInfo.setLatestIndexCreationNumber(newIndexCreationNumber);
    }

    private static void reindexEntryAndShadow(CacheManager cacheManager, IEntryCacheInfo entryCacheInfo, TypeData typeData) {
        ShadowEntryHolder shadowEh = entryCacheInfo.getEntryHolder(cacheManager).getShadow();

        ArrayList<IObjectInfo<IEntryCacheInfo>> newBackRefs = entryCacheInfo.indexesBackRefsKept() ? new ArrayList<IObjectInfo<IEntryCacheInfo>>(typeData.numberOfBackRefs()) : null;
        ArrayList<IObjectInfo<IEntryCacheInfo>> curBackRefs = entryCacheInfo.getBackRefs();
        ArrayList<IObjectInfo<IEntryCacheInfo>> newShadowBackRefs = entryCacheInfo.indexesBackRefsKept() ? new ArrayList<IObjectInfo<IEntryCacheInfo>>(typeData.numberOfBackRefs()) : null;
        ArrayList<IObjectInfo<IEntryCacheInfo>> curShadowBackRefs = shadowEh.getBackRefs();
        int originalPos = 0;
        int originalShadowPos = 0;
        int newIndexCreationNumber = entryCacheInfo.getLatestIndexCreationNumber();
        entryCacheInfo.setBackRefs(newBackRefs);
        shadowEh.setShadowBackRefs(newShadowBackRefs);
        if (entryCacheInfo.indexesBackRefsKept()) {
            newBackRefs.add(curBackRefs.get(originalPos++)); //whole entries list
            newShadowBackRefs.add(curShadowBackRefs.get(originalShadowPos++)); //whole entries list
        }
        // add entry & shadow to indexes
        IEntryData entryData = entryCacheInfo.getEntryHolder(cacheManager).getEntryData();
        IEntryData shadowEntryData = shadowEh.getEntryData();
        final TypeDataIndex[] indexes = typeData.getIndexes();
        for (TypeDataIndex index : indexes) {
            newIndexCreationNumber = Math.max(newIndexCreationNumber, index.getIndexCreationNumber());
            if (typeData.disableIdIndexForEntries(index))
                continue;
            if (index.getIndexCreationNumber() <= entryCacheInfo.getLatestIndexCreationNumber()) {
                if (entryCacheInfo.indexesBackRefsKept()) {
                    newBackRefs.add(curBackRefs.get(originalPos++));
                    newShadowBackRefs.add(curShadowBackRefs.get(originalShadowPos++));
                    if (index.isExtendedIndex() && index.getIndexValue(entryData) != null)
                        newBackRefs.add(curBackRefs.get(originalPos++));
                    if (index.isExtendedIndex() && index.getIndexValue(shadowEntryData) != null)
                        newShadowBackRefs.add(curShadowBackRefs.get(originalShadowPos++));
                }
            } else {//new index
                int beforeOpShadowPos = entryCacheInfo.indexesBackRefsKept() ? newShadowBackRefs.size() : -1;
                //insert the shadow index value
                index.insertEntryIndexedField(entryCacheInfo, index.getIndexValue(shadowEntryData), typeData, newShadowBackRefs);
                if (!index.isMultiValuePerEntryIndex()) {

                    if (TypeData.objectsEquality(index.getIndexValue(shadowEntryData), index.getIndexValue(entryData))) {
                        if (entryCacheInfo.indexesBackRefsKept())
                            for (int pos = beforeOpShadowPos; pos < newShadowBackRefs.size(); pos++)
                                newBackRefs.add(newShadowBackRefs.get(pos));
                    } else {
                        index.insertEntryIndexedField(entryCacheInfo, index.getIndexValue(entryData), typeData, newBackRefs);
                    }
                } else {//multi value field- perform update under xtn

                    index.updateIndexValueUndexXtn(typeData, entryCacheInfo.getEntryHolder(cacheManager), entryCacheInfo, index.getIndexValue(shadowEntryData), index.getIndexValue(entryData), newShadowBackRefs, beforeOpShadowPos, false);
                }
            }
        } /* for (int pos...) */
        entryCacheInfo.setLatestIndexCreationNumber(newIndexCreationNumber);
        if (entryCacheInfo.indexesBackRefsKept())
            shadowEh.setBackrefIndexPos(typeData.createIndexBackreferenceArray(entryCacheInfo, shadowEntryData));
    }


    /**
     * @param fieldValue
     * @param pEntry
     */
    public void removeUniqueIndexedField(K fieldValue,
                                         IEntryCacheInfo pEntry) {
        if (!getUniqueEntriesStore().remove(fieldValue, pEntry)) {
            K other = _considerValueClone ? cloneIndexValue(fieldValue, pEntry.getEntryHolder(_cacheManager)) : fieldValue;
            if (other != fieldValue && (fieldValue.hashCode() != other.hashCode() || !fieldValue.equals(other)))
                throw new RuntimeException("Entry Class: " + pEntry.getClassName() +
                        " - Wrong hashCode() or equals() implementation of " +
                        fieldValue.getClass() + " class field, or field value changed while entry stored in space.");
        }
    }

    public void prepareForReplacingEntryIndexedField(Object fieldValue) {
        if (!_useEconomyHashMap || fieldValue == null)
            return;

        if (isUniqueIndex() /*&& oi == pEntry TBD open-up when unique index is a general feature*/)
            ((IEconomyConcurrentMap<Object, IEntryCacheInfo>) getUniqueEntriesStore()).setKeyUnstable(fieldValue);
        else
            ((IEconomyConcurrentMap<Object, IStoredList<IEntryCacheInfo>>) getNonUniqueEntriesStore()).setKeyUnstable(fieldValue);
    }

    public boolean isIndexed() {
        return _indexType.isIndexed();
    }

    public int getPos() {
        return _position;
    }

    public int getMaxFixedPropertiesSegmentPos() {//valid only in compound indices
        throw new UnsupportedOperationException();
    }

    public SpaceIndexType getIndexType() {
        return _indexType;
    }

    public void addNullNotifyTemplate(TemplateCacheInfo template) {
        IObjectInfo<TemplateCacheInfo> oi = _NNullTemplates.add(template);
        template.m_BackRefs.add(oi);

    }

    public void addNullReadTakeTemplate(TemplateCacheInfo template) {
        IObjectInfo<TemplateCacheInfo> oi = _RTNullTemplates.add(template);
        template.m_BackRefs.add(oi);
    }


    public boolean usedEconomyHashMap() {
        return _useEconomyHashMap;
    }

    public Class<?> getValueType() {
        return _valueType;
    }

    void updateValueType(Object fieldValue) {
        Class<?> type = fieldValue.getClass();
        // If first time, just set the value:
        if (_valueType == null) {
            synchronized (this) {
                if (_valueType == null)
                    _valueType = type;
            }
        }
        // Otherwise, if not the same type or a parent type, look for a common super type:
        else if (_valueType != type && !_valueType.isAssignableFrom(type)) {
            synchronized (this) {
                _valueType = getCommonSuperType(type, _valueType);
            }
        }
    }

    public int getIndexCreationNumber() {
        return _indexCreationNumber;
    }

    public boolean isCustomIndex() {
        return false;
    }

    public boolean isMultiValuePerEntryIndex() {
        return false;
    }

    public int moveValueBackrefsOnUpdate(IEntryCacheInfo pEntry, Object value, ArrayList<IObjectInfo<IEntryCacheInfo>> originalBackRefs, ArrayList<IObjectInfo<IEntryCacheInfo>> updatedBackRefs, int originalRefPos, boolean setNullToOriginalRef) {
        if (pEntry.indexesBackRefsKept()) {
            updatedBackRefs.add(originalBackRefs.get(originalRefPos++));
            if (setNullToOriginalRef)
                originalBackRefs.set(originalRefPos - 1, null);
            if (value != null && isExtendedIndex()) {
                updatedBackRefs.add(originalBackRefs.get(originalRefPos++));
                if (setNullToOriginalRef)
                    originalBackRefs.set(originalRefPos - 1, null);
            }
        }
        return originalRefPos;
    }

    public int updateIndexValue(TypeData pType, IEntryHolder eh, IEntryCacheInfo pEntry, K original, K updated, ArrayList<IObjectInfo<IEntryCacheInfo>> originalBackRefs, int refpos, UpdateIndexModes updateMode) {
        if (originalBackRefs == null && pEntry.indexesBackRefsKept())
            throw new RuntimeException("null backrefs but backrefs have to be kept!!!!");

        if ((updateMode == UpdateIndexModes.INSERT_NONEQUALS || updateMode == UpdateIndexModes.REPLACE_NONEQUALS || updateMode == UpdateIndexModes.REMOVE_NONEQUALS) && TypeData.objectsEquality(updated, original))
            return moveValueBackrefsOnUpdate(pEntry, updated, originalBackRefs, pEntry.getBackRefs(), refpos, true /*setNullToOriginalRef*/);

        validateIdFieldNotBeingUpdated(pType, eh, original, updated);

        if (isExtendedIndex()) {
            getExtendedIndex().onUpdate(pEntry);
            if (getExtendedFGIndex() != null)
                getExtendedFGIndex().onUpdate(pEntry);
        }

        //insert the new value.
        if (updateMode == UpdateIndexModes.INSERT_NONEQUALS || updateMode == UpdateIndexModes.REPLACE_NONEQUALS)
            insertEntryIndexedField(pEntry, updated, pType);

        //remove the indexed  field
        if (updateMode == UpdateIndexModes.REMOVE_NONEQUALS || updateMode == UpdateIndexModes.REPLACE_NONEQUALS) {
            int res = removeEntryIndexedField(pEntry.getEntryHolder(_cacheManager), originalBackRefs, original, refpos, true, pEntry);
            if (isExtendedIndex()) {
                getExtendedIndex().onUpdateEnd(pEntry);
                if (getExtendedFGIndex() != null)
                    getExtendedFGIndex().onUpdateEnd(pEntry);
            }
            return res;
        } else
            return refpos + numOfEntryIndexBackRefs(original);
    }

    public int updateIndexValueUndexXtn(TypeData pType, IEntryHolder eh, IEntryCacheInfo pEntry, K previous, K updated, ArrayList<IObjectInfo<IEntryCacheInfo>> previousBackRefs, int refpos, boolean entry_double_update) {
        if (previousBackRefs == null && pEntry.indexesBackRefsKept())
            throw new RuntimeException("null backrefs but backrefs have to be kept!!!!");

        ShadowEntryHolder shadowEh = pEntry.getEntryHolder(_cacheManager).getShadow();
        //there is an update but the entry is previously updated under this xtn
        IEntryData shadowEntryData = shadowEh.getEntryData();
        final int fnum = getPos();
        if (TypeData.objectsEquality(updated, previous)) {
            if (pEntry.indexesBackRefsKept())
                pEntry.getBackRefs().add(previousBackRefs.get(refpos));
            refpos++;
            if (isExtendedIndex() && updated != null) {
                if (pEntry.indexesBackRefsKept())
                    pEntry.getBackRefs().add(previousBackRefs.get(refpos));
                refpos++;
            }
            return refpos; //nulls, get next field
        }
        validateIdFieldNotBeingUpdated(pType, eh, previous, updated);

        //unequality between original and new value
        boolean removeIndexValue = false; //keep the old value in index
        Object shadowFieldValue = getIndexValue(shadowEntryData);
        if (entry_double_update)
            //if the replaced value equals to the original value of all updates-dont remove from index
            removeIndexValue = !TypeData.objectsEquality(previous, shadowFieldValue);

        //insert the new value . if the new values are equal to the original
        //values in case of double update- no need to insert
        boolean insertIndexValue = true;
        if (entry_double_update)
            //if the replaced value equals to the original value of all updates-dont insert index value
            insertIndexValue = !TypeData.objectsEquality(updated, shadowFieldValue);

        if (insertIndexValue)
            insertEntryIndexedField(pEntry, updated, pType);
        else {//copy the refs from the shadow to the new backrefs array
            if (pEntry.indexesBackRefsKept()) {
                pEntry.getBackRefs().add(shadowEh.getBackRefs().get(shadowEh.getBackrefIndexPos()[fnum]));
                if (isExtendedIndex() && updated != null)
                    pEntry.getBackRefs().add(shadowEh.getBackRefs().get(shadowEh.getBackrefIndexPos()[fnum] + 1));
            }
        }
        //remove the old indexed  field
        return removeEntryIndexedField(pEntry.getEntryHolder(_cacheManager), previousBackRefs, previous, refpos, removeIndexValue, pEntry);
    }

    private void validateIdFieldNotBeingUpdated(TypeData pType, IEntryHolder eh, K original, K updated) {
        //In .NET auto generated field is indexed and the original value after write will be null, we need to check
        //why we have this differentiation between .NET and Java.
        if (pType.getIdField() == this && !(eh.getServerTypeDesc().getTypeDesc().isAutoGenerateId() && !StringUtils.hasLength((String) original)))
            throw new IllegalStateException("ID field cannot be updated or invalid hashCode()/equals() value for ID field (autoGenerateId=" + eh.getServerTypeDesc().getTypeDesc().isAutoGenerateId() + "), original ID='" + original + "', update attempt ID='" + updated + "'");
    }

    int consolidateIndexValueOnXtnEnd(IEntryHolder eh, IEntryCacheInfo pEntry, K keptValue, K deletedVlaue, ArrayList<IObjectInfo<IEntryCacheInfo>> deletedBackRefs, int refpos, boolean onError) {
        if (TypeData.objectsEquality(deletedVlaue, keptValue)) {
            return refpos + numOfEntryIndexBackRefs(deletedVlaue);
            //we keep the ref, just skip on this index
        }
        //remove the index at the pos
        return removeEntryIndexedField(eh, deletedBackRefs, deletedVlaue, refpos, true, pEntry);

    }


    protected int multiValueSize(Object mvo) {
        throw new UnsupportedOperationException();
    }

    protected Iterator<K> multiValueIterator(Object mvo) {
        throw new UnsupportedOperationException();
    }

    public boolean isCompound() {
        return false;
    }

    static Class<?> getCommonSuperType(Class<?> type1, Class<?> type2) {
        if (type1 == null || type2 == null)
            return Object.class;

        if (type1.equals(type2))
            return type1;

        if (type1.isAssignableFrom(type2))
            return type1;

        if (type2.isAssignableFrom(type1))
            return type2;

        return getCommonSuperType(type1.getSuperclass(), type2.getSuperclass());
    }

    static boolean isImmutableIndexValue(Class<?> valueClass) {
        return _immutableTypes.contains(valueClass.getName());

    }

    static boolean isCloneableIndexValue(Class<?> valueClass) {
//TBD +++++++++++ Is it permannent ??
        //test the util.Date sql.Date in serialization .
        if (valueClass.getName().endsWith(".Date"))
            return false;
//TBD --------- Is it permannent ??

        Class<?>[] interfaces = valueClass.getInterfaces();
        boolean res = false;
        if (interfaces != null) {
            for (Class<?> clzz : interfaces) {
                if (clzz.getName().startsWith("java.util"))
                    return false;
                if (clzz.getName().equals("java.lang.Cloneable"))
                    res = true;
            }
        }
        return res;
    }

    private static Set<String> initImmutableTypes() {
        Set<String> immutableTypes = new HashSet<String>();

        // Add primitive types:
        immutableTypes.add(byte.class.getName());
        immutableTypes.add(short.class.getName());
        immutableTypes.add(int.class.getName());
        immutableTypes.add(long.class.getName());
        immutableTypes.add(float.class.getName());
        immutableTypes.add(double.class.getName());
        immutableTypes.add(boolean.class.getName());
        immutableTypes.add(char.class.getName());
        // Add primitive wrapper types:
        immutableTypes.add(Byte.class.getName());
        immutableTypes.add(Short.class.getName());
        immutableTypes.add(Integer.class.getName());
        immutableTypes.add(Long.class.getName());
        immutableTypes.add(Float.class.getName());
        immutableTypes.add(Double.class.getName());
        immutableTypes.add(Boolean.class.getName());
        immutableTypes.add(Character.class.getName());
        // Add common immutable scalar types:
        immutableTypes.add(String.class.getName());

        //  immutableTypes.add(java.util.Date.class.getName());
        immutableTypes.add(java.util.UUID.class.getName());
        //   immutableTypes.add(java.sql.Date.class.getName());
        immutableTypes.add(java.sql.Time.class.getName());
        immutableTypes.add(java.sql.Timestamp.class.getName());

        return immutableTypes;
    }

    /**
     * @return A single index value for template matching.
     */
    public Object getIndexValueForTemplate(ServerEntry entry) {
        return getIndexValue(entry);
    }


    //++++++++++++ templates indexing insertion methods +++++++++++++++++++

    static void insertIndexedTemplate(TemplateCacheInfo pTemplate, TypeData typeData, boolean extendedMatch) {
        int additionToBackrefs = 0;

        pTemplate.initBackRefs(typeData.numberOfBackRefs() + additionToBackrefs);

        boolean useBasicIndex = useBasicIndexTemplate(pTemplate, typeData, extendedMatch);

        // add template to indexes
        if (useBasicIndex) {
            insertBasicIndexTemplate(pTemplate, typeData);
        } else {
            insertExtendedIndexTemplate(pTemplate, typeData);
        }
    }


    private static boolean useBasicIndexTemplate(TemplateCacheInfo pTemplate,
                                                 final TypeData typeData, boolean extendedMatch) {
        final ICustomQuery customQuery = pTemplate.m_TemplateHolder.getCustomQuery();
        List<IQueryIndexScanner> customIndexes = customQuery == null ? null : customQuery.getCustomIndexes();
        if (!extendedMatch && (customIndexes == null || customIndexes.isEmpty()))
            return true;

        if (extendedMatch) {
            for (TypeDataIndex<?> index : typeData.getIndexes()) {
                if (index.getIndexCreationNumber() > 0)
                    continue;
                if (index.isCompound())
                    continue;
                int pos = index.getPos();

                //custom indexes only support basic index
                if (pos >= pTemplate.m_TemplateHolder.getExtendedMatchCodes().length)
                    continue;

                if (pTemplate.m_TemplateHolder.getExtendedMatchCodes()[pos] == TemplateMatchCodes.EQ) {
                    if (index.getIndexValueForTemplate(pTemplate.m_TemplateHolder.getEntryData()) != null)
                        return true;
                    continue;
                }
            }
        }

        if (customIndexes != null) {
            for (IQueryIndexScanner customIndexScanner : customIndexes) {
                if (!customIndexScanner.supportsTemplateIndex())
                    continue;

                String indexName = customIndexScanner.getIndexName();
                TypeDataIndex<?> index = typeData.getIndex(indexName);
                if (index.isCompound())
                    continue;

                if (index.getIndexCreationNumber() > 0)
                    continue;

                return true;
            }
        }

        return !extendedMatch;
    }

    static private void insertBasicIndexTemplate(TemplateCacheInfo pTemplate, final TypeData typeData) {
        final TypeDataIndex<?>[] indexes = typeData.getIndexes();
        for (TypeDataIndex<?> index : indexes) {
            if (index.getIndexCreationNumber() > 0)
                continue;

            if (index.isCompound())
                continue; //waiting templates currently not matched by compound

            int pos = index.getPos();

            //templates with extended match codes are treated as null
            boolean isNullIndex = index.getIndexValueForTemplate(pTemplate.m_TemplateHolder.getEntryData()) == null
                    || (pTemplate.m_TemplateHolder.hasExtendedMatchCodes()
                    && pos < pTemplate.m_TemplateHolder.getExtendedMatchCodes().length
                    && pTemplate.m_TemplateHolder.getExtendedMatchCodes()[pos] != TemplateMatchCodes.EQ);

            index.insertBasicIndexTemplate(pTemplate, isNullIndex);
        }
    }

    protected void insertBasicIndexTemplate(TemplateCacheInfo pTemplate, boolean isNullIndex) {
        // check if field's value is null - if so store it in null templates
        if (isNullIndex) {
            if (pTemplate.m_TemplateHolder.isNotifyTemplate())
                addNullNotifyTemplate(pTemplate);
            else
                addNullReadTakeTemplate(pTemplate);
        } else // there is a non - null value, store it in templates
        {
            if (pTemplate.m_TemplateHolder.isNotifyTemplate())
                insertBasicIndexNotifyTemplate(pTemplate);
            else
                insertBasicIndexReadTakeTemplate(pTemplate);
        }
    }

    private void insertBasicIndexReadTakeTemplate(
            TemplateCacheInfo pTemplate) {
        IObjectInfo<TemplateCacheInfo> oi = null;
        boolean alreadyCloned = false;
        Object fieldValue = getIndexValueForTemplate(pTemplate.m_TemplateHolder.getEntryData());
        IStoredList<TemplateCacheInfo>[] t_vec = _RTTemplates.get(fieldValue);
        IStoredList<TemplateCacheInfo> templates;
        for (; ; ) {
            // if found a templates SL for this field-value
            if (t_vec != null) {
                templates = t_vec[0];
                oi = templates.add(pTemplate);

                //	may have been invalidated by PersistentGC
                if (oi == null) {
                    //help remove entry for key only if currently mapped to given value
                    _RTTemplates.remove(fieldValue, t_vec);
                    t_vec = null; //help GC
                } else
                    break; // OK - done.
            }

            // either wasn't found or we just helped remove it
            if (t_vec == null) {
                // create vector
                templates = StoredListFactory.createConcurrentList(true /*supportFifo*/);
                oi = templates.add(pTemplate);
                t_vec = new IStoredList[2];
                t_vec[0] = templates;
                t_vec[1] = _RTNullTemplates;
                if (_considerValueClone && !alreadyCloned) {
                    fieldValue = cloneIndexValue((K) fieldValue, pTemplate.m_TemplateHolder);
                    alreadyCloned = true;
                }
                t_vec = _RTTemplates.putIfAbsent(fieldValue, t_vec);

                //	putIfAbsent returns null if there was no mapping for key
                if (t_vec == null)
                    break; // OK - done.
            }
        }//for(;;)

        pTemplate.m_BackRefs.add(oi);
    }


    private void insertBasicIndexNotifyTemplate(
            TemplateCacheInfo pTemplate) {
        IObjectInfo<TemplateCacheInfo> oi = null;
        boolean alreadyCloned = false;
        Object fieldValue = getIndexValueForTemplate(pTemplate.m_TemplateHolder.getEntryData());
        IStoredList<TemplateCacheInfo>[] t_vec = _NTemplates.get(fieldValue);
        IStoredList<TemplateCacheInfo> templates;
        for (; ; ) {
            // if found a templates SL for this field-value
            if (t_vec != null) {
                templates = t_vec[0];
                oi = templates.add(pTemplate);

                //	may have been invalidated by PersistentGC
                if (oi == null) {
                    //help remove entry for key only if currently mapped to given value
                    _NTemplates.remove(getIndexValueForTemplate(pTemplate.m_TemplateHolder.getEntryData()), t_vec);
                    t_vec = null; //help GC
                } else
                    break; // OK - done.
            }

            // either wasn't found or we just helped remove it
            if (t_vec == null) {
                // create vector
                templates = StoredListFactory.createConcurrentList(true /*supportFifo*/);
                oi = templates.add(pTemplate);
                t_vec = new IStoredList[2];
                t_vec[0] = templates;
                t_vec[1] = _NNullTemplates;
                if (_considerValueClone && !alreadyCloned) {
                    fieldValue = cloneIndexValue((K) fieldValue, pTemplate.m_TemplateHolder);
                    alreadyCloned = true;
                }
                t_vec = _NTemplates.putIfAbsent(fieldValue, t_vec);

                //	putIfAbsent returns null if there was no mapping for key
                if (t_vec == null)
                    break; // OK - done.
            }
        }//for(;;)

        pTemplate.m_BackRefs.add(oi);
    }


    static private void insertExtendedIndexTemplate(TemplateCacheInfo pTemplate,
                                                    final TypeData typeData) {//NOTE- we index the first extended non-null index found
        //else- we put it in the general list
        TypeDataIndex<?> index_for_NE = null;

        if (typeData.anyInitialExtendedIndex()) {
            for (TypeDataIndex<?> index : typeData.getIndexes()) {
                if (index.getIndexCreationNumber() > 0)
                    continue;
                if (!index.getIndexType().isOrdered())
                    continue;
                if (index.isCompound())
                    continue; //waiting templates currently not matched by compound

                int pos = index.getPos();

                //custom indexes only support basic index
                if (pos >= pTemplate.m_TemplateHolder.getExtendedMatchCodes().length)
                    continue;

                if (index.getIndexValueForTemplate(pTemplate.m_TemplateHolder.getEntryData()) != null) {
                    short matchCode = pTemplate.m_TemplateHolder.getExtendedMatchCodes()[pos];
                    if (matchCode == TemplateMatchCodes.GE || matchCode == TemplateMatchCodes.GT
                            || matchCode == TemplateMatchCodes.LE || matchCode == TemplateMatchCodes.LT) {
                        index.insertExtendedIndexTemplateValue(pTemplate, typeData);
                        return;

                    }
                    if (matchCode == TemplateMatchCodes.NE && index_for_NE == null)
                        index_for_NE = index;
                }
            }
        }
        if (index_for_NE != null) {//use the NE index as a second-best choice
            index_for_NE.insertExtendedIndexTemplateValue(pTemplate, typeData);
            return;
        }

        //use the general list
        insertExtendedIndexTemplateGeneralList(pTemplate, typeData);
    }

    private void insertExtendedIndexTemplateValue(TemplateCacheInfo pTemplate, final TypeData typeData) {
        //index the  extended index for template
        short matchCode = pTemplate.m_TemplateHolder.getExtendedMatchCodes()[getPos()];
        IExtendedIndex<K, TemplateCacheInfo> idx = null;
        switch (matchCode) {
            case TemplateMatchCodes.GE:
            case TemplateMatchCodes.GT:
                idx = pTemplate.m_TemplateHolder.isNotifyTemplate() ? m_Notify_GT_Index : m_RT_GT_Index;
                break;
            case TemplateMatchCodes.LE:
            case TemplateMatchCodes.LT:
                idx = pTemplate.m_TemplateHolder.isNotifyTemplate() ? m_Notify_LT_Index : m_RT_LT_Index;
                break;
            case TemplateMatchCodes.NE:
                idx = pTemplate.m_TemplateHolder.isNotifyTemplate() ? m_Notify_NE_Index : m_RT_NE_Index;
                break;

        }
        pTemplate.m_BackRefs.add(idx.insertEntryIndexedField(pTemplate, (K) getIndexValueForTemplate(pTemplate.m_TemplateHolder.getEntryData()), typeData, false));
    }


    static private void insertExtendedIndexTemplateGeneralList(TemplateCacheInfo pTemplate, TypeData typeData) {
        //for custom index or extended index like ISNULL or NOTNULL or when
        //templates values are null - we use a general extended indexes vector
        IObjectInfo<TemplateCacheInfo> oi;
        if (pTemplate.m_TemplateHolder.isNotifyTemplate())
            oi = typeData.getNotifyExtendedTemplates().add(pTemplate);
        else/* READ, READ_IE, TAKE, TAKE_IE */
            oi = typeData.getReadTakeExtendedTemplates().add(pTemplate);

        pTemplate.m_BackRefs.add(oi);
    }


    //++++++++  templates indexing removal methods +++++++++++++++++++
    static int removeIndexedTemplate(TemplateCacheInfo pTemplate,
                                     boolean extendedMatch, IObjectInfo<TemplateCacheInfo> oi,
                                     int refpos, TypeData typeData) {
        // remove template  indexes
        if (useBasicIndexTemplate(pTemplate, typeData, extendedMatch)) {
            refpos = removeBasicIndexTemplate(pTemplate,
                    oi,
                    refpos,
                    typeData);
        } else {
            refpos = removeExtendedIndexTemplate(pTemplate, refpos, typeData);
        }
        return refpos;
    }


    static private int removeExtendedIndexTemplate(TemplateCacheInfo pTemplate,
                                                   int refpos, final TypeData typeData) {//NOTE- we index the first extended index found
        TypeDataIndex<?> index_for_NE = null;
        if (typeData.anyInitialExtendedIndex()) {
            for (TypeDataIndex<?> index : typeData.getIndexes()) {
                if (index.getIndexCreationNumber() > 0)
                    continue;
                if (!index.getIndexType().isOrdered())
                    continue;
                if (index.isCompound())
                    continue; //waiting templates currently not matched by compound

                int pos = index.getPos();

                //custom indexes only support basic index
                if (pos >= pTemplate.m_TemplateHolder.getExtendedMatchCodes().length)
                    continue;


                if (index.getIndexValueForTemplate(pTemplate.m_TemplateHolder.getEntryData()) != null) {
                    short matchCode = pTemplate.m_TemplateHolder.getExtendedMatchCodes()[pos];
                    if (matchCode == TemplateMatchCodes.GE || matchCode == TemplateMatchCodes.GT
                            || matchCode == TemplateMatchCodes.LE || matchCode == TemplateMatchCodes.LT) {
                        return index.removeExtendedIndexTemplateValue(pTemplate, refpos);

                    }
                    if (matchCode == TemplateMatchCodes.NE && index_for_NE == null)
                        index_for_NE = index;
                }
            }
        }
        if (index_for_NE != null) {//use the NE index as a second-best choice
            return index_for_NE.removeExtendedIndexTemplateValue(pTemplate, refpos);
        }

        //use the general list
        return removeExtendedIndexTemplateGeneralList(pTemplate, refpos, typeData);
    }

    private int removeExtendedIndexTemplateValue(TemplateCacheInfo pTemplate, int refpos) {
        //index the  extended index for template
        short matchCode = pTemplate.m_TemplateHolder.getExtendedMatchCodes()[getPos()];
        IExtendedIndex<K, TemplateCacheInfo> idx = null;
        IObjectInfo<TemplateCacheInfo> oi = pTemplate.m_BackRefs.get(refpos++);
        switch (matchCode) {
            case TemplateMatchCodes.GE:
            case TemplateMatchCodes.GT:
                idx = pTemplate.m_TemplateHolder.isNotifyTemplate() ? m_Notify_GT_Index : m_RT_GT_Index;
                break;
            case TemplateMatchCodes.LE:
            case TemplateMatchCodes.LT:
                idx = pTemplate.m_TemplateHolder.isNotifyTemplate() ? m_Notify_LT_Index : m_RT_LT_Index;
                break;
            case TemplateMatchCodes.NE:
                idx = pTemplate.m_TemplateHolder.isNotifyTemplate() ? m_Notify_NE_Index : m_RT_NE_Index;
                break;

        }

        idx.removeEntryIndexedField(pTemplate.m_TemplateHolder,
                (K) getIndexValueForTemplate(pTemplate.m_TemplateHolder.getEntryData()), pTemplate, oi);

        return refpos;
    }

    private static int removeExtendedIndexTemplateGeneralList(TemplateCacheInfo pTemplate,
                                                              int refpos, TypeData typeData) {
        IObjectInfo<TemplateCacheInfo> oi = pTemplate.m_BackRefs.get(refpos++);
        if (pTemplate.m_TemplateHolder.isNotifyTemplate())
            typeData.getNotifyExtendedTemplates().remove(oi);
        else /* READ, READ_IE, TAKE, TAKE_IE */
            typeData.getReadTakeExtendedTemplates().remove(oi);
        return refpos;
    }


    private static int removeBasicIndexTemplate(TemplateCacheInfo pTemplate,
                                                IObjectInfo<TemplateCacheInfo> oi, int refpos, TypeData typeData) {
        TypeDataIndex[] indexes;
        indexes = typeData.getIndexes();
        for (TypeDataIndex<?> index : indexes) {
            if (index.getIndexCreationNumber() > pTemplate.getLatestIndexCreationNumber())
                continue;   //template was not indexed by this one yet
            if (index.isCompound())
                continue; //waiting templates currently not matched by compound

            refpos = index.removeBasicIndexTemplate(pTemplate, oi, refpos);
        }
        return refpos;
    }

    protected int removeBasicIndexTemplate(TemplateCacheInfo pTemplate,
                                           IObjectInfo<TemplateCacheInfo> oi, int refpos) {
        int pos = getPos();
        oi = pTemplate.m_BackRefs.get(refpos++);
        final Object indexValueForTemplate = getIndexValueForTemplate(pTemplate.m_TemplateHolder.getEntryData());

        //templates with extended match codes are treated as null
        boolean isNullIndex = indexValueForTemplate == null
                || (pTemplate.m_TemplateHolder.hasExtendedMatchCodes()
                && pos < pTemplate.m_TemplateHolder.getExtendedMatchCodes().length
                && pTemplate.m_TemplateHolder.getExtendedMatchCodes()[pos] != TemplateMatchCodes.EQ);

        // check if field's value is null - if so store it in null templates
        if (isNullIndex) {
            if (pTemplate.m_TemplateHolder.isNotifyTemplate())
                _NNullTemplates.remove(oi);
            else
                _RTNullTemplates.remove(oi);
        } else // there is a non - null value,
        {
            IStoredList<TemplateCacheInfo>[] t_vec;

            if (pTemplate.m_TemplateHolder.isNotifyTemplate())
                t_vec = _NTemplates.get(indexValueForTemplate);
            else /* READ, READ_IE, TAKE, TAKE_IE */
                t_vec = _RTTemplates.get(indexValueForTemplate);
            t_vec[0].remove(oi);
        }
        return refpos;
    }

    //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    //++++++++++++++ index searching methods of templates according to entry 

    /**
     * add, to the result of getTemplatesMinIndex, the extended-search templates. extended search
     * templates are stored per type and not per index-value
     */

    static Object getTemplatesExtendedIndexSearch(TypeData templateType, MatchTarget matchTarget, IEntryHolder entry, Object originalResult) {
        boolean notify = matchTarget == MatchTarget.NOTIFY;

        List newResult = null;
        final IEntryData entryData = entry.getEntryData();
        final TypeDataIndex<?>[] indexes = templateType.getIndexes();
        boolean any_zero_field_value = false;
        if (templateType.anyInitialExtendedIndex()) {
            for (TypeDataIndex<?> index : indexes) {
                if (index.getIndexCreationNumber() > 0)
                    continue;   //irrelevant for templates index
                if (!index.getIndexType().isOrdered())
                    continue;
                if (index.isCompound())
                    continue;  //currently waiting templates selection is not supported
                Object entryValue = index.getIndexValue(entryData);
                any_zero_field_value = any_zero_field_value | (entryValue == null);
                if (index.getExtendedIndexForScanning() == null)
                    continue; //ext' index not defined
                IExtendedIndexIterator<TemplateCacheInfo> resultOIS = null;
                if (!any_zero_field_value) {
                    //try GT/GE index
                    newResult = integrateTemplatesResult(newResult, originalResult, index.getExtendedTemplatesIndexSearch(entryValue, notify, TemplateMatchCodes.GE, TemplateMatchCodes.LE));
                    //try LT/LE index
                    newResult = integrateTemplatesResult(newResult, originalResult, index.getExtendedTemplatesIndexSearch(entryValue, notify, TemplateMatchCodes.LE, TemplateMatchCodes.GE));
                    //try NE index one-edge
                    newResult = integrateTemplatesResult(newResult, originalResult, index.getExtendedTemplatesIndexSearch(entryValue, notify, TemplateMatchCodes.NE, TemplateMatchCodes.GT));
                    //try NE index second-edge
                    newResult = integrateTemplatesResult(newResult, originalResult, index.getExtendedTemplatesIndexSearch(entryValue, notify, TemplateMatchCodes.NE, TemplateMatchCodes.LT));
                }

            } /* for */
        }

        // add the general list which contains custom-queries
        IStoredList gl = templateType.getExtendedTemplates(matchTarget);
        if (gl != null && !gl.isEmpty())
            newResult = integrateTemplatesResult(newResult, originalResult, gl);

        return newResult != null ? newResult : originalResult;

    }

    private static List integrateTemplatesResult(List newResult, Object originalResult, Object searchResult) {
        if (searchResult != null) {
            if (newResult == null) {
                if (originalResult instanceof List) {
                    newResult = (List) originalResult;
                } else {
                    newResult = new ArrayList();
                    if (originalResult instanceof IStoredList)
                        newResult.add(originalResult);
                    else
                        newResult.addAll(Arrays.asList((IStoredList[]) originalResult));
                }
            }
            newResult.add(searchResult);
        }
        return newResult;
    }


    IScanListIterator<TemplateCacheInfo> getExtendedTemplatesIndexSearch(Object fieldValue, boolean isNotifySearch, short templatesMatchCode, short scanMatchCode) {
        IExtendedIndex<K, TemplateCacheInfo> idx = null;
        switch (templatesMatchCode) {
            case TemplateMatchCodes.GE:
            case TemplateMatchCodes.GT:
                idx = isNotifySearch ? m_Notify_GT_Index : m_RT_GT_Index;
                break;
            case TemplateMatchCodes.LE:
            case TemplateMatchCodes.LT:
                idx = isNotifySearch ? m_Notify_LT_Index : m_RT_LT_Index;
                break;
            case TemplateMatchCodes.NE:
                idx = isNotifySearch ? m_Notify_NE_Index : m_RT_NE_Index;
                break;

        }

        return idx.establishScan((K) fieldValue, scanMatchCode, null, true);

    }


    public TypeDataIndex getCompoundFifoGroupsIndexForSegment() {
        return _compoundFifoGroupsIndex;
    }

    public void setCompoundFifoGroupsIndexForSegment(TypeDataIndex fgIndex) {
        _compoundFifoGroupsIndex = fgIndex;
    }

    public IFifoGroupsIndexExtention<K> getFifoGroupsIndexExtention() {
        return _fifoGroupsIndexExtention;
    }

    public TypeDataIndex[] getSegmentsOriginatingIndexes() {
        throw new UnsupportedOperationException();//only for compound
    }

    public CompoundIndexSegmentTypeData[] getCompoundIndexSegments() {
        throw new UnsupportedOperationException();//only for compound
    }

    //TBD should remove this method
    public Object getCompoundIndexValueForTemplate(ServerEntry entry) {
        throw new UnsupportedOperationException();//only for compound
    }


    /**
     * modes of updating an index. since the introduction of unique index we are doing it in 2
     * modes
     */
    public static enum UpdateIndexModes {
        REPLACE_NONEQUALS, /* insert + remove*/
        INSERT_NONEQUALS,  /*insert only */
        REMOVE_NONEQUALS  /*remove only*/
    }

    public TemplatesExtendedIndexHandler<K> getM_RT_GT_Index() {
        return m_RT_GT_Index;
    }

    public TemplatesExtendedIndexHandler<K> getM_RT_LT_Index() {
        return m_RT_LT_Index;
    }

    public TemplatesExtendedIndexHandler<K> getM_RT_NE_Index() {
        return m_RT_NE_Index;
    }
}
