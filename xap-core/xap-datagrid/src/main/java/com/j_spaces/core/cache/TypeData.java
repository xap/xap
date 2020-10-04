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

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.metadata.TypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.MatchTarget;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.server.space.metadata.TypeDataFactory;
import com.gigaspaces.internal.server.storage.*;
import com.gigaspaces.metadata.StorageType;
import com.gigaspaces.metadata.index.CompoundIndex;
import com.gigaspaces.metadata.index.ISpaceCompoundIndexSegment;
import com.gigaspaces.metadata.index.ISpaceIndex;
import com.gigaspaces.metadata.index.ISpaceIndex.FifoGroupsIndexTypes;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.query.extension.metadata.TypeQueryExtensions;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.admin.TemplateInfo;
import com.j_spaces.core.cache.TypeDataIndex.UpdateIndexModes;
import com.j_spaces.core.cache.fifoGroup.FifoGroupCacheImpl;
import com.j_spaces.core.client.SequenceNumberException;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.IStoredListIterator;
import com.j_spaces.kernel.StoredListFactory;
import com.j_spaces.kernel.list.IScanListIterator;
import com.j_spaces.kernel.list.ScanSingleListIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@com.gigaspaces.api.InternalApi
public class TypeData {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE);
    private static final int _fifoCountRecheckFrom = Integer.MAX_VALUE - 100000;

    private final CacheManager _cacheManager;
    private final String _className;
    private final String _classUidPrefix;
    private final PropertyInfo[] _properties;

    private final ConcurrentHashMap<String, TypeDataIndex<?>> _indexTable;
    private final boolean _hasIndexes;
    private final boolean _hasInitialIndexes;
    private final TypeDataIndex<Object>[] _indexes;
    private final int _numOfBackRefs;
    private final int _numOfCustomIndexes;

    //indexes that support fifo-grouping. note- dynamic indexes are not allowed to support f-g
    private final TypeDataIndex<Object> _fifoGroupingIndex;

    //if spaceId is defined for this class its the index field
    private final TypeDataIndex<?> _idPropertyIndex;
    private final LongCounter _uidUsageCounter;

    private IStoredList<IEntryCacheInfo> _entries;

    private final IStoredList<TemplateCacheInfo> _readTakeTemplates;
    private final IStoredList<TemplateCacheInfo> _notifyTemplates;

    //extended-search templates. note- extended search templates CANNOT be inserted/searched on an index-level value basis
    private final IStoredList<TemplateCacheInfo> _readTakeExtendedTemplates;
    private final IStoredList<TemplateCacheInfo> _notifyExtendedTemplates;

    /**
     * _ByUidTemplatesIndex is a special index that is storing templates waiting for an entry with
     * specific UID. Those templates are NOT index by their fields, just by the waiting UID
     *
     * key = UID, value = storedList of templates
     */
    private final ConcurrentHashMap<String, IStoredList<TemplateCacheInfo>> _readTakeUidTemplates;
    private final ConcurrentHashMap<String, IStoredList<TemplateCacheInfo>> _notifyUidTemplates;

    private final boolean _fifoSupport;

    private final AtomicInteger _nextInFifoOrder;

    //the number for regular notify templates currently stored in this class (optimistic)
    private final AtomicInteger m_NumRegularNotifyTemplatesStored;

    // the number for durable notify templates currenlty stored in this class
    private final AtomicInteger _numDurableNotifyTemplatesStored;

    //for each column- its ordinal in the entryHolder  
    private final ConcurrentHashMap<String, Integer> _columnsOrdinalsByName;

    private final TypeDataFactory _typeDataFactory;

    //its true if this type data object was replace by a newer version
    //type data is replaced when indexes are added
    private volatile boolean _typeDataReplaced;

    //the following fields are relevant for dynamic indexing
    //changing the following fields results in creating a new typeData= Copy on write
    private final boolean _supportsDynamicIndexing;

    //protect against concurrent changes to the same type
    private final ReentrantReadWriteLock _typeLock;

    //last index completed creation
    private final int _lastIndexCreationNumber;
    //last index being created or null
    private final int _lastIndexPendingCreationNumber;
    //is at least one index extended in the initial typeData?
    private final boolean _anyInitialExtendedIndex;
    //does this type allow fifo index scans
    private final boolean _allowFifoIndexScans;

    private final List<TypeDataIndex> _compoundIndexes;
    private final List<CompoundIndex> _compoundIndexDefinitions;
    private final boolean _anyNonFGCompoundIndex;

    private final List<TypeDataIndex> _uniqueIndexes;
    private final boolean _isLocalCache;

    private final boolean _isAutoGeneratedId;

    private final boolean _isBlobStoreClass;

    //the following describes the properties taking place in indexes(inc' segments)
    //used in off-heap initial  load of fifo/F-G classes
    private final boolean[] _indexesRelatedFixedProperties;
    private final HashSet<String> _indexesRelatedDynamicProperties;

    //the following relates to sequenceNumber
    private final SequenceNumberGenerator _sequenceNumberGenerator;
    private final TypeDataIndex<?> _sequenceNumberIndex; //null if not indexed or undefined

    //reasons for replacing type-data
    public static enum TypeDataRecreationReasons {
        DYNAMIC_INDEX_CREATION, DYNAMIC_INDEX_CREATION_COMPLETION
    }

    public static String Compound_Fifo_Groups_Index_Name_Suffix = "_COMPOUND_FIFO_GROUPS";

    private volatile Set<QueryExtensionIndexManagerWrapper> queryExtensionIndexManagers = new HashSet<QueryExtensionIndexManagerWrapper>();

    @Override
    public String toString() {
        return "TypeData: " + this._className;
    }

    public TypeData(IServerTypeDesc serverTypeDesc, TypeDataFactory typeDataFactory, boolean isLocalCache, boolean isResidentEntriesCachePolicy) {
        if (serverTypeDesc == null)
            throw new IllegalArgumentException("Argument cannot be null - 'serverTypeDesc'.");
        if (serverTypeDesc.isInactive())
            throw new IllegalArgumentException("Cannot create TypeData for inactive type descriptor - '" + serverTypeDesc.getTypeName() + "'.");

        _cacheManager = typeDataFactory.getCcheManager();
        _nextInFifoOrder = new AtomicInteger(0);
        m_NumRegularNotifyTemplatesStored = new AtomicInteger(0);
        _numDurableNotifyTemplatesStored = new AtomicInteger(0);
        _supportsDynamicIndexing = true; //currently allow all types
        _lastIndexCreationNumber = 0;
        _lastIndexPendingCreationNumber = 0;
        _typeDataFactory = typeDataFactory;
        _isLocalCache = isLocalCache;
        _isAutoGeneratedId = serverTypeDesc.getTypeDesc().isAutoGenerateId();

        _className = serverTypeDesc.getTypeName();
        _classUidPrefix = serverTypeDesc.getTypeDesc().getTypeUidPrefix();
        _properties = serverTypeDesc.getTypeDesc().getProperties();
        _indexTable = new ConcurrentHashMap<String, TypeDataIndex<?>>();

        _fifoSupport = serverTypeDesc.isFifoSupported();
        TypeDataIndex<Object> fifoGroupingIndex = null;
        ISpaceIndex fifoGroupingIndexDef = null;

        int numOfExtendedIndexes = 0;

        _columnsOrdinalsByName = new ConcurrentHashMap<String, Integer>();
        final Map<String, SpaceIndex> typeIndexes = serverTypeDesc.getTypeDesc().getIndexes();
        final ArrayList<TypeDataIndex<?>> indexes = new ArrayList<TypeDataIndex<?>>();
        boolean anyRequestsForFGCompound = false;
        _compoundIndexes = new ArrayList<TypeDataIndex>();

        String fifoGroupingName = serverTypeDesc.getTypeDesc().getFifoGroupingPropertyPath();
        Set<String> fifoGroupingIndexes = serverTypeDesc.getTypeDesc().getFifoGroupingIndexesPaths();
        List<CompoundIndex> compoundIndexesToBuild = new ArrayList<CompoundIndex>();
        List<TypeDataIndex> uniqueIndexes = null;

        boolean[] indexesRelatedFixedProperties = new boolean[_properties.length];
        HashSet<String> indexesRelatedDynamicProperties = new HashSet<String>();

        _isBlobStoreClass = typeDataFactory.getCcheManager().isblobStoreDataSpace() && serverTypeDesc.getTypeDesc().isBlobstoreEnabled();

        if (serverTypeDesc.getTypeDesc().hasSequenceNumber()) {
            if (_cacheManager.isEvictableCachePolicy() && !_cacheManager.isMemorySpace())
                throw new SequenceNumberException(_className, " sequence number not supported with persistent LRU");

            _sequenceNumberGenerator = new SequenceNumberGenerator();
        } else
            _sequenceNumberGenerator = null;
        TypeDataIndex<?> sequenceNumberIndex = null;


        for (int i = 0; i < _properties.length; i++) {
            PropertyInfo property = _properties[i];
            _columnsOrdinalsByName.put(property.getName(), i);

            SpaceIndex index = typeIndexes.get(property.getName());
            if (index != null && index.getIndexType().isIndexed()) {
                if (((ISpaceIndex) index).isUnique()) {
                    ISpaceIndex u = (ISpaceIndex) index;
                    String idProperty = serverTypeDesc.getTypeDesc().getIdPropertyName();
                    if (idProperty == null || !idProperty.equals(u.getName())) {
                        if (isLocalCache)
                            u.setUnique(false); //no uniques in local cache-view  just the id
                        else if (!isResidentEntriesCachePolicy)
                            throw new UnsupportedOperationException("unique indices not supported for evictable cache policy=" + serverTypeDesc.getTypeName() + " index=" + u.getName());
                    }
                }
                if (index.getIndexType().isOrdered())
                    numOfExtendedIndexes++;
                TypeDataIndex currIndex = null;
                if (!((ISpaceIndex) index).isMultiValuePerEntryIndex()) {
                    FifoGroupsIndexTypes fifoGroupingType = getIndexFifoGroupingType(index, fifoGroupingName, fifoGroupingIndexes);
                    if (fifoGroupingType == FifoGroupsIndexTypes.AUXILIARY)
                        anyRequestsForFGCompound = true;
                    currIndex = typeDataFactory.createTypeDataIndex(_typeDataFactory.getCcheManager(), getInternalIndex(index), i, 0 /*indexCreationNumber*/, property.getType(), fifoGroupingType);
                } else
                    currIndex = typeDataFactory.createMultiValuePerEntryTypeDataIndex(_typeDataFactory.getCcheManager(), getInternalIndex(index), i, 0 /*indexCreationNumber*/, ((ISpaceIndex) index).getMultiValueIndexType());

                if (hasSequenceNumber() && serverTypeDesc.getTypeDesc().getSequenceNumberFixedPropertyID() == i) {//the sequence # is an index too
                    if (currIndex.isUniqueIndex() && _isLocalCache) {
                        ((ISpaceIndex) index).setUnique(false); //no uniques seq-number in local cache-view
                    }
                    sequenceNumberIndex = currIndex;
                }

                indexes.add(currIndex);
                _indexTable.put(property.getName(), currIndex);
                indexesRelatedFixedProperties[i] = true;
                if (currIndex.isUniqueIndex()) {
                    if (uniqueIndexes == null)
                        uniqueIndexes = new ArrayList<TypeDataIndex>();
                    uniqueIndexes.add(currIndex);
                }
                if (currIndex.isFifoGroupsMainIndex()) {
                    if (fifoGroupingIndex != null)
                        throw new UnsupportedOperationException(" only 1 fifo-group index can be defined per type");
                    fifoGroupingIndex = currIndex;
                    fifoGroupingIndexDef = (ISpaceIndex) index;
                }

            }
        }

        String idProperty = serverTypeDesc.getTypeDesc().getIdPropertyName();
        _idPropertyIndex = idProperty != null ? _indexTable.get(idProperty) : null;
        _uidUsageCounter = _idPropertyIndex != null && _isAutoGeneratedId ? _idPropertyIndex.getUsageCounter() : new LongCounter();

        // go over type descriptor custom indexes and convert to internal representation
        // TODO handle extended indexes and consolidate with property indexes
        int indexPosition = _properties.length;
        int numOfCustomIndexes = 0;
        if (typeIndexes != null) {
            for (Entry<String, SpaceIndex> entry : typeIndexes.entrySet()) {
                // Protect from indexes created by properties.
                if (_indexTable.containsKey(entry.getKey()))
                    continue;
                final SpaceIndex spaceIndex = entry.getValue();
                if (!spaceIndex.getIndexType().isIndexed())
                    continue;
                if (((ISpaceIndex) spaceIndex).isUnique()) {
                    ISpaceIndex u = (ISpaceIndex) spaceIndex;
                    if (isLocalCache)
                        u.setUnique(false); //no uniques in local cache-view  just the id
                    else if (!isResidentEntriesCachePolicy)
                        throw new UnsupportedOperationException("unique indices not supported for non All-In_cache policy type=" + serverTypeDesc.getTypeName() + " index=" + u.getName());
                }

                if (((ISpaceIndex) spaceIndex).isCompoundIndex()) {//after all regular indices are created- will create compounds
                    compoundIndexesToBuild.add((CompoundIndex) spaceIndex);
                    continue;
                }
                TypeDataIndex currIndex = null;

                if (!((ISpaceIndex) spaceIndex).isMultiValuePerEntryIndex()) {
                    FifoGroupsIndexTypes fifoGroupingType = getIndexFifoGroupingType(spaceIndex, fifoGroupingName, fifoGroupingIndexes);
                    if (fifoGroupingType == FifoGroupsIndexTypes.AUXILIARY)
                        anyRequestsForFGCompound = true;
                    currIndex = typeDataFactory.createCustomTypeDataIndex(_typeDataFactory.getCcheManager(), getInternalIndex(spaceIndex), indexPosition++, 0 /* index creation #*/, fifoGroupingType);
                } else
                    currIndex = typeDataFactory.createCustomMultiValuePerEntryTypeDataIndex(_typeDataFactory.getCcheManager(), getInternalIndex(spaceIndex), indexPosition++, 0 /* index creation #*/, ((ISpaceIndex) spaceIndex).getMultiValueIndexType());
                indexes.add(currIndex);
                _indexTable.put(spaceIndex.getName(), currIndex);

                addIndexRelatedProperties(serverTypeDesc, spaceIndex, currIndex, false, indexesRelatedFixedProperties, indexesRelatedDynamicProperties);

                if (currIndex.isUniqueIndex()) {
                    if (uniqueIndexes == null)
                        uniqueIndexes = new ArrayList<TypeDataIndex>();
                    uniqueIndexes.add(currIndex);
                }
                numOfCustomIndexes++;
                if (currIndex.isFifoGroupsMainIndex()) {
                    if (fifoGroupingIndex != null)
                        throw new UnsupportedOperationException(" only 1 fifo-grouping can be defined per type");
                    fifoGroupingIndex = currIndex;
                    fifoGroupingIndexDef = (ISpaceIndex) spaceIndex;
                }

            }
        }

        _fifoGroupingIndex = fifoGroupingIndex;
        _allowFifoIndexScans = _fifoSupport || (_fifoGroupingIndex != null);

        //are any compound indices requested ?
        for (CompoundIndex compoundIndex : compoundIndexesToBuild) {
            TypeDataIndex compound = TypeData.buildCompoundIndex(compoundIndex, _indexTable, serverTypeDesc, indexPosition++, ISpaceIndex.FifoGroupsIndexTypes.NONE, 0 /*indexCreationNumber*/, typeDataFactory);
            indexes.add(compound);
            _compoundIndexes.add(compound);
            if (compound.isUniqueIndex()) {
                if (uniqueIndexes == null)
                    uniqueIndexes = new ArrayList<TypeDataIndex>();
                uniqueIndexes.add(compound);
            }

            addCompundIndexRelatedProperties(serverTypeDesc, compound, false, indexesRelatedFixedProperties, indexesRelatedDynamicProperties);
            numOfCustomIndexes++;
            if (compound.isExtendedIndex())
                numOfExtendedIndexes++;
        }

        _compoundIndexDefinitions = compoundIndexesToBuild;
        _anyNonFGCompoundIndex = !_compoundIndexes.isEmpty();

        //create compund indexes defined as auxiliary for the fifo-group index
        if (_fifoGroupingIndex != null && anyRequestsForFGCompound) {

            List<TypeDataIndex> fgCompounds = FifoGroupCacheImpl.createFifoGroupCompoundIndexes(_properties, typeIndexes, _indexTable,
                    fifoGroupingIndex, fifoGroupingIndexDef, typeDataFactory,
                    indexPosition, fifoGroupingName, fifoGroupingIndexes, _compoundIndexDefinitions, serverTypeDesc);

            numOfCustomIndexes += fgCompounds.size();
            for (TypeDataIndex c : fgCompounds) {
                _compoundIndexes.add(c);
                indexes.add(c);
                if (c.isExtendedIndex())
                    numOfExtendedIndexes++;
                addCompundIndexRelatedProperties(serverTypeDesc, c, false, indexesRelatedFixedProperties, indexesRelatedDynamicProperties);
            }
        }

        _hasIndexes = _indexTable.size() != 0;
        _hasInitialIndexes = _hasIndexes;
        _numOfBackRefs = _indexTable.size() + numOfExtendedIndexes + 1;
        _numOfCustomIndexes = numOfCustomIndexes;
        _anyInitialExtendedIndex = numOfExtendedIndexes > 0;

        _entries = createEntriesStoredList(_fifoSupport || _fifoGroupingIndex != null);

        _readTakeTemplates = createStoreList();
        _notifyTemplates = createStoreList();

        _readTakeExtendedTemplates = createStoreList();
        _notifyExtendedTemplates = createStoreList();

        _readTakeUidTemplates = new ConcurrentHashMap<String, IStoredList<TemplateCacheInfo>>();
        _notifyUidTemplates = new ConcurrentHashMap<String, IStoredList<TemplateCacheInfo>>();

        _typeLock = new ReentrantReadWriteLock();
        TypeDataIndex<Object>[] temp_i = new TypeDataIndex[indexes.size()];
        if (indexes.size() > 0)
            temp_i = indexes.toArray(temp_i);
        _indexes = temp_i;
        if (uniqueIndexes != null)
            _uniqueIndexes = new ArrayList<TypeDataIndex>(uniqueIndexes);
        else
            _uniqueIndexes = null;

        _indexesRelatedFixedProperties = indexesRelatedFixedProperties;
        _indexesRelatedDynamicProperties = indexesRelatedDynamicProperties;

        _sequenceNumberIndex = sequenceNumberIndex;

        ITypeDesc typeDesc = serverTypeDesc.getTypeDesc();
        final TypeQueryExtensions queryExtensionsInfo = typeDesc.getQueryExtensions();
        if (queryExtensionsInfo != null) {
            for (String namespace : queryExtensionsInfo.getNamespaces()) {
                try {
                    QueryExtensionIndexManagerWrapper manager = _cacheManager.getQueryExtensionManager(namespace);
                    manager.introduceType(typeDesc);
                    queryExtensionIndexManagers.add(manager);
                } catch (Exception e) {
                    if (_logger.isWarnEnabled())
                        _logger.warn("Failed to install query extension manager for " + namespace, e);
                }
            }
        }
    }

    public static FifoGroupsIndexTypes getIndexFifoGroupingType(SpaceIndex spaceIndex, String fifoGroupingName, Set<String> fifoGroupingIndexes) {
        if (spaceIndex.getName().equals(fifoGroupingName))
            return FifoGroupsIndexTypes.MAIN;
        if (fifoGroupingIndexes != null && fifoGroupingIndexes.contains(spaceIndex.getName()))
            return FifoGroupsIndexTypes.AUXILIARY;
        return FifoGroupsIndexTypes.NONE;
    }


    /**
     * given an original type data create a new one- used to add new indexes
     *
     * @param reason - reason for replacing the type-data
     */
    public TypeData(IServerTypeDesc serverTypeDesc, TypeData originalTypeData, TypeDataRecreationReasons reason) {
        if (!originalTypeData.supportsDynamicIndexing())
            throw new UnsupportedOperationException();

        _cacheManager = originalTypeData._cacheManager;
        _nextInFifoOrder = originalTypeData._nextInFifoOrder;
        m_NumRegularNotifyTemplatesStored = originalTypeData.m_NumRegularNotifyTemplatesStored;
        _numDurableNotifyTemplatesStored = originalTypeData._numDurableNotifyTemplatesStored;
        _supportsDynamicIndexing = true; //currently allow all types
        _typeDataFactory = originalTypeData._typeDataFactory;
        _isLocalCache = originalTypeData._isLocalCache;
        _isAutoGeneratedId = originalTypeData._isAutoGeneratedId;

        _className = originalTypeData._className;
        _classUidPrefix = originalTypeData._classUidPrefix;
        _properties = serverTypeDesc.getTypeDesc().getProperties();

        _fifoSupport = originalTypeData._fifoSupport;
        _columnsOrdinalsByName = originalTypeData._columnsOrdinalsByName;

        _entries = originalTypeData._entries;
        _typeLock = originalTypeData._typeLock;

        _hasInitialIndexes = originalTypeData._hasInitialIndexes;
        _anyInitialExtendedIndex = originalTypeData._anyInitialExtendedIndex;
        _isBlobStoreClass = originalTypeData._isBlobStoreClass;
        boolean[] indexesRelatedFixedProperties = new boolean[originalTypeData._indexesRelatedFixedProperties.length];
        System.arraycopy(originalTypeData._indexesRelatedFixedProperties, 0, indexesRelatedFixedProperties, 0, indexesRelatedFixedProperties.length);
        HashSet<String> indexesRelatedDynamicProperties = new HashSet<String>(originalTypeData._indexesRelatedDynamicProperties);
        _sequenceNumberGenerator = originalTypeData._sequenceNumberGenerator;
        TypeDataIndex<?> sequenceNumberIndex = originalTypeData._sequenceNumberIndex;

        if (reason == TypeDataRecreationReasons.DYNAMIC_INDEX_CREATION) {
            final ArrayList<TypeDataIndex<?>> indexes = new ArrayList<TypeDataIndex<?>>();
            final List<TypeDataIndex> compoundIndexes = new ArrayList<TypeDataIndex>(originalTypeData._compoundIndexes);
            boolean anyNonFGCompoundIndex = originalTypeData._anyNonFGCompoundIndex;
            int numOfExtendedIndexes = 0;
            _indexTable = originalTypeData._indexTable;
            final HashMap<String, TypeDataIndex<?>> nonCustomIndexes = new HashMap<String, TypeDataIndex<?>>();
            List<CompoundIndex> compoundIndexDefinitions = new ArrayList<CompoundIndex>(originalTypeData._compoundIndexDefinitions);
            List<CompoundIndex> newCompounds = new ArrayList<CompoundIndex>();

            _lastIndexCreationNumber = originalTypeData._lastIndexCreationNumber;
            int numNewIndexes = 0;

            final Map<String, SpaceIndex> typeIndexes = serverTypeDesc.getTypeDesc().getIndexes();
            for (int i = 0; i < _properties.length; i++) {
                PropertyInfo property = _properties[i];
                _columnsOrdinalsByName.put(property.getName(), i);

                SpaceIndex index = typeIndexes.get(property.getName());
                if (index != null && index.getIndexType().isIndexed()) {
                    if (index.getIndexType().isOrdered())
                        numOfExtendedIndexes++;

                    TypeDataIndex currIndex = _indexTable.get(property.getName());

                    boolean newIndex = currIndex == null;
                    if (newIndex) {
                        if (((ISpaceIndex) index).isUnique()) {
                            ISpaceIndex u = (ISpaceIndex) index;
                            if (_isLocalCache)
                                u.setUnique(false); //no uniques in local cache-view  just the id
                            else
                                throw new UnsupportedOperationException("dynamic unique indices not supported type=" + serverTypeDesc.getTypeName() + " index=" + u.getName());
                        }
                        numNewIndexes++;
                        if (!((ISpaceIndex) index).isMultiValuePerEntryIndex())
                            currIndex = _typeDataFactory.createTypeDataIndex(_typeDataFactory.getCcheManager(), getInternalIndex(index), i, _lastIndexCreationNumber + numNewIndexes /* index creation #*/, property.getType(), ISpaceIndex.FifoGroupsIndexTypes.NONE);
                        else
                            currIndex = _typeDataFactory.createMultiValuePerEntryTypeDataIndex(_typeDataFactory.getCcheManager(), getInternalIndex(index), i, _lastIndexCreationNumber + numNewIndexes /* index creation #*/, ((ISpaceIndex) index).getMultiValueIndexType());

                        if (hasSequenceNumber() && serverTypeDesc.getTypeDesc().getSequenceNumberFixedPropertyID() == i) {//the sequence # is an index too
                            sequenceNumberIndex = currIndex;
                        }
                        indexesRelatedFixedProperties[i] = true;
                    }

                    indexes.add(currIndex);
                    if (newIndex)
                        _indexTable.put(property.getName(), currIndex);

                    nonCustomIndexes.put(property.getName(), currIndex);
                }
            }

            String idProperty = serverTypeDesc.getTypeDesc().getIdPropertyName();
            _idPropertyIndex = idProperty != null ? _indexTable.get(idProperty) : null;
            _uidUsageCounter = _idPropertyIndex != null && _isAutoGeneratedId ? _idPropertyIndex.getUsageCounter() : new LongCounter();

            // go over type descriptor custom indexes and convert to internal representation
            // TODO handle extended indexes and consolidate with property indexes
            int indexPosition = _properties.length;
            int numOfCustomIndexes = 0;
            if (typeIndexes != null) {

                //iterate over the existing custom indexes-we want to keep them in the same order
                //in order not to change the position attribute in the typeDataIndex
                for (TypeDataIndex index : originalTypeData._indexes) {
                    if (!index.isCustomIndex())
                        continue;

                    indexes.add(index);
                    indexPosition = index.getPos() + 1;
                    numOfCustomIndexes++;
                }

                for (Entry<String, SpaceIndex> entry : typeIndexes.entrySet()) {
                    // Protect from indexes created by properties.
                    if (nonCustomIndexes.containsKey(entry.getKey()))
                        continue;
                    final SpaceIndex spaceIndex = entry.getValue();
                    if (!spaceIndex.getIndexType().isIndexed())
                        continue;


                    TypeDataIndex currIndex = _indexTable.get(spaceIndex.getName());

                    boolean newIndex = currIndex == null;
                    if (!newIndex)
                        continue;
                    if (((ISpaceIndex) spaceIndex).isUnique()) {
                        ISpaceIndex u = (ISpaceIndex) spaceIndex;
                        if (_isLocalCache)
                            u.setUnique(false); //no uniques in local cache-view  just the id
                        else
                            throw new UnsupportedOperationException("dynamic unique indices not supported type=" + serverTypeDesc.getTypeName() + " index=" + u.getName());
                    }
                    if (((ISpaceIndex) spaceIndex).isCompoundIndex()) {//new compound index
                        newCompounds.add((CompoundIndex) spaceIndex);
                        continue;
                    }
                    numNewIndexes++;
                    if (!((ISpaceIndex) spaceIndex).isMultiValuePerEntryIndex())
                        currIndex = _typeDataFactory.createCustomTypeDataIndex(_typeDataFactory.getCcheManager(), getInternalIndex(spaceIndex),
                                indexPosition++, _lastIndexCreationNumber + numNewIndexes /* index creation #*/, FifoGroupsIndexTypes.NONE);
                    else
                        currIndex = _typeDataFactory.createCustomMultiValuePerEntryTypeDataIndex(_typeDataFactory.getCcheManager(), getInternalIndex(spaceIndex),
                                indexPosition++, _lastIndexCreationNumber + numNewIndexes /* index creation #*/, ((ISpaceIndex) spaceIndex).getMultiValueIndexType());


                    indexes.add(currIndex);
                    _indexTable.put(spaceIndex.getName(), currIndex);

                    numOfCustomIndexes++;
                    addIndexRelatedProperties(serverTypeDesc, spaceIndex, currIndex, true, indexesRelatedFixedProperties, indexesRelatedDynamicProperties);

                }
                //handle new compound indices
                for (CompoundIndex ci : newCompounds) {
                    numNewIndexes++;
                    TypeDataIndex compound = TypeData.buildCompoundIndex(ci, _indexTable, serverTypeDesc, indexPosition++, ISpaceIndex.FifoGroupsIndexTypes.NONE, _lastIndexCreationNumber + numNewIndexes /*indexCreationNumber*/, _typeDataFactory);
                    indexes.add(compound);
                    anyNonFGCompoundIndex = true;
                    compoundIndexDefinitions.add(ci);
                    compoundIndexes.add(compound);
                    numOfCustomIndexes++;
                    if (compound.isExtendedIndex())
                        numOfExtendedIndexes++;
                    addCompundIndexRelatedProperties(serverTypeDesc, compound, true, indexesRelatedFixedProperties, indexesRelatedDynamicProperties);

                }

            }
            _hasIndexes = _indexTable.size() != 0;
            _numOfBackRefs = _indexTable.size() + numOfExtendedIndexes + 1;
            _numOfCustomIndexes = numOfCustomIndexes;
            TypeDataIndex<Object>[] temp_i = new TypeDataIndex[indexes.size()];
            if (indexes.size() > 0)
                temp_i = indexes.toArray(temp_i);
            _indexes = temp_i;

            if (numNewIndexes > 0)
                _lastIndexPendingCreationNumber = _lastIndexCreationNumber + numNewIndexes;
            else //no new index at all
                _lastIndexPendingCreationNumber = 0;

            _compoundIndexes = compoundIndexes;
            _compoundIndexDefinitions = compoundIndexDefinitions;
            _anyNonFGCompoundIndex = anyNonFGCompoundIndex;


        } else if (reason == TypeDataRecreationReasons.DYNAMIC_INDEX_CREATION_COMPLETION) {//index completion
            _indexTable = originalTypeData._indexTable;
            _idPropertyIndex = originalTypeData._idPropertyIndex;
            _uidUsageCounter = originalTypeData._uidUsageCounter;
            _indexes = originalTypeData._indexes;
            _hasIndexes = originalTypeData._hasIndexes;
            _numOfBackRefs = originalTypeData._numOfBackRefs;
            _numOfCustomIndexes = originalTypeData._numOfCustomIndexes;


            if (originalTypeData._lastIndexPendingCreationNumber > 0)
                _lastIndexCreationNumber = originalTypeData._lastIndexPendingCreationNumber;
            else
                _lastIndexCreationNumber = originalTypeData._lastIndexCreationNumber;

            _lastIndexPendingCreationNumber = 0;
            _compoundIndexDefinitions = originalTypeData._compoundIndexDefinitions;
            _compoundIndexes = originalTypeData._compoundIndexes;
            _anyNonFGCompoundIndex = originalTypeData._anyNonFGCompoundIndex;

        } else
            throw new UnsupportedOperationException(); //never happensa


        _readTakeTemplates = originalTypeData._readTakeTemplates;
        _notifyTemplates = originalTypeData._notifyTemplates;

        _readTakeExtendedTemplates = originalTypeData._readTakeExtendedTemplates;
        _notifyExtendedTemplates = originalTypeData._notifyExtendedTemplates;

        _readTakeUidTemplates = originalTypeData._readTakeUidTemplates;
        _notifyUidTemplates = originalTypeData._notifyUidTemplates;
        //fifo group indexes not supported for dynamic indexes
        _fifoGroupingIndex = originalTypeData._fifoGroupingIndex;
        _allowFifoIndexScans = _fifoSupport | (_fifoGroupingIndex != null);
        _uniqueIndexes = originalTypeData._uniqueIndexes;
        _indexesRelatedFixedProperties = indexesRelatedFixedProperties;
        _indexesRelatedDynamicProperties = indexesRelatedDynamicProperties;
        _sequenceNumberIndex = sequenceNumberIndex;
        queryExtensionIndexManagers = originalTypeData.queryExtensionIndexManagers;
    }


    public Set<QueryExtensionIndexManagerWrapper> getForeignQueriesHandlers() {
        return queryExtensionIndexManagers;
    }

    public boolean isUsingQueryExtensionIndexManager(){
        return queryExtensionIndexManagers != null && !queryExtensionIndexManagers.isEmpty();
    }

    public static TypeDataIndex buildCompoundIndex(CompoundIndex definition, Map<String, TypeDataIndex<?>> indexTable, IServerTypeDesc serverTypeDesc, int indexPosition, ISpaceIndex.FifoGroupsIndexTypes fifoGroupsIndexType, int indexCreationNumber, TypeDataFactory typeDataFactory) {
        if (indexTable.containsKey(definition))
            throw new IllegalArgumentException("duplicate index name=" + definition.getName());

        if (!serverTypeDesc.getTypeDesc().supportsDynamicProperties()) {//verify that the segments point at properties
            for (int i = 0; i < definition.getNumSegments(); i++) {
                String path = definition.getCompoundIndexSegments()[i].getName();
                String root = path.indexOf(".") != -1 ? path.substring(0, path.indexOf(".")) : path;
                if (serverTypeDesc.getTypeDesc().getFixedPropertyPosition(root) == TypeDesc.NO_SUCH_PROPERTY)
                    throw new IllegalArgumentException("path has to be based on a property " + root);

                //in case of a fixed propert verify storage type
                if (serverTypeDesc.getTypeDesc().getFixedProperty(root).getStorageType() != StorageType.OBJECT &&
                        !(serverTypeDesc.getTypeDesc().getFixedProperty(root).getStorageType() == StorageType.DEFAULT &&
                                serverTypeDesc.getTypeDesc().getStorageType() == StorageType.OBJECT))
                    throw new IllegalArgumentException("invalid compound index segment storage type " + root);
            }
        }

        //create segments & verify that no duplicate segments exist
        CompoundIndexSegmentTypeData[] segments = new CompoundIndexSegmentTypeData[definition.getNumSegments()];
        HashSet<String> names = new HashSet<String>();

        for (int i = 0; i < segments.length; i++) {
            if (names.contains(definition.getCompoundIndexSegments()[i].getName()))
                throw new UnsupportedOperationException("duplicate segment name=" + definition.getCompoundIndexSegments()[i].getName() + " index=" + definition.getName());

            segments[i] = new CompoundIndexSegmentTypeData(definition.getCompoundIndexSegments()[i], indexTable, serverTypeDesc);
        }
        //if need to- this can also be a compound F_G index
        if (indexCreationNumber == 0 && fifoGroupsIndexType != ISpaceIndex.FifoGroupsIndexTypes.COMPOUND && segments.length == 2 && segments[1].getOriginatingIndex() != null &&
                segments[1].getOriginatingIndex().isFifoGroupsMainIndex() && segments[0].getOriginatingIndex() != null && segments[0].getOriginatingIndex().getFifoGroupsIndexType() == ISpaceIndex.FifoGroupsIndexTypes.AUXILIARY && !segments[0].getOriginatingIndex().getIndexDefinition().isMultiValuePerEntryIndex()) {
            //the compound index will also be F-G compound index
            fifoGroupsIndexType = ISpaceIndex.FifoGroupsIndexTypes.COMPOUND;
        }

        //using the segments create the compound index
        TypeDataIndex newIndex = typeDataFactory.createCompoundCustomTypeDataIndex(typeDataFactory.getCcheManager(), definition, segments, indexCreationNumber, indexPosition, fifoGroupsIndexType);

        if (fifoGroupsIndexType == ISpaceIndex.FifoGroupsIndexTypes.COMPOUND) {
            segments[0].getOriginatingIndex().setCompoundFifoGroupsIndexForSegment(newIndex);

        }

        //add the new index to the indices map
        indexTable.put(definition.getName(), newIndex);

        return newIndex;
    }


    private void addCompundIndexRelatedProperties(IServerTypeDesc serverTypeDesc, TypeDataIndex c, boolean fromAddIndex, boolean[] indexesRelatedFixedProperties, HashSet<String> indexesRelatedDynamicProperties) {
        CompoundIndexSegmentTypeData[] segs = ((CompoundCustomTypeDataIndex) c).getCompoundIndexSegments();
        for (CompoundIndexSegmentTypeData segData : segs) {
            ISpaceCompoundIndexSegment seg = segData.getDefinitionSegment();
            int pos = seg.getName().indexOf(".");
            String root = (pos == -1 ? seg.getName() : seg.getName().substring(0, pos));
            if (serverTypeDesc.getTypeDesc().getFixedPropertyPosition(root) != -1)
                indexesRelatedFixedProperties[(serverTypeDesc.getTypeDesc().getFixedPropertyPosition(root))] = true;
            else if (serverTypeDesc.getTypeDesc().supportsDynamicProperties())
                indexesRelatedDynamicProperties.add(root);
            else if (!fromAddIndex)
                throw new RuntimeException("invalid root segment property in compoundindex root=" + root);
        }

    }

    private void addIndexRelatedProperties(IServerTypeDesc serverTypeDesc, SpaceIndex indexDefinition, TypeDataIndex index, boolean fromAddIndex, boolean[] indexesRelatedFixedProperties, HashSet<String> indexesRelatedDynamicProperties) {
        int pos = indexDefinition.getName().indexOf(".");
        String root = (pos == -1 ? indexDefinition.getName() : indexDefinition.getName().substring(0, pos));
        if (index.isMultiValuePerEntryIndex()) {
            pos = root.indexOf("[");
            root = (pos == -1 ? root : root.substring(0, pos));
        }

        if (serverTypeDesc.getTypeDesc().getFixedPropertyPosition(root) != -1)
            indexesRelatedFixedProperties[(serverTypeDesc.getTypeDesc().getFixedPropertyPosition(root))] = true;
        else if (serverTypeDesc.getTypeDesc().supportsDynamicProperties())
            indexesRelatedDynamicProperties.add(root);
        else if (!fromAddIndex)
            throw new RuntimeException("invalid root property in index root=" + root);
    }


    private static <T> IStoredList<T> createStoreList() {
        return StoredListFactory.createConcurrentSegmentedList(true /*supportsFifoPerSegment*/,1 /*numOfSegments*/,true /* padded*/);
    }

    /**
     * @return Returns the entries.
     */
    public IStoredList<IEntryCacheInfo> getEntries() {
        return _entries;
    }

    public IScanListIterator<IEntryCacheInfo> scanTypeEntries()
    {
        if (getEntries() == null)
            return null;
        return new ScanSingleListIterator<IEntryCacheInfo>(getEntries(),false/*fifo*/);
    }

    public PropertyInfo getProperty(int propertyID) {
        return _properties[propertyID];
    }

    public IStoredList<TemplateCacheInfo> getTemplates(MatchTarget matchTarget) {
        return matchTarget == MatchTarget.NOTIFY ? _notifyTemplates : _readTakeTemplates;
    }

    public IStoredList<TemplateCacheInfo> getNotifyTemplates() {
        return _notifyTemplates;
    }

    public IStoredList<TemplateCacheInfo> getReadTakeTemplates() {
        return _readTakeTemplates;
    }

    public boolean hasIndexes() {
        return _hasIndexes;
    }

    public String getClassName() {
        return _className;
    }

    public String generateUid(Object id) {
        return SpaceUidFactory.generateUid(_classUidPrefix, id.toString());
    }

    public CacheManager getCacheManager() {
        return _cacheManager;
    }

    /**
     * @return field ordinal by name
     */
    public int getFieldOrdinal(String fieldName) {
        Integer res = _columnsOrdinalsByName.get(fieldName);
        return res == null ? -1 : res.intValue();
    }

    public TypeDataIndex<?> getIndex(String indexName) {
        return _indexTable.get(indexName);
    }

    public TypeDataIndex[] getIndexes() {
        return _indexes;
    }

    public int numberOfBackRefs() {
        return _numOfBackRefs;
    }

    public IStoredList<TemplateCacheInfo> getExtendedTemplates(MatchTarget matchTarget) {
        return matchTarget == MatchTarget.NOTIFY ? _notifyExtendedTemplates : _readTakeExtendedTemplates;
    }

    public IStoredList<TemplateCacheInfo> getNotifyExtendedTemplates() {
        return _notifyExtendedTemplates;
    }

    public IStoredList<TemplateCacheInfo> getReadTakeExtendedTemplates() {
        return _readTakeExtendedTemplates;
    }

    public IStoredList<TemplateCacheInfo> getUidTemplates(MatchTarget matchTarget, String uid) {
        return matchTarget == MatchTarget.NOTIFY ? _notifyUidTemplates.get(uid) : _readTakeUidTemplates.get(uid);
    }

    public ConcurrentHashMap<String, IStoredList<TemplateCacheInfo>> getNotifyUidTemplates() {
        return _notifyUidTemplates;
    }

    public ConcurrentHashMap<String, IStoredList<TemplateCacheInfo>> getReadTakeUidTemplates() {
        return _readTakeUidTemplates;
    }

    public List<TemplateInfo> fillTemplatesInfo(List<TemplateInfo> templates) {
        addTemplatesInfo(templates, _notifyTemplates);
        addTemplatesInfo(templates, _notifyExtendedTemplates);
        if (_hasIndexes) {
            addTemplatesInfo(templates, _indexes[0]._NNullTemplates);
            for (IStoredList<TemplateCacheInfo>[] lists : _indexes[0]._NTemplates.values()) {
                addTemplatesInfo(templates, lists[0]);
            }
        }

        for (IStoredList<TemplateCacheInfo> list : _notifyUidTemplates.values()) {
            addTemplatesInfo(templates, list);
        }
        return templates;
    }

    private void addTemplatesInfo(List<TemplateInfo> templates,
                                  IStoredList<TemplateCacheInfo> list) {
        for (IStoredListIterator<TemplateCacheInfo> pos = list.establishListScan(false); pos != null; pos = list.next(pos)) {
            TemplateCacheInfo template = pos.getSubject();
            if (template == null || template.m_TemplateHolder.isDeleted()) {
                continue;
            }
            TemplateInfo info = createTemplateInfo(template.m_TemplateHolder);
            templates.add(info);
        }
    }

    private TemplateInfo createTemplateInfo(ITemplateHolder template) {
        return new TemplateInfo(template.isFifoTemplate(),
                new Date(template.getExpirationTime()),
                template.getEntryData().getFixedPropertiesValues(),
                template.getUidToOperateBy(),
                template.getOperationModifiers());
    }

    public int getTotalNotifyTemplates() {
        return m_NumRegularNotifyTemplatesStored.get() + _numDurableNotifyTemplatesStored.get();
    }

    public int getM_NumRegularNotifyTemplatesStored() {
        return m_NumRegularNotifyTemplatesStored.get();
    }

    public int decM_NumRegularNotifyTemplatesStored() {
        return m_NumRegularNotifyTemplatesStored.decrementAndGet();
    }

    public int incM_NumRegularNotifyTemplatesStored() {
        return m_NumRegularNotifyTemplatesStored.incrementAndGet();
    }

    public int decNumDurableNotificationsStored() {
        return _numDurableNotifyTemplatesStored.decrementAndGet();
    }

    public int incNumDurableNotificationsStored() {
        return _numDurableNotifyTemplatesStored.incrementAndGet();
    }

    private IStoredList<IEntryCacheInfo> createEntriesStoredList(boolean fifoSupport) {
        if (fifoSupport) {
                if(_fifoGroupingIndex != null){
                    return StoredListFactory.createConcurrentSegmentedList(true /* supportsFifoPerSegment*/, 0 /*numOfSegments*/, true /*padded*/);
                } else {
                    return StoredListFactory.createConcurrentSegmentedList(true /* supportsFifoPerSegment*/, 1 /*numOfSegments*/, true /*padded*/);
                }
        }
        return StoredListFactory.createConcurrentSegmentedList(false /* supportsFifoPerSegment*/);
    }

    public boolean isFifoSupport() {
        return _fifoSupport;
    }

    public boolean isAllowFifoIndexScans() {
        return _allowFifoIndexScans;
    }

    public TypeDataIndex getIdField() {
        return _idPropertyIndex;
    }

    public LongCounter getUidUsageCounter() {
        return _uidUsageCounter;
    }

    public boolean disableIdIndexForEntries(TypeDataIndex index) {
        return (!_cacheManager.forceSpaceIdIndexIfEqual() && getIdField() == index && !index.isExtendedIndex() && !_isAutoGeneratedId);
    }

    public boolean isBlobStoreClass() {
        return _isBlobStoreClass;
    }


    List<TypeDataIndex> getUniqueIndexes() {
        return _uniqueIndexes;
    }

    int getNumUniqueIndexes() {
        return _uniqueIndexes == null ? 0 : _uniqueIndexes.size();
    }


    public boolean[] getIndexesRelatedFixedProperties() {
        return _indexesRelatedFixedProperties;
    }

    public HashSet<String> getIndexesRelatedDynamicProperties() {
        return _indexesRelatedDynamicProperties;
    }

    public boolean hasSequenceNumber() {
        return _sequenceNumberGenerator != null;
    }

    public SequenceNumberGenerator getSequenceNumberGenerator() {
        return _sequenceNumberGenerator;
    }

    public boolean hasSequenceNumberIndex() {
        return _sequenceNumberIndex != null;
    }

    public TypeDataIndex<?> getSequenceNumberIndex() {
        return _sequenceNumberIndex;
    }

    /**
     * given an entry set its fifo order fields, order is timestamp + counter, currently used only
     * in persistent space
     *
     * @param eh - entryholder
     */
    void setFifoOrderFieldsForEntry(IEntryHolder eh) {
        while (true) {
            eh.setOrder(_nextInFifoOrder.incrementAndGet());

            if (eh.getOrder() < 0) {//we wrapped around, make sure we wait at least 1 millisecond in order
                // for time to advance
                long starttine = SystemTime.timeMillis();
                do {
                    try {
                        Thread.sleep(1); //sleep for 1 milli
                    } catch (InterruptedException e) {
                    }
                }
                while (SystemTime.timeMillis() == starttine);

                synchronized (this) {//change the counter to zero if i am the first to do so
                    if (_nextInFifoOrder.get() < 0)
                        _nextInFifoOrder.set(0);
                }

                continue;
            }//if (eh.m_Order < 0)

            eh.setSCN(SystemTime.timeMillis());

            //protect against the chance that this thread was swapped out after
            //getting the count and before getting the time and though having
            // a large count while the global count wrapped to zero before the thread woke
            // with improper time. note that we minimize touching
            //the global count which is volatile
            if (_fifoCountRecheckFrom <= eh.getOrder()) {
                if (eh.getOrder() > _nextInFifoOrder.get())
                    continue;
            }
            return;
        } //while
    }


    /**
     * update the entry refs in  cache.
     */
    public void updateEntryReferences(CacheManager cacheManager, IEntryHolder eh, IEntryCacheInfo pEntry, IEntryData oldEntryData) {

        if (!hasIndexes())
            return;

        if (pEntry.getEntryHolder(cacheManager).hasShadow()) {
            updateEntryReferencesUnderXtn(cacheManager, pEntry, oldEntryData);
            return;

        }
        boolean insertedNewValues = false;
        int refpos = 1;
        // replace indexes only on a need-to basis
        ArrayList<IObjectInfo<IEntryCacheInfo>> deletedBackRefs = pEntry.getBackRefs();
        if (deletedBackRefs != null) {
            pEntry.setBackRefs(new ArrayList<IObjectInfo<IEntryCacheInfo>>(deletedBackRefs.size()));
            pEntry.getBackRefs().add(deletedBackRefs.get(0));
        }
        IEntryData entryData = pEntry.getEntryHolder(cacheManager).getEntryData();
        int numOfFieldsDone = 0;
        ArrayList<Object> originalUniques = null;
        try {
            for (TypeDataIndex<Object> index : _indexes) {
                if (index.disableIndexUsageForOperation(this, pEntry.getLatestIndexCreationNumber()))
                    continue;
                //indexed field, replace it  only if need to
                Object fieldValue = index.getIndexValue(entryData);
                Object oldFieldValue = index.getIndexValue(oldEntryData);
                if (index.isUniqueIndex() && getNumUniqueIndexes() > 1) {//we need to insert to all uniques first and afterwards to remove all
                    int originalpos = refpos;
                    refpos = index.updateIndexValue(this, pEntry.getEntryHolder(cacheManager), pEntry, oldFieldValue, fieldValue, deletedBackRefs, refpos, UpdateIndexModes.INSERT_NONEQUALS);
                    if (deletedBackRefs != null) {
                        if (deletedBackRefs.get(originalpos) != null) //non-equality value, need to revisit this unique
                        {
                            if (originalUniques == null)
                                originalUniques = new ArrayList<Object>(getNumUniqueIndexes() * 2);
                            originalUniques.add(index);
                            originalUniques.add(originalpos);
                        }
                    } else {
                        if (!TypeData.objectsEquality(oldFieldValue, fieldValue)) {
                            if (originalUniques == null)
                                originalUniques = new ArrayList<Object>(getNumUniqueIndexes() * 2);
                            originalUniques.add(index);
                            originalUniques.add(-1); //unused
                        }
                    }
                } else
                    refpos = index.updateIndexValue(this, pEntry.getEntryHolder(cacheManager), pEntry, oldFieldValue, fieldValue, deletedBackRefs, refpos, UpdateIndexModes.REPLACE_NONEQUALS);
                numOfFieldsDone++;
            }//for

            for (QueryExtensionIndexManagerWrapper queryExtensionIndexManager : getForeignQueriesHandlers()) {
                try {
                    queryExtensionIndexManager.replaceEntry(new SpaceServerEntryImpl(pEntry, cacheManager));
                } catch (Exception ex) {
                    throw new RuntimeException("Remove entry to foreign index failed", ex);
                }
            }
            insertedNewValues = true;
            //if unique indexes inserted first- we need to remove the original values
            if (originalUniques != null) {
                for (int pos = 0; pos < originalUniques.size(); pos++) {
                    TypeDataIndex index = (TypeDataIndex) originalUniques.get(pos++);
                    Object fieldValue = index.getIndexValue(entryData);
                    Object oldFieldValue = index.getIndexValue(oldEntryData);
                    refpos = (Integer) originalUniques.get(pos);
                    index.updateIndexValue(this, pEntry.getEntryHolder(cacheManager), pEntry, oldFieldValue, fieldValue, deletedBackRefs, refpos, UpdateIndexModes.REMOVE_NONEQUALS);
                }
            }
        } catch (RuntimeException ex) {
            try {
                restoreReferencesAfterUpdateFailure(cacheManager, eh, pEntry, oldEntryData, deletedBackRefs, numOfFieldsDone, insertedNewValues, originalUniques);
            } catch (Exception ex1) {
                if (_logger.isErrorEnabled())
                    _logger.error("failed to restore entry after update failure ", ex1);
            }
            throw ex;
        }

    }


    //index update failed- remove the new vaues, the old ones are still inserted
    private void restoreReferencesAfterUpdateFailure(CacheManager cacheManager, IEntryHolder eh, IEntryCacheInfo pEntry, IEntryData originalEntryData, ArrayList<IObjectInfo<IEntryCacheInfo>> originalBackRefs,
                                                     int numOfUpdatedFields, boolean insertedNewValues, ArrayList<Object> originalUniques) {
        if (insertedNewValues)
            return;   //aleady done inserting the  new ones- nothing to do

        ArrayList<IObjectInfo<IEntryCacheInfo>> failedBackRefs = pEntry.getBackRefs();
        IEntryData failedEntryData = pEntry.getEntryHolder(cacheManager).getEntryData();

        //update "back" up to the failed field
        int refpos = 1;
        // replace indexes only on a need-to basis
        if (originalBackRefs != null) {
            pEntry.setBackRefs(new ArrayList<IObjectInfo<IEntryCacheInfo>>(originalBackRefs.size()));
            pEntry.getBackRefs().add(failedBackRefs.get(0));
        }
        int numOfFieldsDone = 0;
        eh.updateEntryData(originalEntryData, originalEntryData.getExpirationTime());
        try {
            for (TypeDataIndex<Object> index : _indexes) {
                if (index.disableIndexUsageForOperation(this, pEntry.getLatestIndexCreationNumber()))
                    continue;
                if (numOfFieldsDone >= numOfUpdatedFields)
                    break;
                //indexed field, replace it  only if need to
                Object originalValue = index.getIndexValue(originalEntryData);
                Object failedValue = index.getIndexValue(failedEntryData);
                if (originalUniques != null && index.isUniqueIndex() && originalUniques.contains(index)) {//remove the new value, old one is still in
                    refpos = index.updateIndexValue(this, pEntry.getEntryHolder(cacheManager), pEntry, failedValue, originalValue, failedBackRefs, refpos, UpdateIndexModes.REMOVE_NONEQUALS);

                    if (originalBackRefs != null) {
                        pEntry.getBackRefs().add(originalBackRefs.get(pEntry.getBackRefs().size()));
                        if (index.numOfEntryIndexBackRefs(originalValue) == 2)
                            pEntry.getBackRefs().add(originalBackRefs.get(pEntry.getBackRefs().size()));
                    }
                } else
                    refpos = index.updateIndexValue(this, pEntry.getEntryHolder(cacheManager), pEntry, failedValue, originalValue, failedBackRefs, refpos, UpdateIndexModes.REPLACE_NONEQUALS);
                numOfFieldsDone++;
            }//for
        } catch (RuntimeException ex) {
            //restore failed. just an hopless entry
            throw ex;
        }

        //add the left refs
        if (originalBackRefs != null) {
            for (int i = pEntry.getBackRefs().size(); i < originalBackRefs.size(); i++)
                pEntry.getBackRefs().add(originalBackRefs.get(i));
        }
    }


//TBD- economy should be removed    

    /**
     * used only in economy-hash maps
     *
     *
     * index values are about to be replaced in entryData . protect against possible inconsistency
     * in data structures
     */
    public void prepareForUpdatingIndexValues(CacheManager cacheManager, IEntryCacheInfo pEntry, IEntryData newEntryData) {
        if (!hasIndexes() || !_typeDataFactory.useEconomyHashMap())
            return;

        IEntryData entryData = pEntry.getEntryHolder(cacheManager).getEntryData();
        for (TypeDataIndex<Object> index : _indexes) {
            if (!index.usedEconomyHashMap())
                continue;
            if (index.disableIndexUsageForOperation(this, pEntry.getLatestIndexCreationNumber()))
                continue;
            //indexed field, replace it  only if need to
            Object fieldValue = index.getIndexValue(entryData);
            Object newFieldValue = index.getIndexValue(newEntryData);
            if (!objectsEquality(fieldValue, newFieldValue))
                index.prepareForReplacingEntryIndexedField(fieldValue);
        }
    }

    /**
     * update the entry refs in  cache when there is an updating xtn, we keep the master entry and
     * shadow entry as one object pointing at each other so we cant remove the original index
     * references
     *
     * @param pEntry       TODO
     * @param oldEntryData TODO
     */
    public void updateEntryReferencesUnderXtn(CacheManager cacheManager, IEntryCacheInfo pEntry, IEntryData oldEntryData) {
        RuntimeException ex_thrown = null;
        try {
            ShadowEntryHolder shadowEh = pEntry.getEntryHolder(cacheManager).getShadow();
            shadowEh.resetNumOfIndexesUpdated();
            shadowEh.incrementNumOfUpdates();
            //there is an update but the entry is previously updated under this xtn
            IEntryData shadowEntryData = shadowEh.getEntryData();
            boolean double_update = !isEqualProperties(shadowEntryData, oldEntryData);
            if (hasIndexes()) {
                int refpos = 1;
                ArrayList<IObjectInfo<IEntryCacheInfo>> deletedBackRefs = pEntry.getBackRefs();
                if (pEntry.indexesBackRefsKept()) {
                    pEntry.setBackRefs(new ArrayList<IObjectInfo<IEntryCacheInfo>>(numberOfBackRefs()));
                    pEntry.getBackRefs().add(deletedBackRefs.get(0));
                }
                IEntryData newEntryData = pEntry.getEntryHolder(cacheManager).getEntryData();

                // replace indexes only on a need-to basis
                for (TypeDataIndex index : _indexes) {
                    if (index.disableIndexUsageForOperation(this, pEntry.getLatestIndexCreationNumber()))
                        continue;
                    //indexed field, replace it  only if need to
                    Object newFieldValue = index.getIndexValue(newEntryData);
                    Object oldFieldValue = index.getIndexValue(oldEntryData);
                    int cursize = pEntry.indexesBackRefsKept() ? pEntry.getBackRefs().size() : -1;
                    try {
                        refpos = index.updateIndexValueUndexXtn(this, pEntry.getEntryHolder(cacheManager), pEntry, oldFieldValue, newFieldValue, deletedBackRefs, refpos, double_update);
                        shadowEh.incrementNumOfIndexesUpdated();
                    } catch (RuntimeException ex) {
                        if (ex_thrown == null)
                            ex_thrown = ex;
                        if (pEntry.indexesBackRefsKept()) {
                            //since settling references under xtn is complicated we set them as dummy refs and xtn rollback will
                            //erase the change
                            if (pEntry.getBackRefs().size() == cursize)
                                pEntry.getBackRefs().add(TypeDataIndex._DummyOI);
                            refpos++;
                            if (index.isExtendedIndex()) {
                                if (newFieldValue != null && pEntry.getBackRefs().size() == cursize + 1)
                                    pEntry.getBackRefs().add(TypeDataIndex._DummyOI);
                                if (oldFieldValue != null)
                                    refpos++;
                            }
                        }
                    }
                }//for
                for (QueryExtensionIndexManagerWrapper queryExtensionIndexManager : getForeignQueriesHandlers()) {
                    if (double_update) //double under same xtn
                        queryExtensionIndexManager.replaceEntry(new SpaceServerEntryImpl(pEntry, cacheManager));
                    else
                        queryExtensionIndexManager.insertEntry(new SpaceServerEntryImpl(pEntry, cacheManager), true /*fromTransactionalUpdate*/);
                }
            }//if (pType.isM_AnyIndexes())
        } finally {
            if (ex_thrown != null) {
                //restore values before update op
                XtnEntry xtnEntry = pEntry.getEntryHolder(cacheManager).getWriteLockOwner();
                cacheManager.consolidateWithShadowEntry(this, pEntry, true /* restoreOriginalValus*/, true /*onError*/);
                pEntry.getEntryHolder(cacheManager).setWriteLockOwnerAndOperation(xtnEntry, SpaceOperations.READ);

                //remove indication of rewritten entry (if exists)
                xtnEntry.getXtnData().removeRewrittenEntryIndication(pEntry.getUID());
                throw ex_thrown;
            }
        }
    }

    private boolean isEqualProperties(IEntryData shadow, IEntryData old) {
        if (!(shadow instanceof BinaryEntryData)) {
            return shadow.getFixedPropertiesValues() == old.getFixedPropertiesValues();
        } else {
            return ((BinaryEntryData) shadow).getSerializedFields() == ((BinaryEntryData) old).getSerializedFields();
        }
    }


    /**
     * perform objects equality , cater for null and also first use == which is not always true for
     * various class  "equals"
     */
    static boolean objectsEquality(Object a, Object b) {
        if (a == null)
            return b == null;
        if (a == b)
            return true;
        return (a.equals(b));
    }

    public int[] createIndexBackreferenceArray(IEntryCacheInfo pEntry, IEntryData masterEntryData) {
        if (!_hasIndexes || !pEntry.indexesBackRefsKept())
            return null;

        //create back-refs pos array
        int[] backrefIndexPos = new int[masterEntryData.getNumOfFixedProperties() + _numOfCustomIndexes];
        int refpos = 1;
        for (TypeDataIndex index : _indexes) {
            if (index.disableIndexUsageForOperation(this, pEntry.getLatestIndexCreationNumber()/*inputIndexCreationNumber*/))
                continue;
            int position = index.getPos();
            backrefIndexPos[position] = refpos;
            refpos += index.numOfEntryIndexBackRefs(index.getIndexValue(masterEntryData));
        }

        return backrefIndexPos;
    }

    public boolean isTypeDataReplaced() {
        return _typeDataReplaced;
    }

    public void setTypeDataReplaced() {
        _typeDataReplaced = true;
    }

    public boolean supportsDynamicIndexing() {
        return _supportsDynamicIndexing;
    }

    void typeLock() {
        _typeLock.writeLock().lock();
    }

    void typeUnLock() {
        _typeLock.writeLock().unlock();
    }

    public int getLastIndexCreationNumber() {
        return _lastIndexCreationNumber;
    }

    int getLastIndexPendingCreationNumber() {
        return _lastIndexPendingCreationNumber;
    }


    public boolean hasInitialIndexes() {
        return _hasInitialIndexes;
    }

    public boolean anyInitialExtendedIndex() {
        return _anyInitialExtendedIndex;
    }

    private static ISpaceIndex getInternalIndex(SpaceIndex index) {
        if (index instanceof ISpaceIndex)
            return (ISpaceIndex) index;

        throw new UnsupportedOperationException("Space indexes must implement internal ISpaceIndex interface.");
    }

    public TypeDataFactory getTypeDataFactory() {
        return _typeDataFactory;
    }

    public boolean hasFifoGroupingIndex() {
        return _fifoGroupingIndex != null;
    }

    public TypeDataIndex<Object> getFifoGroupingIndex() {
        return _fifoGroupingIndex;
    }

    public boolean anyCompoundIndex() {
        return !_compoundIndexes.isEmpty();
    }

    public List<TypeDataIndex> getCompoundIndexes() {
        return _compoundIndexes;
    }

    //TBD- do we really need it ?
    public boolean anyNonFGCompoundIndex() {
        return _anyNonFGCompoundIndex;
    }

    public boolean isAutoGeneratedId() {
        return _isAutoGeneratedId;
    }
}
