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

package com.j_spaces.core.cache.fifoGroup;

import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.query.IQueryIndexScanner;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.metadata.TypeDataFactory;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metadata.index.CompoundIndex;
import com.gigaspaces.metadata.index.ISpaceIndex;
import com.gigaspaces.metadata.index.ISpaceIndex.FifoGroupsIndexTypes;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexFactory;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.client.ClientUIDHandler;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.list.IObjectsList;
import com.j_spaces.kernel.list.IScanListIterator;
import com.j_spaces.kernel.list.MultiStoredList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 9.0
 */
 /*
 * caching methods for fifo group handling
 */

@com.gigaspaces.api.InternalApi
public class FifoGroupCacheImpl {
    private final CacheManager _cacheManager;
    private final FifoGroupsCacheHandler _fgCacheHandeler;

    private static String Compound_Fifo_Groups_Index_Name_Suffix = "+COMPOUND_FIFO_GROUPS";

    private final AtomicInteger _numOfTemplates = new AtomicInteger();

    private static int MIN_NUM_ENTRIES_TO_CONSIDER_COMPOUND_INDEX_USAGE = 30;

    private static int MIN_NUM_ENTRIES_TO_CONSIDER_ALL_VALUES_SCAN_USAGE = 10;

    private static int FIFO_GROUPS_RATIO = 10;
    private final Logger _logger;


    public FifoGroupCacheImpl(CacheManager cacheManager, Logger logger) {
        _cacheManager = cacheManager;
        _fgCacheHandeler = new FifoGroupsCacheHandler(_cacheManager);
        _logger = logger;
    }


    private IScanListIterator<IEntryCacheInfo> getScannableEntriesMinIndexFifoGroup(Context context, TypeData typeData, ITemplateHolder template, IStoredList<IEntryCacheInfo> res, boolean extendedMatch) {
        if (!res.isMultiObjectCollection())
            return res.getObjectFromHead();
        TypeDataIndex fgIndex = typeData.getFifoGroupingIndex();
        if (res == typeData.getEntries())
            return new FifoGroupsScanAllValuesIterator(fgIndex.getFifoGroupsIndexExtention().getFifoGroupLists());

        if (extendedMatch && res instanceof IFifoGroupIterator)
            return (IScanListIterator<IEntryCacheInfo>) res;
        TypeDataIndex indexUsed = context.getIndexUsedInFifoGroupScan(res);
        if (context.getIndexUsedInFifoGroupScan(res) == typeData.getFifoGroupingIndex()) {
            if (template.getExtendedMatchCodes() == null && template.getCustomQuery() == null && typeData.anyCompoundIndex() && res.size() >= MIN_NUM_ENTRIES_TO_CONSIDER_COMPOUND_INDEX_USAGE) {//can we use a compound index of f-g value + other index ?
                for (TypeDataIndex other : typeData.getCompoundIndexes()) {
                    if (other.getFifoGroupsIndexExtention() == null)
                        continue;  //non f-g related
                    if (other.getSegmentsOriginatingIndexes()[0].getIndexValueForTemplate(template.getEntryData()) != null) {
                        IStoredList<IStoredList> list = other.getIndexEntries(other.getCompoundIndexValueForTemplate(template.getEntryData()));
                        return list != null ? new FifoGroupsScanByFGIndexIterator(list) : null;
                    }
                }
            }
            return new FifoGroupsScanByFGIndexIterator(res);
        }
        //non f-g index used for simple scan. can we used the compound f-g index ?
        return createScanIterOnNonFGIndex(res, template, indexUsed, typeData);

    }


    public static IScanListIterator<IEntryCacheInfo> createScanIterOnNonFGIndex(IStoredList<IEntryCacheInfo> entries, ITemplateHolder template, TypeDataIndex usedIndexForRes, TypeData typeData) {
        int num_entries = entries.size();
        TypeDataIndex fgIndex = typeData.getFifoGroupingIndex();
        if (usedIndexForRes.getCompoundFifoGroupsIndexForSegment() != null && num_entries >= MIN_NUM_ENTRIES_TO_CONSIDER_COMPOUND_INDEX_USAGE) {

            Object indexValue = usedIndexForRes.getIndexValueForTemplate(template.getEntryData());
            IStoredList<IStoredList> list = null;
            if (template.getExtendedMatchCodes() == null && template.getCustomQuery() == null && fgIndex.getIndexValueForTemplate(template.getEntryData()) != null) {//both index and f-g value non null- use compound index
                list = usedIndexForRes.getCompoundFifoGroupsIndexForSegment().getIndexEntries(usedIndexForRes.getCompoundFifoGroupsIndexForSegment().getCompoundIndexValueForTemplate(template.getEntryData()));
                return list != null ? new FifoGroupsScanByFGIndexIterator(list) : null;
            }
            list = usedIndexForRes.getCompoundFifoGroupsIndexForSegment().getFifoGroupsIndexExtention().getFifoGroupLists(indexValue);
            return list != null ? new FifoGroupsScanAllValuesByCompoundIndexIterator(list) : null;
        }
        return (num_entries > MIN_NUM_ENTRIES_TO_CONSIDER_ALL_VALUES_SCAN_USAGE && template.isTakeOperation() && fgIndex.getFifoGroupsIndexExtention().getNumGroups() < num_entries / FIFO_GROUPS_RATIO) ? new FifoGroupsScanAllValuesIterator(fgIndex.getFifoGroupsIndexExtention().getFifoGroupLists())
                : new FifoGroupsScanByGeneralIndexIterator(typeData.getFifoGroupingIndex(), entries);

    }

    public IScanListIterator<IEntryCacheInfo> getScannableEntriesMinIndex(Context context, TypeData typeData, int numOfFields, ITemplateHolder template) {
        context.resetFifoGroupIndexUsedInFifoGroupScan();
        IStoredList<IEntryCacheInfo> res = getEntriesMinIndex(context, typeData, numOfFields, template);
        return res != null ? getScannableEntriesMinIndexFifoGroup(context, typeData, template, res, false) : null;
    }


    private IStoredList<IEntryCacheInfo> getEntriesMinIndex(Context context, TypeData typeData, int numOfFields, ITemplateHolder template) {

        if (!typeData.hasFifoGroupingIndex())
            return null;

        context.setAnyFifoGroupIndex();
        return _cacheManager.getEntriesMinIndex(context, typeData, numOfFields, template);
    }


    public IScanListIterator<IEntryCacheInfo> getScannableEntriesMinIndexExtended(Context context, TypeData entryType, int numOfFields, ITemplateHolder template) {
        context.setFifoGroupQueryContainsOrCondition(false);
        context.resetFifoGroupIndexUsedInFifoGroupScan();
        Object chosen = getEntriesMinIndexExtended(context, entryType, numOfFields, template);
        if (chosen == null)
            return null;
        if (chosen instanceof ExtendedIndexFifoGroupsIterator)
            return (ExtendedIndexFifoGroupsIterator) chosen;
        if (chosen instanceof MultiStoredList)
            return new FifoGroupsMultiList((MultiStoredList) chosen, entryType, context.getFfoGroupIndexResultsUsedInFifoGroupScan(), template);
        return (chosen != null) ?
                getScannableEntriesMinIndexFifoGroup(context, entryType, template, (IStoredList<IEntryCacheInfo>) chosen, true) : null;

    }

    private Object getEntriesMinIndexExtended(Context context, TypeData entryType, int numOfFields, ITemplateHolder template) {
        //FIFO template ? consider only fifo-supported classses
        if (template.isFifoSearch())
            throw new UnsupportedOperationException(" fifo template cannot support fifo groups");
        if (!entryType.hasFifoGroupingIndex())
            return null;
        context.setAnyFifoGroupIndex();

        return getEntriesMinIndexExtended_impl(context, entryType, numOfFields, template);
    }

    private Object getEntriesMinIndexExtended_impl(Context context, TypeData entryType, int numOfFields, ITemplateHolder template) {

        // get index for scan
        IStoredList<IEntryCacheInfo> resultSL = null;
        IScanListIterator<IEntryCacheInfo> resultOIS = null;
        final boolean ignoreOrderedIndexes = entryType.getEntries().size() < _cacheManager.getMinExtendedIndexActivationSize();

        int latestIndexToConsider = entryType.getLastIndexCreationNumber();

        //the following belong to fifo-groups
        Object fifoGroupMainIndexResult = null;
        IStoredList<IEntryCacheInfo> fgMainIndexEqResult = null;
        IStoredList<IEntryCacheInfo> fgOtherIndexEqResult = null;
        TypeDataIndex fgOtherIndexEq = null;

        final TypeDataIndex[] indexes = entryType.getIndexes();
        for (TypeDataIndex<Object> index : indexes) {
            int pos = index.getPos();
            if (pos >= numOfFields)
                break;

            if (index.getIndexCreationNumber() > latestIndexToConsider)
                continue;   //uncompleted index

            final short extendedMatchCode = template.getExtendedMatchCodes()[pos];
            final boolean fifoGroupIndex = template.isFifoGroupPoll() && index == entryType.getFifoGroupingIndex();

            // ignore index that cant be basic- extended matching is not fifo
            if (!fifoGroupIndex && !TemplateMatchCodes.supportFifoOrder(extendedMatchCode))
                continue;

            final Object templateValue = index.getIndexValueForTemplate(template.getEntryData());
            IStoredList<IEntryCacheInfo> entriesVector = null;
            //what kind of match we need on the template ?
            switch (extendedMatchCode) {
                case TemplateMatchCodes.NOT_NULL:    //any not null will do
                case TemplateMatchCodes.REGEX:        //unusable index for this
                    continue;
                case TemplateMatchCodes.IS_NULL:
                    entriesVector = index.getNullEntries();
                    if (resultSL == null || resultSL.size() > entriesVector.size())
                        resultSL = entriesVector;
                    break; //evaluate

                case TemplateMatchCodes.NE:
                    if (templateValue == null)
                        continue; //TBD

                    entriesVector = index.getNonUniqueEntriesStore().get(templateValue);
                    if (entriesVector != null && entriesVector.size() == entryType.getEntries().size())
                        return null;
                    continue;  //nothing to do here

                case TemplateMatchCodes.EQ:
                    if (templateValue == null)
                        continue;//TBD

                    if (entryType.disableIdIndexForEntries(index))
                        entriesVector = _cacheManager.getPEntryByUid(entryType.generateUid(templateValue));
                    else
                        entriesVector = index.getIndexEntries(templateValue);
                    if (entriesVector == null)
                        return null; //no values
                    if (fifoGroupIndex) {
                        fgMainIndexEqResult = entriesVector;
                    } else {
                        if (fgOtherIndexEqResult == null && index.getCompoundFifoGroupsIndexForSegment() != null) {
                            fgOtherIndexEqResult = entriesVector;
                            fgOtherIndexEq = index;
                        }
                    }
                    if (resultSL == null || resultSL.size() > entriesVector.size()) {
                        resultSL = entriesVector;
                        if (fifoGroupIndex)
                            fifoGroupMainIndexResult = resultSL;
                        context.setFifoGroupIndexUsedInFifoGroupScan(entriesVector, index);
                    }
                    break; //evaluate

                case TemplateMatchCodes.LT:
                case TemplateMatchCodes.LE:
                case TemplateMatchCodes.GE:
                case TemplateMatchCodes.GT: //for GT we must clip first value if eq to limit
                    if (templateValue == null)
                        continue; //TBD
                    if (ignoreOrderedIndexes)
                        continue; //# of entries did not reach limit

                    if (index.getExtendedIndexForScanning() == null)
                        continue; //ordered index not defined
                    if (resultOIS == null || (fifoGroupIndex)) {
                        final Object rangeValue = template.getRangeValue(pos);
                        final boolean isInclusive = rangeValue == null ? false : template.getRangeInclusion(pos);
                        //range limit passed- query with "up to" range
                        //NOTE! - currently we support only range "up-to" inclusive
                        resultOIS = index.getExtendedFifoGroupsIndexForScanning().establishScan(templateValue,
                                extendedMatchCode, rangeValue, isInclusive);

                        if (resultOIS == null)
                            return null;  //no values
                        fifoGroupMainIndexResult = resultOIS;

                    }
                    break; //evaluate
            }//switch
        } // for

        final ICustomQuery customQuery = template.getCustomQuery();
        if (customQuery != null && customQuery.getCustomIndexes() != null) {
            for (IQueryIndexScanner index : customQuery.getCustomIndexes()) {
                // Get entries in space that match the indexed value in the query (a.k.a potential match list):
                IObjectsList result = index.getIndexedEntriesByType(context, entryType, template, latestIndexToConsider);

                if (result == IQueryIndexScanner.RESULT_IGNORE_INDEX)
                    continue;

                if (result == IQueryIndexScanner.RESULT_NO_MATCH)
                    return null;

                //check the return type - can be extended iterator
                if (result != null && result.isIterator()) {
                    resultOIS = (IScanListIterator<IEntryCacheInfo>) result;
                    // Log index usage
                    if (_logger.isLoggable(Level.FINEST))
                        _logger.log(Level.FINEST, "EXTENDED-INDEX '" + index.getIndexName() + "' has been used for type [" +
                                entryType.getClassName() + "]");
                    continue;
                }

                IStoredList<IEntryCacheInfo> entriesVector = (IStoredList<IEntryCacheInfo>) result;
                // Log index usage
                if (_logger.isLoggable(Level.FINEST))
                    _logger.log(Level.FINEST, "BASIC-INDEX '" + index.getIndexName() + "' has been used for type [" +
                            entryType.getClassName() + "]");

                if (entriesVector == null)
                    return null; //no values matching the index value

                // check if the minimal index needs to be updated
                if (resultSL == null || resultSL.size() > entriesVector.size())
                    resultSL = entriesVector;
            }
        }

        if (fifoGroupMainIndexResult != null && (fifoGroupMainIndexResult == resultOIS || fifoGroupMainIndexResult == resultSL)) {
            if (fifoGroupMainIndexResult == fgMainIndexEqResult && fgOtherIndexEqResult != null && fgMainIndexEqResult.size() >= MIN_NUM_ENTRIES_TO_CONSIDER_ALL_VALUES_SCAN_USAGE) {
                //use the compound index if possible
                TypeDataIndex compound = fgOtherIndexEq.getCompoundFifoGroupsIndexForSegment();
                IStoredList<IEntryCacheInfo> res = compound.getIndexEntries(compound.getCompoundIndexValueForTemplate(template.getEntryData()));
                context.setFifoGroupIndexUsedInFifoGroupScan(res, entryType.getFifoGroupingIndex());
                return res;
            }

            return fifoGroupMainIndexResult;
        }

        if (resultSL == null) {
            // the entry type is indexed, but the template has a null value for
            // every field which is indexed (or template is null), so we must return all the entries.
            if (resultOIS == null)
                return entryType.getEntries();

            return resultOIS;
        }

        if (resultOIS == null || resultSL.size() < entryType.getEntries().size()) {

            if (resultSL != null && resultSL == fgOtherIndexEqResult && fgMainIndexEqResult != null && resultSL.size() >= MIN_NUM_ENTRIES_TO_CONSIDER_ALL_VALUES_SCAN_USAGE) {
                TypeDataIndex compound = fgOtherIndexEq.getCompoundFifoGroupsIndexForSegment();
                IStoredList<IEntryCacheInfo> res = compound.getIndexEntries(compound.getCompoundIndexValueForTemplate(template.getEntryData()));
                context.setFifoGroupIndexUsedInFifoGroupScan(res, entryType.getFifoGroupingIndex());
                return res;
            }

            return resultSL;
        }

        return resultOIS;
    }


    //we have an index value (according to the entry rendered) - get the f-g index list by this value
    public IScanListIterator<IEntryCacheInfo> getScannableEntriesByIndexValue(Context context, ITemplateHolder template, IEntryHolder entry, IServerTypeDesc templateTypeDesc) {
        TypeData typeData = _cacheManager.getTypeData(entry.getServerTypeDesc());

        if (!typeData.hasFifoGroupingIndex())
            return null;
        Object groupToFind = typeData.getFifoGroupingIndex().getIndexValue(entry.getEntryData());
        if (groupToFind == null)
            return null;

        //can we use compound index ?
        if (template.getExtendedMatchCodes() == null && template.getCustomQuery() == null && typeData.anyCompoundIndex()) {//can we use a compound index of f-g value + other index ?
            for (TypeDataIndex other : typeData.getCompoundIndexes()) {
                if (other.getFifoGroupsIndexExtention() == null)
                    continue;  //non f-g related
                if (other.getSegmentsOriginatingIndexes()[0].getIndexValueForTemplate(template.getEntryData()) != null) {
                    IStoredList<IStoredList> list = other.getIndexEntries(other.getIndexValue(entry.getEntryData()));
                    return list != null ? new FifoGroupsScanByFGIndexIterator(list) : null;
                }
            }
        }


        IStoredList<IEntryCacheInfo> chosen = typeData.getFifoGroupingIndex().getIndexEntries(groupToFind);
        if (chosen == null)
            return null;
        return new FifoGroupsScanByFGIndexIterator(chosen);
    }

    //is this fg value + match unused ? if so set the entry as user
    //used to prevent scanning thru different indices problem
    //or the problem of updated entry pop-up
    public boolean testAndSetFGCacheForEntry(Context context, IEntryHolder entry, ITemplateHolder template, Object val, boolean testOnly) {
        return _fgCacheHandeler.testAndSetFGCacheForEntry(context, entry, template, val, testOnly);
    }


    public void handleFifoGroupsCacheOnXtnEnd(Context context, XtnEntry xtnEntry) {
        Map<String, Object> emap = xtnEntry.getXtnData().getFifoGroupsEntries();
        if (emap == null || emap.isEmpty())
            return;
        Iterator<Map.Entry<String, Object>> iter = emap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Object> el = iter.next();
            _fgCacheHandeler.removeEntryFromFGCache(context, el.getKey(), el.getValue(), xtnEntry);
        }
    }


    //when a data type is created- create fifo group compound indexes if specified
    public static List<TypeDataIndex> createFifoGroupCompoundIndexes(PropertyInfo[] _properties, Map<String, SpaceIndex> typeIndexes, ConcurrentHashMap<String, TypeDataIndex<?>> _indexTable,
                                                                     TypeDataIndex<Object> _fifoGroupIndex, ISpaceIndex fifoGroupingPropertyDef, TypeDataFactory typeDataFactory,
                                                                     int indexPosition, String fifoGroupingName, Set<String> fifoGroupingIndexes, List<CompoundIndex> compoundIndexDefinitions, IServerTypeDesc serverTypeDesc) {
        List<TypeDataIndex> res = new ArrayList<TypeDataIndex>();
        HashSet<String> propertiesIndexes = new HashSet<String>();

        RuntimeException exp = null;

        try {

            for (int i = 0; i < _properties.length; i++) {
                PropertyInfo property = _properties[i];
                SpaceIndex index = typeIndexes.get(property.getName());
                if (index != null && index.getIndexType().isIndexed()) {
                    FifoGroupsIndexTypes indexFifoGroupingType = TypeData.getIndexFifoGroupingType(index, fifoGroupingName, fifoGroupingIndexes);

                    propertiesIndexes.add(index.getName());
                    if (!((ISpaceIndex) index).isMultiValuePerEntryIndex() && indexFifoGroupingType == FifoGroupsIndexTypes.AUXILIARY) {
                        boolean create = true;
                        String compound_index_name = property.getName() + Compound_Fifo_Groups_Index_Name_Suffix;
                        CompoundIndex def = (CompoundIndex) SpaceIndexFactory.createCompoundIndex(new String[]{property.getName(), _fifoGroupIndex.getIndexDefinition().getName()}, SpaceIndexType.EQUAL, compound_index_name, false /*unique*/);

                        //do we already have an equivalent one ?
                        for (CompoundIndex c : compoundIndexDefinitions) {
                            if (c.isEquivalent(def)) {
                                create = false;
                                break;
                            }
                        }
                        if (!create)
                            continue;

                        //create compound index
                        TypeDataIndex newIndex = TypeData.buildCompoundIndex(def, _indexTable, serverTypeDesc, indexPosition++, ISpaceIndex.FifoGroupsIndexTypes.COMPOUND, 0 /*indexCreationNumber*/, typeDataFactory);
                        _indexTable.put(compound_index_name, newIndex);
                        res.add(newIndex);
                    }
                }
            }
            if (typeIndexes != null) {
                for (Entry<String, SpaceIndex> entry : typeIndexes.entrySet()) {
                    // Protect from indexes created by properties.
                    if (propertiesIndexes.contains(entry.getKey()))
                        continue;
                    final SpaceIndex spaceIndex = entry.getValue();
                    if (!spaceIndex.getIndexType().isIndexed())
                        continue;
                    FifoGroupsIndexTypes indexFifoGroupingType = TypeData.getIndexFifoGroupingType(spaceIndex, fifoGroupingName, fifoGroupingIndexes);
                    if (!((ISpaceIndex) spaceIndex).isMultiValuePerEntryIndex() && indexFifoGroupingType == FifoGroupsIndexTypes.AUXILIARY) {
                        boolean create = true;
                        String compound_index_name = spaceIndex.getName() + Compound_Fifo_Groups_Index_Name_Suffix;
                        CompoundIndex def = (CompoundIndex) SpaceIndexFactory.createCompoundIndex(new String[]{spaceIndex.getName(), _fifoGroupIndex.getIndexDefinition().getName()}, SpaceIndexType.EQUAL, compound_index_name, false /*unique*/);

                        //do we already have an equivalent one ?
                        for (CompoundIndex c : compoundIndexDefinitions) {
                            if (c.isEquivalent(def)) {
                                create = false;
                                break;
                            }
                        }
                        if (!create)
                            continue;

                        //create compound index
                        TypeDataIndex newIndex = TypeData.buildCompoundIndex(def, _indexTable, serverTypeDesc, indexPosition++, ISpaceIndex.FifoGroupsIndexTypes.COMPOUND, 0 /*indexCreationNumber*/, typeDataFactory);
                        _indexTable.put(compound_index_name, newIndex);
                        res.add(newIndex);
                    }
                }
            }
        } catch (Exception ex) {
            CacheManager.getCacheLogger().log(Level.SEVERE, "createFifoGroupCompoundIndexes failed with exception " + ex.fillInStackTrace() + "]");
            exp = new RuntimeException(ex);
        } finally {
            if (exp != null)
                throw exp;
            return res;
        }

    }

    public void incrementNumOfTemplates() {
        _numOfTemplates.incrementAndGet();
    }

    public void decrementNumOfTemplates() {
        _numOfTemplates.decrementAndGet();
    }

    public int getNumOfTemplates() {
        return _numOfTemplates.get();
    }

    public Object aquireIndexLock(Object obj) {
        return _fgCacheHandeler.aquireIndexLock(obj);
    }

    public void releaseIndexLock(Object lockObject) {
        _fgCacheHandeler.releaseIndexLock(lockObject);
    }


}
