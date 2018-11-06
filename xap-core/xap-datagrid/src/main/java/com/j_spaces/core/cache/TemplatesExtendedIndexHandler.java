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

import com.gigaspaces.internal.backport.java.util.concurrent.FastConcurrentSkipListMap;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.StoredListFactory;

import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * handles data manipulation of space extended index
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 8.03
 */
@com.gigaspaces.api.InternalApi
public class TemplatesExtendedIndexHandler<K>
        implements IExtendedIndex<K, TemplateCacheInfo> {
    private final FastConcurrentSkipListMap<K, IStoredList<TemplateCacheInfo>> _orderedStore;
    private final TypeDataIndex _index;
    private final AtomicInteger _size;

    private static final boolean _FORCE_ORDERED_SCAN = true;

    public TemplatesExtendedIndexHandler(TypeDataIndex index) {
        _orderedStore = new FastConcurrentSkipListMap<K, IStoredList<TemplateCacheInfo>>();
        _index = index;
        _size = new AtomicInteger(0);
    }


    /**
     * insert a value for this key- if key already exist insert into the SL of same values
     *
     * @return the index backref
     */
    public IObjectInfo insertEntryIndexedField(TemplateCacheInfo pTemplate, K fieldValue, TypeData pType, boolean alreadyCloned) {
        IObjectInfo oi = null;
        IStoredList<TemplateCacheInfo> newSL = null;
        IStoredList<TemplateCacheInfo> currentSL = null;
        boolean first = true;
        _size.incrementAndGet();
        while (true) {
            if (first) {
                first = false;
                currentSL = _orderedStore.get(fieldValue);
            }
            if (currentSL == null) {
                if (!alreadyCloned && _index.considerValueClone()) {
                    fieldValue = (K) _index.cloneIndexValue(fieldValue, pTemplate.m_TemplateHolder);
                    alreadyCloned = true;
                }
                currentSL = _orderedStore.putIfAbsent(fieldValue, pTemplate);
                if (currentSL == null) {
                    oi = pTemplate;
                    break;
                }
            }
            if (currentSL.isMultiObjectCollection()) {//a real SL
                oi = currentSL.add(pTemplate);
                // may have been invalidated by PersistentGC
                if (oi == null) {
                    //help remove template for key only if currently mapped to given value
                    _orderedStore.remove(fieldValue, currentSL);
                    currentSL = null;
                    continue;
                } else
                    break; // OK - done.
            }
            //a single object is stored, create a SL and add it
            if (newSL == null)
                newSL = StoredListFactory.createConcurrentList(true);

            IObjectInfo otheroi = newSL.addUnlocked(currentSL.getObjectFromHead());
            IObjectInfo myoi = newSL.addUnlocked(pTemplate);

            if (!_orderedStore.replace(fieldValue, currentSL, newSL)) {
                newSL.removeUnlocked(otheroi);
                newSL.removeUnlocked(myoi);
                myoi = null;
                currentSL = null;
            } else {
                oi = myoi;
                break; // OK - done.
            }
        }

        return oi;

    }


    /**
     * remove entry indexed field from cache.
     */
    public void removeEntryIndexedField(IEntryHolder eh,
                                        Object fieldValue, TemplateCacheInfo pTemplate, IObjectInfo oi) {
        removeNonUniqueIndexedField(eh,
                fieldValue,
                pTemplate,
                oi);
        _size.decrementAndGet();

    }

    private void removeNonUniqueIndexedField(IEntryHolder eh, Object fieldValue,
                                             TemplateCacheInfo pTemplate, IObjectInfo oi) {
        while (true) {
            IStoredList<TemplateCacheInfo> templates = _orderedStore.get(fieldValue);
            if (templates == null)
                throw new RuntimeException("Template Class: " + eh.getClassName() +
                        " - Wrong hashCode() or equals() implementation of " +
                        fieldValue.getClass() + " class field.");

            if (templates.isMultiObjectCollection()) {//a true SL
                IObjectInfo myoi = oi;
                if (myoi == pTemplate) {
                    myoi = templates.getHead();
                    if (myoi.getSubject() != pTemplate)
                        throw new RuntimeException("Template Class: " + eh.getClassName() +
                                " - Single-template to multiple wrong OI ,  " +
                                fieldValue.getClass() + " class field.");
                }
                templates.remove(myoi);
                if (templates.invalidate())
                    _orderedStore.remove(fieldValue, templates);

                break;
            }
            if (templates != pTemplate) {
                throw new RuntimeException("Template Class: " + eh.getClassName() +
                        " - Single-template Wrong hashCode() or equals() implementation of " +
                        fieldValue.getClass() + " class field.");
            }
            //single value- remove me
            if (_orderedStore.remove(fieldValue, pTemplate))
                break;

        }
    }

    /**
     * establish a scan according to the relation given and startPos : the start-scan object ,  null
     * means scan all values. The relation is from com.j_spaces.client.TemplateMatchCodes: LT, LE,
     * GT, GE (other codes are not relevant) endPos- key up to (or null if no limit in  index)
     * endPosInclusive : is the endPos up to (or down to) and including ? returns an
     * ExtendedIndexIterator object which enables scanning the ordered index, Null if no relevant
     * elements to scan
     */
    public ExtendedIndexIterator<TemplateCacheInfo> establishScan(K startPos, short relation, K endPos, boolean endPosInclusive) {
        return
                establishScan(startPos, relation, endPos, endPosInclusive, false /* ordered*/);


    }

    /**
     * establish a scan according to the relation given and startPos : the start-scan object ,  null
     * means scan all values. The relation is from com.j_spaces.client.TemplateMatchCodes: LT, LE,
     * GT, GE (other codes are not relevant) endPos- key up to (or null if no limit in  index)
     * endPosInclusive : is the endPos up to (or down to) and including ? ordered - according to the
     * condition. GT, GE ==> ascending, LT, LE =====> descending. returns an IOrderedIndexScan
     * object which enables scanning the ordered index, Null if no relevant elements to scan
     */
    public ExtendedIndexIterator<TemplateCacheInfo> establishScan(K startPos, short relation, K endPos, boolean endPosInclusive, boolean ordered) {
        if (isEmpty())
            return null;
        ordered |= _FORCE_ORDERED_SCAN; //should we force ordered scan always ?

        return ordered ?
                establishScanOrdered(startPos, relation, endPos, endPosInclusive) :
                establishScanUnOrdered(startPos, relation, endPos, endPosInclusive);
    }

    private boolean isEmpty() {
        return (_size.get() == 0);
    }


    private ExtendedIndexIterator<TemplateCacheInfo> establishScanUnOrdered(K startPos, short relation, K endPos, boolean endPosInclusive) {
        boolean reversedScan = (relation == TemplateMatchCodes.LT || relation == TemplateMatchCodes.LE);
        Object originalStart = startPos;
        Object originalEnd = endPos;
        short originalStartCondition = reversedScan ? 0 : relation;
        short originalEndCondition = !reversedScan ? 0 : relation;

        K start;
        K end;
        boolean endInclusive;
        boolean startinclusive;
        if (reversedScan) {
            start = endPos;
            startinclusive = endPosInclusive;
            end = startPos;
            endInclusive = relation == TemplateMatchCodes.LE;
        } else {
            startinclusive = relation == TemplateMatchCodes.GE;
            start = startPos;
            end = endPos;
            endInclusive = endPosInclusive;

        }

        NavigableMap<K, IStoredList<TemplateCacheInfo>> baseMap = _orderedStore;
        NavigableMap<K, IStoredList<TemplateCacheInfo>> mapToScan;
        if (end == null)
            mapToScan = start != null ? baseMap.tailMap(start, startinclusive) : baseMap;
        else
            mapToScan = start != null ? baseMap.subMap(start, startinclusive, end, endInclusive) : baseMap.headMap(end, endInclusive);
        return new ExtendedIndexIterator<TemplateCacheInfo>(mapToScan, _index,originalStart,originalStartCondition, originalEnd,originalEndCondition);
    }

    private ExtendedIndexIterator<TemplateCacheInfo> establishScanOrdered(K startPos, short relation, K endPos, boolean endPosInclusive) {

        boolean reversedScan = (relation == TemplateMatchCodes.LT || relation == TemplateMatchCodes.LE);
        boolean startinclusive = (relation == TemplateMatchCodes.GE || relation == TemplateMatchCodes.LE);
        Object originalStart = startPos;
        Object originalEnd = endPos;
        short originalStartCondition = reversedScan ? 0 : relation;
        short originalEndCondition = !reversedScan ? 0 : relation;

        NavigableMap<K, IStoredList<TemplateCacheInfo>> baseMap = reversedScan ? _orderedStore.descendingMap() : _orderedStore;
        NavigableMap<K, IStoredList<TemplateCacheInfo>> mapToScan;
        if (endPos == null)
            mapToScan = startPos != null ? baseMap.tailMap(startPos, startinclusive) : baseMap;
        else
            mapToScan = startPos != null ? baseMap.subMap(startPos, startinclusive, endPos, endPosInclusive) : baseMap.headMap(endPos, endPosInclusive);
        return new ExtendedIndexIterator<TemplateCacheInfo>(mapToScan, _index,originalStart,originalStartCondition, originalEnd,originalEndCondition);
    }

    public FastConcurrentSkipListMap<K, IStoredList<TemplateCacheInfo>> getOrderedStore() {
        return _orderedStore;
    }
}
