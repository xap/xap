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

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ShadowEntryHolder;
import com.j_spaces.core.cache.TypeDataIndex.UpdateIndexModes;
import com.j_spaces.kernel.IObjectInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 8.0
 */
/* common methods for collection/array/ indexing*/

public abstract class AbstractMultiValueIndexHandler<K> {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE);

    final TypeDataIndex<K> _typeDataIndex;

    public AbstractMultiValueIndexHandler(TypeDataIndex<K> typeDataIndex) {
        _typeDataIndex = typeDataIndex;


    }

    public void insertEntryIndexedField(IEntryCacheInfo pEntry, K fieldValue, TypeData pType, ArrayList<IObjectInfo<IEntryCacheInfo>> insertBackRefs) {
        if (fieldValue == null) {
            _typeDataIndex.insertEntryIndexedField_impl(pEntry, fieldValue, pType, insertBackRefs);
            return;
        }

        //create a backrefs list
        int size = multiValueSize(fieldValue);

        MultiValueIndexBackref mvbref = pEntry.indexesBackRefsKept() ? new MultiValueIndexBackref(numOfMultiValueBackrefs(size)) : null;
        HashSet<K> redundantValsFilter = new HashSet<K>(size);
        Iterator<K> iter = multiValueIterator(fieldValue);
        //iterate over the values and index them
        //NOTE- we assume that 2 iterators over an unchanged multi-value return the
        //elements in the same order

        int numIndexesInserted = 0;
        while (iter.hasNext()) {
            K val = iter.next();
            if (val == null)
                continue;
            if (redundantValsFilter.add(val)) {
                try {
                    _typeDataIndex.insertEntryIndexedField_impl(pEntry, val, pType, mvbref != null ? mvbref.getBackrefs() : null);
                    numIndexesInserted++;
                } catch (RuntimeException ex) {
                    try {
                        removePartiallyInsertedCollectionAfterInsertFailure(pEntry, fieldValue, mvbref, numIndexesInserted);
                    } catch (Exception ex1) {
                    }
                    throw ex;
                }
            }
        }

        if (pEntry.indexesBackRefsKept())
            addMultiValueBackref(insertBackRefs, mvbref);
    }

    private void removePartiallyInsertedCollectionAfterInsertFailure(IEntryCacheInfo pEntry, K fieldValue, MultiValueIndexBackref mvbref, int numIndexesInserted) {
        if (numIndexesInserted == 0)
            return;   //nothing to do

        int numRemoved = 0;
        Iterator<K> iter = multiValueIterator(fieldValue);
        HashSet<K> redundantValsFilter = new HashSet<K>(multiValueSize(fieldValue));
        int mvRefPos = 0;
        while (iter.hasNext()) {
            if (numRemoved >= numIndexesInserted)
                break;
            K val = iter.next();
            if (val == null)
                continue;

            if (redundantValsFilter.add(val))
                mvRefPos = _typeDataIndex.removeEntryIndexedField_impl(pEntry.getEntryHolder(_typeDataIndex.getCacheManager()), pEntry.indexesBackRefsKept() ? mvbref.getBackrefs() : null,
                        val, mvRefPos, true, pEntry);
            numRemoved++;
        }
    }


    public int removeEntryIndexedField(IEntryHolder eh, ArrayList<IObjectInfo<IEntryCacheInfo>> deletedBackRefs,
                                       K fieldValue, int refpos, boolean removeIndexedValue, IEntryCacheInfo pEntry) {
        if (!removeIndexedValue || fieldValue == null)
            return _typeDataIndex.removeEntryIndexedField_impl(eh, deletedBackRefs,
                    fieldValue, refpos, removeIndexedValue, pEntry);

        //go over all the values and remove them
        //get the  backrefs list
        MultiValueIndexBackref mvbref = deletedBackRefs != null ? (MultiValueIndexBackref) deletedBackRefs.get(refpos) : null;

        //iterate over the  values and index them

        Iterator<K> iter = multiValueIterator(fieldValue);
        HashSet<K> redundantValsFilter = new HashSet<K>(multiValueSize(fieldValue));

        int mvRefPos = 0;
        while (iter.hasNext()) {
            K val = iter.next();
            if (val == null)
                continue;

            if (redundantValsFilter.add(val))
                mvRefPos = _typeDataIndex.removeEntryIndexedField_impl(eh, mvbref != null ? mvbref.getBackrefs() : null,
                        val, mvRefPos, removeIndexedValue, pEntry);
        }
        if (deletedBackRefs != null) {
            deletedBackRefs.set(refpos++, null);
            if (_typeDataIndex.isExtendedIndex())
                deletedBackRefs.set(refpos++, null);
        }

        return refpos;
    }

    /*
     * update a multi-value index, return the pos in the mainbackrefs list
     */
    public int updateMultiValueIndex(TypeData pType, IEntryHolder eh, IEntryCacheInfo pEntry, K original, K updated, ArrayList<IObjectInfo<IEntryCacheInfo>> originalBackRefs, int refpos, UpdateIndexModes updateMode) {
        if ((updateMode == UpdateIndexModes.INSERT_NONEQUALS || updateMode == UpdateIndexModes.REPLACE_NONEQUALS) && (original == updated))
            return _typeDataIndex.moveValueBackrefsOnUpdate(pEntry, original, originalBackRefs, pEntry.getBackRefs(), refpos, true);

        if (original == null || updated == null) {
            if (updateMode == UpdateIndexModes.INSERT_NONEQUALS || updateMode == UpdateIndexModes.REPLACE_NONEQUALS)
                insertEntryIndexedField(pEntry, updated, pType, pEntry.getBackRefs());
            if (updateMode == UpdateIndexModes.REMOVE_NONEQUALS || updateMode == UpdateIndexModes.REPLACE_NONEQUALS)
                return _typeDataIndex.removeEntryIndexedField(eh, originalBackRefs, original, refpos, true, pEntry);
            else
                return refpos + _typeDataIndex.numOfEntryIndexBackRefs(original);
        }
        if (updateMode == UpdateIndexModes.REMOVE_NONEQUALS)
            return removeOriginalIndexValuesOnUpdateEnd(pType, eh, pEntry, original, updated, originalBackRefs, refpos);
        //we have 2 multi-values, perform update
        //NOTE- we assume that 2 iterators over an unchanged multi-values object return the
        //elements in the same order

        int size_u = multiValueSize(updated);
        //create a backrefs list
        MultiValueIndexBackref mvbref_u = pEntry.indexesBackRefsKept() ? new MultiValueIndexBackref(numOfMultiValueBackrefs(size_u)) : null;
        //get the  backrefs list
        MultiValueIndexBackref mvbref_o = pEntry.indexesBackRefsKept() ? (MultiValueIndexBackref) originalBackRefs.get(refpos) : null;

        //iterate over the original values

        HashMap<K, Integer> originalHM = buildMultiValueHashMapFromObject(original);
        HashSet<K> redundantValsFilter = new HashSet<K>(size_u);

        int numInserted = 0;
        //iterate over the new values and index if needed
        Iterator<K> iter_u = multiValueIterator(updated);
        try {
            while (iter_u.hasNext()) {
                K val = iter_u.next();
                if (val == null)
                    continue;
                if (!redundantValsFilter.add(val))
                    continue;
                Integer orgpos = originalHM.remove(val);
                if (orgpos == null) {//a new value
                    int pos = pEntry.indexesBackRefsKept() ? mvbref_u.getBackrefs().size() : -1;
                    _typeDataIndex.insertEntryIndexedField_impl(pEntry, val, pType, pEntry.indexesBackRefsKept() ? mvbref_u.getBackrefs() : null);
                    numInserted++;
                } else { //move the unchanged index ref
                    if (pEntry.indexesBackRefsKept())
                        _typeDataIndex.moveValueBackrefsOnUpdate(pEntry, val, mvbref_o.getBackrefs(), mvbref_u.getBackrefs(), orgpos, false);
                }
            }//while
        } catch (RuntimeException ex) {//revert the updates done so far. the old values are still inserted
            try {
                removePartiallyInsertedCollectionAfterUpdateFailure(pEntry, original, updated, mvbref_u, numInserted);
            } catch (Exception ex1) {
            }
            throw ex;
        }

        //are there values left in the old hash map ? if so iterate
        // and remove them
        if (updateMode == UpdateIndexModes.REPLACE_NONEQUALS && originalHM.size() > 0) {
            for (Map.Entry<K, Integer> entry : originalHM.entrySet()) {
                _typeDataIndex.removeEntryIndexedField_impl(eh, pEntry.indexesBackRefsKept() ? mvbref_o.getBackrefs() : null,
                        entry.getKey(), entry.getValue(), true, pEntry);
            }
        }

        //set the external backref
        if (pEntry.indexesBackRefsKept())
            addMultiValueBackref(pEntry.getBackRefs(), mvbref_u);
        return refpos + _typeDataIndex.numOfEntryIndexBackRefs(original);
    }


    private void removePartiallyInsertedCollectionAfterUpdateFailure(IEntryCacheInfo pEntry, K original, K updated, MultiValueIndexBackref mvbref_u, int numInserted) {
        if (numInserted == 0)
            return;   //nothing to do

        Iterator<K> iter_u = multiValueIterator(updated);
        HashSet<K> redundantValsFilter = new HashSet<K>(multiValueSize(updated));
        int mvRefPos = 0;
        HashSet<K> oldVals = original != null ? new HashSet<K>() : null;
        if (oldVals != null) {
            Iterator<K> iter_o = multiValueIterator(original);
            while (iter_o.hasNext()) {
                K val = iter_o.next();
                if (val != null)
                    oldVals.add(val);
            }
        }

        int numRemoved = 0;
        while (iter_u.hasNext()) {
            if (numRemoved >= numInserted)
                break;     //no more to remove
            K val = iter_u.next();
            if (val == null)
                continue;
            if (redundantValsFilter.add(val)) {
                if (oldVals == null || !oldVals.contains(val)) {
                    mvRefPos = _typeDataIndex.removeEntryIndexedField_impl(pEntry.getEntryHolder(_typeDataIndex.getCacheManager()), pEntry.indexesBackRefsKept() ? mvbref_u.getBackrefs() : null,
                            val, mvRefPos, true, pEntry);
                    numRemoved++;
                } else
                    mvRefPos += _typeDataIndex.numOfEntryIndexBackRefs(val);
            }
        }
    }

    //after update we remove the original unequal values
    private int removeOriginalIndexValuesOnUpdateEnd(TypeData pType, IEntryHolder eh, IEntryCacheInfo pEntry, K original, K updated, ArrayList<IObjectInfo<IEntryCacheInfo>> originalBackRefs, int refpos) {
        MultiValueIndexBackref mvbref_o = null;
        if (pEntry.indexesBackRefsKept()) {
            mvbref_o = (MultiValueIndexBackref) originalBackRefs.get(refpos);
            if (mvbref_o.getBackrefs().isEmpty())
                return refpos + _typeDataIndex.numOfEntryIndexBackRefs(original);   //nothing to do
        }

        Iterator<K> iter_o = multiValueIterator(original);
        HashSet<K> redundantValsFilter = new HashSet<K>(multiValueSize(original));
        int mvRefPos = 0;
        HashSet<K> updatedVals = updated != null ? new HashSet<K>() : null;
        if (updatedVals != null) {
            Iterator<K> iter_u = multiValueIterator(updated);
            while (iter_u.hasNext()) {
                K val = iter_u.next();
                if (val != null)
                    updatedVals.add(val);
            }
        }

        while (iter_o.hasNext()) {
            if (mvbref_o != null && mvRefPos >= mvbref_o.getBackrefs().size())
                break;     //no more to remove
            K val = iter_o.next();
            if (val == null)
                continue;
            if (redundantValsFilter.add(val)) {
                if (updatedVals == null || !updatedVals.contains(val))
                    mvRefPos = _typeDataIndex.removeEntryIndexedField_impl(pEntry.getEntryHolder(_typeDataIndex.getCacheManager()), pEntry.indexesBackRefsKept() ? mvbref_o.getBackrefs() : null,
                            val, mvRefPos, true, pEntry);
                else
                    mvRefPos += _typeDataIndex.numOfEntryIndexBackRefs(val);
            }
        }

        return refpos + _typeDataIndex.numOfEntryIndexBackRefs(original);   //nothing to do

    }

    /*
     * update undex xtn a multi-value index, return the pos in the mainbackrefs list
     */
    public int updateMultiValueIndexUndexXtn(TypeData pType, IEntryHolder eh, IEntryCacheInfo pEntry, K previous, K updated, ArrayList<IObjectInfo<IEntryCacheInfo>> previousBackRefs, int refpos) {
        RuntimeException ex_thrown = null;

        try {
            ShadowEntryHolder shadowEh = pEntry.getEntryHolder(_typeDataIndex.getCacheManager()).getShadow();
            IEntryData shadowEntryData = shadowEh.getEntryData();

            IObjectInfo<IEntryCacheInfo> backref_prev = pEntry.indexesBackRefsKept() ? previousBackRefs.get(refpos) : null;

            IObjectInfo<IEntryCacheInfo> backref_shadow = pEntry.indexesBackRefsKept() ? (previousBackRefs == shadowEh.getBackRefs() ? backref_prev : shadowEh.getBackRefs().get(shadowEh.getBackrefIndexPos()[_typeDataIndex.getPos()])) : null;

            //there is an update but the entry is previously updated under this xtn
            boolean doubleUpdate = shadowEh.getNumOfUpdates() > 1;

            Object shadowFieldValue = _typeDataIndex.getIndexValue(shadowEntryData);

            MultiValueIndexBackref mvbref_u = null;
            if (previous == updated) {
                return
                        _typeDataIndex.moveValueBackrefsOnUpdate(pEntry, previous, previousBackRefs, pEntry.getBackRefs(), refpos, false);
            }
            if (shadowFieldValue == null && updated == null) {
                _typeDataIndex.moveValueBackrefsOnUpdate(pEntry, shadowFieldValue, shadowEh.getBackRefs(), pEntry.getBackRefs(), pEntry.indexesBackRefsKept() ? shadowEh.getBackrefIndexPos()[_typeDataIndex.getPos()] : -1, false);

                return _typeDataIndex.removeEntryIndexedField(eh, previousBackRefs,
                        previous, refpos, true, pEntry);
            }
            int size_u;

            if (updated == null)
                insertEntryIndexedField(pEntry, updated, pType, pEntry.getBackRefs());
            else {
                size_u = multiValueSize(updated);
                //create a backrefs list
                if (pEntry.indexesBackRefsKept())
                    mvbref_u = new MultiValueIndexBackref(numOfMultiValueBackrefs(size_u));
            }
            MultiValueIndexBackref mvbref_prev = null;
            HashMap<K, Integer> previousHM = null;


            MultiValueIndexBackref mvbref_shadow = null;
            HashMap<K, Integer> shadowHM = null;
            if (shadowFieldValue != null) {
                mvbref_shadow = (MultiValueIndexBackref) backref_shadow;
                shadowHM = buildMultiValueHashMapFromObject(shadowFieldValue);
            }

            if (doubleUpdate && previous != null) {
                mvbref_prev = (MultiValueIndexBackref) backref_prev;
                previousHM = buildMultiValueHashMapFromObject(previous);
            }

            //now we can run over the values in the new multi-value
            //for each value we do
            // 1. if it appears in the shadow we take its ref
            // 2. if not and it appears in previous we take its ref
            // 3 else we index it
            // 3. remove it from the maps
            // after the scan, in case of double update we run on the
            // remanning previous map and delete its values

            if (updated != null) {
                Iterator<K> iter_u = multiValueIterator(updated);
                HashSet<K> redundantValsFilter = new HashSet<K>(multiValueSize(updated));
                while (iter_u.hasNext()) {
                    K val = iter_u.next();
                    if (val == null)
                        continue;
                    if (!redundantValsFilter.add(val))
                        continue;
                    Integer shadowpos = shadowHM != null ? shadowHM.remove(val) : null;
                    Integer previouspos = previousHM != null ? previousHM.remove(val) : null;
                    if (shadowpos == null && previouspos == null) {//a new value
                        int cursize = pEntry.indexesBackRefsKept() ? mvbref_u.getBackrefs().size() : -1;
                        try {
                            _typeDataIndex.insertEntryIndexedField_impl(pEntry, val, pType, mvbref_u != null ? mvbref_u.getBackrefs() : null);
                        } catch (RuntimeException ex) {
                            if (ex_thrown == null)
                                ex_thrown = ex;
                            //since settling references under xtn is complicated we set them as dummy refs and xtn rollback will
                            //erase the change
                            if (pEntry.indexesBackRefsKept()) {
                                if (mvbref_u.getBackrefs().size() == cursize)
                                    mvbref_u.getBackrefs().add(TypeDataIndex._DummyOI);
                                if (_typeDataIndex.isExtendedIndex()) {
                                    if (val != null && mvbref_u.getBackrefs().size() == cursize + 1)
                                        mvbref_u.getBackrefs().add(TypeDataIndex._DummyOI);
                                }
                            }
                        }

                    } else { //move the unchanged index ref
                        if (shadowpos != null)
                            _typeDataIndex.moveValueBackrefsOnUpdate(pEntry, val, mvbref_shadow != null ? mvbref_shadow.getBackrefs() : null, mvbref_u != null ? mvbref_u.getBackrefs() : null, shadowpos, false);
                        else
                            //take previous ref
                            _typeDataIndex.moveValueBackrefsOnUpdate(pEntry, val, mvbref_prev != null ? mvbref_prev.getBackrefs() : null, mvbref_u != null ? mvbref_u.getBackrefs() : null, previouspos, false);
                    }
                }//while
            }

            if (doubleUpdate) {
                if (previousHM != null && previousHM.size() > 0) {
                    for (Map.Entry<K, Integer> entry : previousHM.entrySet()) {
                        if (shadowHM == null || !shadowHM.containsKey(entry.getKey()))
                            _typeDataIndex.removeEntryIndexedField_impl(eh, mvbref_prev != null ? mvbref_prev.getBackrefs() : null,
                                    entry.getKey(), entry.getValue(), true, pEntry);
                    }
                }
                if (previous == null) {
                    _typeDataIndex.removeEntryIndexedField(eh, previousBackRefs,
                            previous, refpos, true, pEntry);
                }
            }


            //set the external backref
            if (updated != null && pEntry.indexesBackRefsKept())
                addMultiValueBackref(pEntry.getBackRefs(), mvbref_u);
            return refpos + _typeDataIndex.numOfEntryIndexBackRefs(previous);
        } finally {
            if (ex_thrown != null)
                throw ex_thrown;

        }
    }


    //build a hashmap, key = object in multi-value, value = pos in backref array
    private HashMap<K, Integer> buildMultiValueHashMapFromObject(Object mvo) {
        HashMap<K, Integer> hm = new HashMap<K, Integer>(multiValueSize(mvo));
        Iterator<K> iter_p = multiValueIterator(mvo);
        int orefpos = 0;
        while (iter_p.hasNext()) {
            K val = iter_p.next();
            if (val == null)
                continue;
            if (hm.get(val) != null)
                continue;
            hm.put(val, orefpos);

            orefpos += _typeDataIndex.numOfEntryIndexBackRefs(val);
        }
        return hm;
    }


    //on commit/update, if values are not equal, remove the unneeded ones
    //return the new ref-pos
    int consolidateMultiValueIndexOnXtnEnd(IEntryHolder eh, IEntryCacheInfo pEntry, K keptValue, K deletedVlaue, ArrayList<IObjectInfo<IEntryCacheInfo>> deletedBackRefs, int refpos, boolean onError) {
        if (deletedVlaue == keptValue) {
            return refpos + _typeDataIndex.numOfEntryIndexBackRefs(deletedVlaue);
        }
        if (deletedVlaue == null || keptValue == null) {
            return
                    removeEntryIndexedField(eh, deletedBackRefs,
                            deletedVlaue, refpos, true, pEntry);
        }

        //create a hashmap from the kept value multi-value
        if (multiValueSize(keptValue) == 0) {
            return
                    removeEntryIndexedField(eh, deletedBackRefs,
                            deletedVlaue, refpos, true, pEntry);
        }
        HashMap<K, Integer> keptHM = buildMultiValueHashMapFromObject(keptValue);

        //iterate over deleted values to get the diff, delete each one needed
        Iterator<K> iter_d = multiValueIterator(deletedVlaue);
        int orefpos = 0;
        MultiValueIndexBackref mvbref_del = pEntry.indexesBackRefsKept() ? (MultiValueIndexBackref) deletedBackRefs.get(refpos) : null;
        HashSet<K> redundantValsFilter = new HashSet<K>(multiValueSize(deletedVlaue));
        while (iter_d.hasNext()) {
            K val = iter_d.next();
            if (val == null)
                continue;
            if (!redundantValsFilter.add(val))
                continue;
            if (keptHM.remove(val) != null) {
                orefpos += _typeDataIndex.numOfEntryIndexBackRefs(val);
            } else {
                if (onError) {//ignore execptions
                    try {
                        orefpos =
                                _typeDataIndex.removeEntryIndexedField_impl(eh, mvbref_del != null ? mvbref_del.getBackrefs() : null,
                                        val, orefpos, true, pEntry);
                    } catch (Exception ex) {
                    }
                } else
                    orefpos =
                            _typeDataIndex.removeEntryIndexedField_impl(eh, mvbref_del != null ? mvbref_del.getBackrefs() : null,
                                    val, orefpos, true, pEntry);
            }
        }
        return refpos + _typeDataIndex.numOfEntryIndexBackRefs(deletedVlaue);

    }


    private void addMultiValueBackref(ArrayList<IObjectInfo<IEntryCacheInfo>> backRefs, MultiValueIndexBackref mvbref) {
        backRefs.add(mvbref);
        if (_typeDataIndex.isExtendedIndex())
            backRefs.add(mvbref);
    }

    private int numOfMultiValueBackrefs(int size) {
        return size * (_typeDataIndex.isExtendedIndex() ? 2 : 1);
    }

    abstract protected int multiValueSize(Object mvo);

    abstract protected Iterator<K> multiValueIterator(Object mvo);
}
