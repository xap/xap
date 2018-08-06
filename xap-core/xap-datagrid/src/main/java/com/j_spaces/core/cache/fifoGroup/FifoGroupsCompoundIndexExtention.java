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

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.ICompoundIndexValueHolder;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.StoredListFactory;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * extentions for fifo-groups compound index
 *
 * @author yechielf
 */
@com.gigaspaces.api.InternalApi
public class FifoGroupsCompoundIndexExtention<K> implements IFifoGroupsIndexExtention<K> {
    private final TypeDataIndex<K> _index;
    //a 2-level tree  view of f-g values + lists
    private final ConcurrentMap<Object, IStoredList<Object>> _fifoGroupValues;
    private final ConcurrentMap<Object, IObjectInfo<IEntryCacheInfo>> _fifoGroupsBackrefs;
    private final FifoGroupCacheImpl _fifoGroupsCacheImpl;

    public FifoGroupsCompoundIndexExtention(CacheManager cacheManager, TypeDataIndex<K> index) {
        _index = index;
        _fifoGroupValues = new ConcurrentHashMap<Object, IStoredList<Object>>();
        ;
        _fifoGroupsBackrefs = new ConcurrentHashMap<Object, IObjectInfo<IEntryCacheInfo>>();
        _fifoGroupsCacheImpl = cacheManager.getFifoGroupCacheImpl();
    }


    public void insertEntryIndexedField(IEntryCacheInfo pEntry, K fieldValue, TypeData pType) {
        if (fieldValue == null) {
            _index.insertEntryIndexedField_impl(pEntry, fieldValue, pType, pEntry.getBackRefs());
            return;
        }
        Object lockObject = _fifoGroupsCacheImpl.aquireIndexLock(fieldValue);
        try {
            synchronized (lockObject) {
                _index.insertEntryIndexedField_impl(pEntry, fieldValue, pType, pEntry.getBackRefs());
            }
        } finally {
            _fifoGroupsCacheImpl.releaseIndexLock(lockObject);
        }

    }

    public void insertEntryIndexedField(IEntryCacheInfo pEntry, K fieldValue, TypeData pType, ArrayList<IObjectInfo<IEntryCacheInfo>> insertBackRefs) {
        if (fieldValue == null) {
            _index.insertEntryIndexedField_impl(pEntry, fieldValue, pType, insertBackRefs);
            return;
        }
        Object lockObject = _fifoGroupsCacheImpl.aquireIndexLock(fieldValue);
        try {
            synchronized (lockObject) {
                _index.insertEntryIndexedField_impl(pEntry, fieldValue, pType, insertBackRefs);
            }
        } finally {
            _fifoGroupsCacheImpl.releaseIndexLock(lockObject);
        }
    }

    public void addToValuesList(K rawValue, IStoredList list) {
        ICompoundIndexValueHolder value = (ICompoundIndexValueHolder) rawValue;
        Object nonMainIndexValue = value.getValueBySegment(1);
        Object valToKeep = new CompoundFGListHolder(value, list);
        IStoredList newMainList = null;
        IStoredList curMainList = _fifoGroupValues.get(value.getValueBySegment(1));
        IObjectInfo fgoi = null;
        IObjectInfo newfgoi = null;

        while (true) {

            if (curMainList == null) {
                if (newMainList == null) {
                    newMainList = StoredListFactory.createConcurrentList(true /*supportFifo*/);
                    newfgoi = newMainList.add(valToKeep);
                }
                fgoi = newfgoi;
                if ((curMainList = _fifoGroupValues.putIfAbsent(nonMainIndexValue, newMainList)) == null)
                    break;
            } else {
                fgoi = curMainList.add(valToKeep);
                if (fgoi == null)//invalidated by another thread
                {
                    _fifoGroupValues.remove(nonMainIndexValue, curMainList);
                    curMainList = null;
                } else
                    break;
            }
        }
        _fifoGroupsBackrefs.put(list.isMultiObjectCollection() ? list : new FifoGroupsBackRefsSingleEntryHolder(list, value), fgoi);
    }

    public void removeFromValuesList(K rawValue, IStoredList list) {
        ICompoundIndexValueHolder value = (ICompoundIndexValueHolder) rawValue;
        IStoredList curMainList = _fifoGroupValues.get(value.getValueBySegment(1));
        IObjectInfo fgoi = _fifoGroupsBackrefs.remove(list.isMultiObjectCollection() ? list : new FifoGroupsBackRefsSingleEntryHolder(list, rawValue));
        curMainList.remove(fgoi);
        if (curMainList.invalidate())
            _fifoGroupValues.remove(value.getValueBySegment(1), curMainList);
    }

    public int removeEntryIndexedField(IEntryHolder eh, ArrayList<IObjectInfo<IEntryCacheInfo>> deletedBackRefs,
                                       K fieldValue, int refpos, boolean removeIndexedValue, IEntryCacheInfo pEntry) {
        if (fieldValue == null) {
            return
                    _index.removeEntryIndexedField_impl(eh, deletedBackRefs,
                            fieldValue, refpos, removeIndexedValue, pEntry);

        }
        Object lockObject = _fifoGroupsCacheImpl.aquireIndexLock(fieldValue);
        try {
            synchronized (lockObject) {
                return
                        _index.removeEntryIndexedField_impl(eh, deletedBackRefs,
                                fieldValue, refpos, removeIndexedValue, pEntry);
            }
        } finally {
            _fifoGroupsCacheImpl.releaseIndexLock(lockObject);
        }

    }

    public IStoredList getFifoGroupLists() {
        throw new UnsupportedOperationException();
    }

    public IStoredList getFifoGroupLists(Object otherIndexValue) {
        return otherIndexValue != null ? _fifoGroupValues.get(otherIndexValue) : null;
    }


    public int getNumGroups() {
        //expensive
        throw new UnsupportedOperationException();
    }

    public static class CompoundFGListHolder
            implements IFifoGroupsListHolder {
        private final ICompoundIndexValueHolder _key; //other index value _group value
        private final IStoredList _list;

        public CompoundFGListHolder(ICompoundIndexValueHolder key, IStoredList list) {
            _key = key;
            _list = list;
        }

        public Object getOtherIndexValue() {
            return _key.getValueBySegment(1);
        }

        public Object getMainGroupValue() {
            return _key.getValueBySegment(2);
        }

        public IStoredList getList() {
            return _list;
        }
    }


}
