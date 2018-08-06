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
 * extentions for fifo-groups main index
 *
 * @author yechielf
 */
@com.gigaspaces.api.InternalApi
public class FifoGroupsMainIndexExtention<K> implements IFifoGroupsIndexExtention<K> {
    private final TypeDataIndex<K> _index;
    //a flat view of f-g values + lists
    private final IStoredList<Object> _fifoGroupValues;
    private final ConcurrentMap<Object, IObjectInfo<IEntryCacheInfo>> _fifoGroupsBackrefs;
    private final FifoGroupCacheImpl _fifoGroupsCacheImpl;

    public FifoGroupsMainIndexExtention(CacheManager cacheManager, TypeDataIndex<K> index) {
        _index = index;
        _fifoGroupValues = StoredListFactory.createConcurrentSegmentedList(true /*supportFifo*/);
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

    public void addToValuesList(K groupValue, IStoredList list) {
        if (groupValue == null)
            return;
        IObjectInfo fgoi = _fifoGroupValues.add(new PlainFGListHolder(groupValue, list));
        _fifoGroupsBackrefs.put(list.isMultiObjectCollection() ? list : new FifoGroupsBackRefsSingleEntryHolder(list, groupValue), fgoi);
    }

    public void removeFromValuesList(K groupValue, IStoredList list) {
        if (groupValue == null)
            return;
        IObjectInfo fgoi = _fifoGroupsBackrefs.remove(list.isMultiObjectCollection() ? list : new FifoGroupsBackRefsSingleEntryHolder(list, groupValue));
        _fifoGroupValues.remove(fgoi);
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
        return _fifoGroupValues;
    }

    public IStoredList getFifoGroupLists(Object otherIndexValue) {
        throw new UnsupportedOperationException();
    }

    public int getNumGroups() {
        return _fifoGroupValues.size();
    }


    public static class PlainFGListHolder
            implements IFifoGroupsListHolder {
        private final Object _mainGroupValue;
        private final IStoredList _list;

        public PlainFGListHolder(Object key, IStoredList list) {
            _mainGroupValue = key;
            _list = list;
        }

        public Object getMainGroupValue() {
            return _mainGroupValue;
        }

        public Object getOtherIndexValue() {
            throw new UnsupportedOperationException();
        }

        public IStoredList getList() {
            return _list;
        }
    }

}
