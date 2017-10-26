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

//
package com.j_spaces.core.cache.blobStore.sadapter;

import com.gigaspaces.internal.backport.java.util.concurrent.FastConcurrentSkipListMap;
import com.gigaspaces.internal.server.storage.EntryDataType;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.FifoEntriesComparator;
import com.j_spaces.core.cache.TypeData;

import java.util.Iterator;

/**
 * off-heap initial-load of fifo/f-g classes we need to sort by fifo order and before that throw out
 * any non-related-to-index property
 *
 * @author yechiel
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class BlobStoreFifoInitialLoader implements Iterator<IEntryHolder> {
    //TBD- use CM property with getter
    private final boolean _splittedEntries = true;

    private boolean _finished;
    private final FastConcurrentSkipListMap<IEntryHolder, IEntryHolder> _orderedList;
    private Iterator<IEntryHolder> _iter;
    private boolean _firstCalled;


    public BlobStoreFifoInitialLoader() {
        _orderedList = new FastConcurrentSkipListMap<IEntryHolder, IEntryHolder>(new FifoEntriesComparator());
    }

    public void add(IEntryHolder eh, TypeData typeData) {
        if (!eh.isBlobStoreEntry() || (typeData.getFifoGroupingIndex() == null && !typeData.isFifoSupport()))
            throw new UnsupportedOperationException();
        if (eh.getEntryData().getEntryDataType() != EntryDataType.FLAT)
            throw new UnsupportedOperationException();

        //throw out non index-related fields
        if (!_splittedEntries) {
            for (int i = 0; i < eh.getEntryData().getFixedPropertiesValues().length; i++) {
                if (!typeData.getIndexesRelatedFixedProperties()[i])
                    eh.getEntryData().getFixedPropertiesValues()[i] = null;
            }
            if (!typeData.getIndexesRelatedDynamicProperties().isEmpty()) {
                if (!eh.getServerTypeDesc().getTypeDesc().supportsDynamicProperties())
                    throw new UnsupportedOperationException();
                Iterator<String> i = eh.getEntryData().getDynamicProperties().keySet().iterator();
                while (i.hasNext()) {
                    String p = i.next();
                    if (!typeData.getIndexesRelatedDynamicProperties().contains(p))
                        i.remove();
                }
            }
        }
        _orderedList.put(eh, eh);
    }

    @Override
    public IEntryHolder next() {
        return _iter.next();
    }

    @Override
    public boolean hasNext() {
        if (!_firstCalled) {
            _firstCalled = true;
            _iter = _orderedList.keySet().iterator();
        }
        _finished = _iter.hasNext();
        return _finished;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty() {
        return _orderedList.isEmpty();
    }

}
