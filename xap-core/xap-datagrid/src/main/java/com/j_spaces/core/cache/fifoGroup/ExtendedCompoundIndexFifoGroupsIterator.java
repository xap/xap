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

import com.j_spaces.core.cache.SimpleCompoundIndexValueHolder;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.kernel.IStoredList;

import java.util.Map;
import java.util.NavigableMap;

@com.gigaspaces.api.InternalApi
public class ExtendedCompoundIndexFifoGroupsIterator<V>
        extends ExtendedIndexFifoGroupsIterator<V> {
    private final Object _nonGroupValue;

    public ExtendedCompoundIndexFifoGroupsIterator(NavigableMap mapToScan, TypeDataIndex idx, Object nonGroupValue) {
        super(mapToScan, idx);
        _nonGroupValue = nonGroupValue;
    }

    @Override
    protected Object getActualGroupValue(Map.Entry<Object, IStoredList<V>> mapEntry) {
        return ((SimpleCompoundIndexValueHolder) mapEntry.getKey()).getValueBySegment(2);
    }
}
