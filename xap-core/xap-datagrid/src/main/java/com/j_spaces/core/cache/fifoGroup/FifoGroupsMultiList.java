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

import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.list.IObjectsList;
import com.j_spaces.kernel.list.IScanListIterator;
import com.j_spaces.kernel.list.MultiStoredList;

import java.util.HashMap;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 9.0
 */
 /*
 * f-g version for multi-list.   created as a result of f-g execution of a 
 * template query which resultss multi list  
 * NOTE !!!- for single threaded use
 */

@com.gigaspaces.api.InternalApi
public class FifoGroupsMultiList<T>
        extends MultiStoredList<T>
        implements IFifoGroupIterator<T> {
    private final TypeData _typeData;
    private final HashMap<Object, TypeDataIndex> _fifoGroupIndexResultsUsedInFifoGroupScan;
    private final ITemplateHolder _template;

    public FifoGroupsMultiList(MultiStoredList<T> curMultiList, TypeData typeData, HashMap<Object, TypeDataIndex> fifoGroupIndexResultsUsedInFifoGroupScan, ITemplateHolder template) {
        super(curMultiList.getAllLists(), true, template.isServerIterator());
        _typeData = typeData;
        _fifoGroupIndexResultsUsedInFifoGroupScan = fifoGroupIndexResultsUsedInFifoGroupScan;
        _template = template;
    }

    @Override
    protected IScanListIterator<T> prepareListIterator(IObjectsList list) {

        if (list.isIterator()) {
            if (list instanceof MultiStoredList)
                return new FifoGroupsMultiList((MultiStoredList) list, _typeData, _fifoGroupIndexResultsUsedInFifoGroupScan, _template);
            else
                return (ExtendedIndexFifoGroupsIterator) list;
        }
        if (list == _typeData.getEntries())
            return new FifoGroupsScanAllValuesIterator(_typeData.getFifoGroupingIndex().getFifoGroupsIndexExtention().getFifoGroupLists());
        IStoredList<IEntryCacheInfo> sl = (IStoredList) list;
        if (_fifoGroupIndexResultsUsedInFifoGroupScan.get(list) == _typeData.getFifoGroupingIndex() && sl.size() > 1)
            return (new FifoGroupsScanByFGIndexIterator<T>((IStoredList<T>) sl));
        return (IScanListIterator<T>) FifoGroupCacheImpl.createScanIterOnNonFGIndex(sl, _template, _fifoGroupIndexResultsUsedInFifoGroupScan.get(sl), _typeData);
    }

    public void nextGroup()
            throws SAException {
        IScanListIterator sl = getCurrentList();
        if (sl != null && sl instanceof IFifoGroupIterator)
            ((IFifoGroupIterator) sl).nextGroup();

    }


    @Override
    public String getAlreadyMatchedIndexPath() {
        return null;
    }
}
