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

import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.list.IScanListIterator;
import com.j_spaces.kernel.list.ScanSingleListIterator;

import java.util.HashSet;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 9.0
 */
 /*
 * scan iterator for all values of a matching group using a single non f-g index list.
 * scan is done in order to traverse all possible groups if the f-g index value rendered is null  
 * NOTE !!!- for single threaded use
 */



@com.gigaspaces.api.InternalApi
public class FifoGroupsScanByGeneralIndexIterator
        implements IFifoGroupIterator<IEntryCacheInfo> {

    private final TypeDataIndex<Object> _f_g_index;

    private HashSet<Object> _alreadyUsedGroupValues;
    private Object _curGroupValue;
    private final Object _onlyGroupValue; //in case only a specific group desired

    private IScanListIterator<IEntryCacheInfo> _curValueList;

    public FifoGroupsScanByGeneralIndexIterator(TypeDataIndex<Object> f_g_index, IStoredList<IEntryCacheInfo> entries) {
        this(f_g_index, entries, null);
    }

    public FifoGroupsScanByGeneralIndexIterator(TypeDataIndex<Object> f_g_index, IStoredList<IEntryCacheInfo> entries, Object onlyGroupValue) {
        _f_g_index = f_g_index;
        _onlyGroupValue = onlyGroupValue;
        _curValueList = new ScanSingleListIterator<IEntryCacheInfo>(entries, true /*fifoScan*/);
    }

    /*
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext()
            throws SAException {
        if (_curValueList != null) {
            if (_curValueList.hasNext())
                return true;
            _curValueList.releaseScan();
            _curValueList = null;
        }
        return false;
    }

    /*
     * @see java.util.Iterator#next()
     */
    public IEntryCacheInfo next() throws SAException {
        if (_curValueList == null)
            return null;

        IEntryCacheInfo res = null;
        do {
            res = _curValueList.next();
            Object groupValue;
            if (res != null) {
                groupValue = _f_g_index.getIndexValue(res.getEntryHolder(_f_g_index.getCacheManager()).getEntryData());
                if (_onlyGroupValue != null && !_onlyGroupValue.equals(groupValue))
                    continue;
                if (_alreadyUsedGroupValues == null || !_alreadyUsedGroupValues.contains(groupValue)) {
                    _curGroupValue = groupValue;
                    return res;
                }
            }
            if (!_curValueList.hasNext()) {
                _curValueList.releaseScan();
                _curValueList = null;
                return null;
            }
        } while (true);

    }

    /**
     * move to next group-value
     */
    public void nextGroup() throws SAException {
        if (_curGroupValue != null) {
            if (_alreadyUsedGroupValues == null)
                _alreadyUsedGroupValues = new HashSet<Object>();
            _alreadyUsedGroupValues.add(_curGroupValue);
            _curGroupValue = null;
        }
    }

    /*
     * @see java.util.Iterator#remove()
     */
    public void remove() {
        throw new UnsupportedOperationException();

    }

    /**
     * release SLHolder for this scan
     */
    public void releaseScan() throws SAException {
        if (_curValueList != null) {
            _curValueList.releaseScan();
            _curValueList = null;
        }

    }

    //TBD- we can optimize here
    public int getAlreadyMatchedFixedPropertyIndexPos() {
        return -1;
    }

    @Override
    public String getAlreadyMatchedIndexPath() {
        return null;
    }

    public boolean isAlreadyMatched() {
        return false;
    }

    @Override
    public boolean isExtensionIndex() {
        return false;
    }

    public boolean isIterator() {
        return true;
    }


}
