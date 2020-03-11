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

import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.list.IScanListIterator;
import com.j_spaces.kernel.list.ScanSingleListIterator;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 9.0
 */
 /*
 * scan iterator for all values of an fifo-group index which is rendered as null.
 * scan is done in order to traverse all possible groups  
 * NOTE !!!- for single threaded use
 */


@com.gigaspaces.api.InternalApi
public class FifoGroupsScanAllValuesIterator<T>
        implements IFifoGroupIterator<T> {
    //indexes values to scan
    private ScanSingleListIterator<Object> _entriesIter;

    private IScanListIterator<T> _curValueList;
    private ScanSingleListIterator<T> _lastIterUsed;
    private IFifoGroupsListHolder _curValueListHolder;  //currently no need to keep it, will be used in engine after entry lock


    public FifoGroupsScanAllValuesIterator(IStoredList<Object> entries) {
        _entriesIter = new ScanSingleListIterator<Object>(entries, false /*fifoScan*/);
    }

    /*
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext()
            throws SAException {
        while (true) {
            if (_entriesIter == null)
                return false;

            if (_curValueList != null) {
                if (_curValueList.hasNext())
                    return true;
                _curValueList.releaseScan();
                _curValueList = null;
            }
            if (_entriesIter != null && _entriesIter.hasNext()) {
                Object nxt = _entriesIter.next();
                IStoredList nextList = getActualList(nxt);
                if (nextList == null)
                    continue;  //next group
                if (_lastIterUsed == null) {
                    _curValueList = new ScanSingleListIterator<T>(nextList, true /*fifoScan*/);
                    _lastIterUsed = (ScanSingleListIterator<T>) _curValueList;
                } else {
                    _curValueList = _lastIterUsed;
                    _lastIterUsed.reuse(nextList);
                }
            } else {
                releaseScan();
            }
        }
    }

    /*
     * @see java.util.Iterator#next()
     */
    public T next() throws SAException {
        return _curValueList.next();
//		return res == null && isRelevantEntry(res) ? res : null;
    }

    /**
     * move to next group-value
     */
    public void nextGroup() throws SAException {
        _curValueListHolder = null;
        if (_curValueList != null) {
            _curValueList.releaseScan();
            _curValueList = null;
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
        if (_entriesIter != null) {
            _entriesIter.releaseScan();
            _entriesIter = null;
        }
        _curValueListHolder = null;

    }

    //TBD- we can optimize here
    public int getAlreadyMatchedFixedPropertyIndexPos() {
        return -1;
    }

    public boolean isAlreadyMatched() {
        return false;
    }

    public boolean isIterator() {
        return true;
    }


    protected IStoredList getActualList(Object candidate) {
        _curValueListHolder = (IFifoGroupsListHolder) candidate;
        return _curValueListHolder.getList();
    }

/*	 protected boolean isRelevantEntry(Object res)
     {
			EntryCacheInfo ci = (EntryCacheInfo) res;
			return  _curValueListHolder.getMainGroupValue().equals(_mainIndex.getIndexValue(ci.m_EntryHolder.getEntryData()));
	 }
*/
}