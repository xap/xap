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

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.list.IScanListIterator;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 8.03
 */
 /*
 * scan iterator for an SAIterator 
 * NOTE !!!- for single threaded use
 */
@com.gigaspaces.api.InternalApi
public class ScanListSAIterator
        implements IScanListIterator<IEntryCacheInfo> {

    private final ISAdapterIterator<IEntryHolder> _SAiter;

    private IEntryCacheInfo _nextObj;


    public ScanListSAIterator(ISAdapterIterator<IEntryHolder> list) {
        _SAiter = list;
    }

    /*
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext()
            throws SAException {
        if (_SAiter == null)
            return false;
        IEntryHolder next = _SAiter.next();
        _nextObj = next != null ? EntryCacheInfoFactory.createEntryCacheInfo(next) : null;

        return _nextObj != null;
    }

    /*
     * @see java.util.Iterator#next()
     */
    public IEntryCacheInfo next() {
        // TODO Auto-generated method stub
        return _nextObj;
    }

    /*
     * @see java.util.Iterator#remove()
     */
    public void remove() {
        throw new UnsupportedOperationException();

    }

    public ISAdapterIterator<IEntryHolder> get_SAiter() {
        return _SAiter;
    }

    /**
     * release SLHolder for this scan
     */
    public void releaseScan()
            throws SAException {
        if (_SAiter != null)
            _SAiter.close();
    }

    //TBD- we can optimize here
    public int getAlreadyMatchedFixedPropertyIndexPos() {
        return -1;
    }

    public boolean isAlreadyMatched() {
        return true;
    }

    public boolean isIterator() {
        return true;
    }

}
