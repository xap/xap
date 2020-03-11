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
package com.j_spaces.kernel.list;

import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.kernel.IStoredList;

import java.util.Set;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 14.3
 */
/*
 * scan iterator for a list of uids
 * NOTE !!!- for single threaded use
 */
@com.gigaspaces.api.InternalApi
public class ScanUidsIterator
        implements IScanListIterator<IEntryCacheInfo>{

    private static final int WEIGHT_GT_1 = 5;

    private  Object[]       _uids;
    private  int            _nextPos;
    private  CacheManager    _cacheManager;
    private  IEntryCacheInfo _subject;

    public ScanUidsIterator(CacheManager    cacheManager,Set<String> uids) {
        _uids = uids.toArray();
        _cacheManager = cacheManager;
    }

    /*
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext() {
        while(_nextPos < _uids.length)
        {
            _subject = _cacheManager.getPEntryByUid((String)(_uids[_nextPos++]));
            if (_subject != null)
                return true;
        }
        return false;
    }

    /*
     * @see java.util.Iterator#next()
     */
    public IEntryCacheInfo next() {
        IEntryCacheInfo res = _subject;
        _subject =  null;
        return res;
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
    public void releaseScan() {
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

    public void reuse(IStoredList<IEntryCacheInfo> list) {
        throw new UnsupportedOperationException();
    }

    public int  size()
    {
        return _uids.length > 1? ((_uids.length -1) * WEIGHT_GT_1 + 1) : _uids.length;
    }
}
