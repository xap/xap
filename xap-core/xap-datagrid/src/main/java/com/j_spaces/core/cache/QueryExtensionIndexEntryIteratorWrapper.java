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

import com.gigaspaces.query.extension.QueryExtensionEntryIterator;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.list.IScanListIterator;

/**
 * @author yechielf
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class QueryExtensionIndexEntryIteratorWrapper implements IScanListIterator<IEntryCacheInfo> {

    private final QueryExtensionIndexManagerWrapper manager;
    private final QueryExtensionEntryIterator iterator;
    private IEntryCacheInfo _nextEntry;
    private boolean _finished;
    private String alreadyMatchedIndexPath;

    public QueryExtensionIndexEntryIteratorWrapper(QueryExtensionIndexManagerWrapper manager, QueryExtensionEntryIterator iterator) {
        this.manager = manager;
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() throws SAException {
        _nextEntry = null;
        while (true) {
            if (_finished || iterator == null || !iterator.hasNext()) {
                _finished = true;
                return false;
            }
            _nextEntry = nextImpl();
            if (_nextEntry != null)
                return true;
        }
    }

    @Override
    public IEntryCacheInfo next() throws SAException {
        return _nextEntry;
    }

    private IEntryCacheInfo nextImpl() throws SAException {
        try {
            final String uid = iterator.nextUid();
            SpaceServerEntryImpl entry = uid != null ? manager.getByUid(uid) : null;
            return entry == null ? null : entry.getEntryCacheInfo();
        } catch (Exception ex) {
            throw new SAException(ex);
        }
    }

    /**
     * release SLHolder for this scan
     */
    @Override
    public void releaseScan() throws SAException {
        try {
            if (iterator != null)
                iterator.close();
        } catch (Exception ex) {
            throw new SAException(ex);
        }

    }

    /**
     * if the scan is on a property index, currently supported for extended index
     */
    @Override
    public int getAlreadyMatchedFixedPropertyIndexPos() {
        return -1;
        //return iterator == null ? -1 : iterator.getAlreadyMatchedFixedPropertyIndexPos();
    }

    @Override
    public String getAlreadyMatchedIndexPath() {
        return alreadyMatchedIndexPath;
        //return iterator == null ? null : iterator.getPrematchedPath();
    }

    public void setAlreadyMatchedIndexPath(String alreadyMatchedIndexPath) {
        this.alreadyMatchedIndexPath = alreadyMatchedIndexPath;
    }

    /**
     * is the entry returned already matched against the searching template currently is true if the
     * underlying scan made by CacheManager::EntriesIter
     */
    @Override
    public boolean isAlreadyMatched() {
        return false; //???????????????????????????????????????????????????????
    }

    @Override
    public boolean isExtensionIndex() {
        return true;
    }

    @Override
    public boolean isIterator() {
        return true;
    }

    @Override
    public boolean hasSize() {
        return true;
    }

    @Override
    public int size() {
        return iterator.size();
    }

    public boolean needRematch() {
        return iterator.isNeedRematch();
    }

}
