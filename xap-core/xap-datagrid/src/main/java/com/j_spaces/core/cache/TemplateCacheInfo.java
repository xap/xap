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

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.IStoredListIterator;

import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@com.gigaspaces.api.InternalApi
public class TemplateCacheInfo
        implements ILeasedEntryCacheInfo, IStoredList<TemplateCacheInfo>,
        IStoredListIterator<TemplateCacheInfo>, IObjectInfo<TemplateCacheInfo> {
    public ArrayList<IObjectInfo<TemplateCacheInfo>> m_BackRefs;

    //creation number of latest index addition to the template
    private int _latestIndexCreationNumber;

    final public ITemplateHolder m_TemplateHolder;
    //the following fields are backrefs to the lease manager relevant only for leased notify template
    //backrefs to the lease manager (Note- an update command can turn a non leased entry to leased
    private IStoredList<Object> _leaseManagerListRef; //null if non-leased
    private IObjectInfo<Object> _leaseManagerPosRef;

    public TemplateCacheInfo(ITemplateHolder templateHolder) {
        //m_BackRefs = new ArrayList();
        m_TemplateHolder = templateHolder;
    }

    public void initBackRefs(int numOfIndexes) {
        m_BackRefs = new ArrayList<IObjectInfo<TemplateCacheInfo>>(numOfIndexes);
    }

    public void setLeaseManagerListRefAndPosition(IStoredList<Object> entriesList, IObjectInfo<Object> entryPos) {
        _leaseManagerListRef = entriesList;
        _leaseManagerPosRef = entryPos;
    }


    public IStoredList<Object> getLeaseManagerListRef() {
        return _leaseManagerListRef;
    }

    public IObjectInfo<Object> getLeaseManagerPosition() {
        return _leaseManagerPosRef;
    }

    public boolean isConnectedToLeaseManager() {
        return (getLeaseManagerListRef() != null);
    }

    public boolean isSameLeaseManagerRef(ILeasedEntryCacheInfo other) {
        return other.getLeaseManagerListRef() == getLeaseManagerListRef() &&
                other.getLeaseManagerPosition() == getLeaseManagerPosition();
    }

    @Override
    public boolean isBlobStoreEntry() {
        return false;
    }

    @Override
    public Object getObjectStoredInLeaseManager() {
        return m_TemplateHolder;
    }

    public int getLatestIndexCreationNumber() {
        return _latestIndexCreationNumber;
    }

    public void setLatestIndexCreationNumber(int val) {
        _latestIndexCreationNumber = val;
    }

    //+++++++++++++++++++  IStoredList-IObjectInfo methods for a unique-index single entry
    //+++++++++++++++++++  or single-values index
    public IObjectInfo<TemplateCacheInfo> getHead() {
        throw new RuntimeException(" invalid usage");
    }

    public TemplateCacheInfo getObjectFromHead() {
        return this;
    }

    public void freeSLHolder(IStoredListIterator slh) {
        throw new RuntimeException(" invalid usage");
    }

    public void remove(IObjectInfo oi) {
        throw new RuntimeException(" invalid usage");
    }

    public void removeUnlocked(IObjectInfo oi) {
        throw new RuntimeException(" invalid usage");
    }

    public boolean invalidate() {
        return false;
    }

    @Override
    public void dump(Logger logger, String msg) {
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    public IStoredListIterator<TemplateCacheInfo> establishListScan(boolean random_scan) {
        return this;
    }

    public IStoredListIterator<TemplateCacheInfo> next(IStoredListIterator<TemplateCacheInfo> slh) {
        return null;
    }

    public IObjectInfo<TemplateCacheInfo> add(TemplateCacheInfo subject) {
        throw new RuntimeException(" invalid usage");
    }

    public IObjectInfo<TemplateCacheInfo> addUnlocked(TemplateCacheInfo subject) {
        throw new RuntimeException(" invalid usage");
    }

    public boolean removeByObject(TemplateCacheInfo obj) {
        return false;
    }

    public boolean contains(TemplateCacheInfo obj) {
        return this == obj;
    }

    public void setSubject(TemplateCacheInfo subject) {

    }

    public TemplateCacheInfo getSubject() {
        return this;
    }

    public boolean isMultiObjectCollection() {
        //its a single entry and not a container
        return false;
    }

    /**
     * return true if we can save iterator creation and get a single entry
     *
     * @return true if we can optimize
     */
    public boolean optimizeScanForSingleObject() {
        return true;
    }

    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // methods for hash entry handling
    //---------------------------------------------------------------

    public int getHashCode(int id) {
        throw new UnsupportedOperationException();
    }

    public Object getKey(int id) {
        throw new UnsupportedOperationException();
    }

    public IStoredList<TemplateCacheInfo> getValue(int id) {
        return this;
    }

    public boolean isNativeHashEntry() {
        throw new UnsupportedOperationException();
    }

    public void release() {

    }

    //++++++++++++++++++++++++++++++++++++++++++++++++++++++
    //IScanListIterator methods- support only a single object get
    //++++++++++++++++++++++++++++++++++++++++++++++++++++++
    public boolean isMultiValueIterator() {
        return false;
    }

    public void releaseScan() {
    }

    public int getIndexPos() {
        return -1;
    }

    public boolean hasNext() {
        return true;
    }

    /*
     * @see java.util.Iterator#next()
     */
    public TemplateCacheInfo next() {
        return this;

    }

    /*
     * @see java.util.Iterator#remove()
     */
    public void remove() {
        throw new UnsupportedOperationException();

    }

    public boolean isIterator() {
        return false;
    }


}
