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
//

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.IStoredListIterator;

import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@com.gigaspaces.api.InternalApi
public class MemoryBasedEntryCacheInfo implements IEntryCacheInfo {
    public static final int UID_HASH_INDICATOR = -1;

    final private IEntryHolder m_EntryHolder;
    private transient ArrayList<IObjectInfo<IEntryCacheInfo>> _backRefs;
    //creation number of latest index addition to the entry
    private int _latestIndexCreationNumber;
    //backrefs to the lease manager (Note- an update command can turn a non leased entry to leased
    private IStoredList<Object> _leaseManagerListRef; //null if non-leased
    private IObjectInfo<Object> _leaseManagerPosRef;

    /**
     * Const used only for snapshots, called from CacheManager.prePrepare(Context,
     * ServerTransaction). Assuming no need for the m_BackRefs ArrayList.
     */
    public MemoryBasedEntryCacheInfo(IEntryHolder entryHolder) {
        m_EntryHolder = entryHolder;
    }

    public MemoryBasedEntryCacheInfo(IEntryHolder entryHolder, int backRefsSize) {
        m_EntryHolder = entryHolder;
        _backRefs = new ArrayList<IObjectInfo<IEntryCacheInfo>>(backRefsSize);
    }

    /**
     * @return the m_EntryHolder.
     */
    @Override
    public IEntryHolder getEntryHolder(CacheManager cacheManager) {
        return m_EntryHolder;
    }

    public IEntryHolder getEntryHolder() {
        return m_EntryHolder;
    }

    @Override
    public IEntryHolder getEntryHolder(CacheManager cacheManager, Context context) {
        return m_EntryHolder;
    }

    /**
     * @return true if the entry is deleted
     */
    public boolean isDeleted() {
        return getEntryHolder().isDeleted();
    }

    /**
     * @return Returns the m_BackRefs.
     */
    @Override
    public ArrayList<IObjectInfo<IEntryCacheInfo>> getBackRefs() {
        return _backRefs;
    }

    @Override
    public void setBackRefs(ArrayList<IObjectInfo<IEntryCacheInfo>> backRefs) {
        _backRefs = backRefs;
    }

    @Override
    public IObjectInfo<IEntryCacheInfo> getMainListBackRef() {
        return _backRefs == null ? null : _backRefs.get(0);
    }

    @Override
    public boolean indexesBackRefsKept() {
        return true;
    }

    @Override
    public void setMainListBackRef(IObjectInfo<IEntryCacheInfo> mainListBackref) {
        _backRefs.add(mainListBackref);
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
        return m_EntryHolder;
    }

    @Override
    public IServerTypeDesc getServerTypeDesc()
    {
        return m_EntryHolder.getServerTypeDesc();
    }


    public int getLatestIndexCreationNumber() {
        return _latestIndexCreationNumber;
    }

    public void setLatestIndexCreationNumber(int val) {
        _latestIndexCreationNumber = val;
    }


    public String getClassName() {
        return getEntryHolder().getClassName();
    }


    /**
     * in order to avoid searching the eviction handler data structures when an existing entry is
     * rendered to it by the space cache manager, the eviction-handler may store a backref to the
     * entry in it data structure and retrieve it later
     */
    public void setEvictionPayLoad(Object evictionBackRef) {
        throw new RuntimeException("setEvictionBackref invalid here");
    }


    /**
     * in order to avoid searching the eviction handler data structures when an existing entry is
     * rendered to it by the space cache manager, the eviction-handler may store a backref to the
     * entry in it data structure and retrieve it later
     *
     * @return the eviction back-ref to the entry, or null if not stored
     */
    public Object getEvictionPayLoad() {
        return null;
    }


    public boolean isTransient() {
        return getEntryHolder().isTransient();
    }


    public String getUID() {
        return getEntryHolder().getUID();
    }

    //dummy cache manipulation methods
    public void setInCache(boolean checkPendingPin) {
    }

    public boolean isInCache() {
        return true;
    }

    public boolean isInserted() {
        return true;
    }

    public boolean setPinned(boolean value, boolean waitIfPendingInsertion) {
        return true;
    }

    public boolean setPinned(boolean value) {
        return setPinned(value, false /*waitIfPendingInsertion*/);
    }

    public boolean isPinned() {
        return true;
    }

    public boolean setRemoving(boolean isPinned) {
        return true;
    }

    public boolean isRemoving() {
        return false;
    }

    public void setRemoved() {
    }

    public boolean isRemoved() {
        return false;
    }

    public boolean isRemovingOrRemoved() {
        return false;
    }


    public boolean wasInserted() {
        return true;
    }

    public boolean isRecentDelete() {
        return false;
    }

    public void setRecentDelete() {
        throw new RuntimeException("invalid usage !!!");
    }

    @Override
    public boolean preMatch(Context context, ITemplateHolder template) {
        throw new RuntimeException("invalid usage !!!");
    }


    //+++++++++++++++++++  IStoredList-IObjectInfo methods for a unique-index single entry
    //+++++++++++++++++++  or single-values index
    public IObjectInfo<IEntryCacheInfo> getHead() {
        throw new RuntimeException(" invalid usage");
    }

    public IEntryCacheInfo getObjectFromHead() {
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

    public void dump(Logger logger, String msg) {
    }

    public int size() {
        return 1;
    }

    public boolean isEmpty() {
        return false;
    }

    public IStoredListIterator<IEntryCacheInfo> establishListScan(boolean random_scan) {
        return this;
    }

    public IStoredListIterator<IEntryCacheInfo> next(IStoredListIterator<IEntryCacheInfo> slh) {
        return null;
    }

    public IObjectInfo<IEntryCacheInfo> add(IEntryCacheInfo subject) {
        throw new RuntimeException(" invalid usage");
    }

    public IObjectInfo<IEntryCacheInfo> addUnlocked(IEntryCacheInfo subject) {
        throw new RuntimeException(" invalid usage");
    }

    public boolean removeByObject(IEntryCacheInfo obj) {
        return false;
    }

    public boolean contains(IEntryCacheInfo obj) {
        return this == obj;
    }

    public void setSubject(IEntryCacheInfo subject) {

    }

    public IEntryCacheInfo getSubject() {
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
    //methoda for IScanListIterator
    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    public boolean hasNext()
            throws SAException {
        return true;
    }

    public IEntryCacheInfo next()
            throws SAException {
        return this;
    }

    /**
     * release SLHolder for this scan
     */
    public void releaseScan()
            throws SAException {
    }

    /**
     * if the scan is on a property index, currently supported for extended index
     */
    public int getAlreadyMatchedFixedPropertyIndexPos() {
        return -1;
    }

    /**
     * is the entry returned already matched against the searching template currently is true if the
     * underlying scan made by CacheManager::EntriesIter
     */
    public boolean isAlreadyMatched() {
        return false;
    }

    public boolean isIterator() {
        return false;
    }


    //ServerEntry methods

    /**
     * Gets the entry's type descriptor.
     *
     * @return Current entry's type descriptor.
     */
    public SpaceTypeDescriptor getSpaceTypeDescriptor() {
        return getEntryHolder().getEntryData().getSpaceTypeDescriptor();
    }

    /**
     * Gets the specified fixed property's value.
     *
     * @param position Position of requested property.
     * @return Requested property's value in current entry.
     */
    public Object getFixedPropertyValue(int position) {
        return getEntryHolder().getEntryData().getFixedPropertyValue(position);

    }

    /**
     * Gets the specified property's value.
     *
     * @param name Name of requested property.
     * @return Requested property's value in current entry.
     */
    public Object getPropertyValue(String name) {
        return getEntryHolder().getEntryData().getPropertyValue(name);
    }

    @Override
    public Object getPathValue(String path) {
        return getEntryHolder().getEntryData().getPathValue(path);
    }


    /**
     * Gets the entry version.
     *
     * @return the entry version.
     * @since 9.0.0
     */
    public int getVersion() {
        return getEntryHolder().getEntryData().getVersion();

    }

    /**
     * Gets the entry expiration time.
     *
     * @return the entry expiration time.
     * @since 9.0.0
     */
    public long getExpirationTime() {
        return getEntryHolder().getEntryData().getExpirationTime();

    }


    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // methods for hash entry handling
    //---------------------------------------------------------------

    public int getHashCode(int id) {
        return getKey(id).hashCode();
    }

    public Object getKey(int id) {
        if (id == UID_HASH_INDICATOR)
            return getUID();
        return getEntryHolder().getEntryData().getFixedPropertyValue(id);
    }

    public IStoredList<IEntryCacheInfo> getValue(int id) {
        return this;
    }

    public boolean isNativeHashEntry() {
        return false;
    }

    public void release() {

    }
}
