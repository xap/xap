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


package com.gigaspaces.internal.server.storage;

import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.ILeasedEntryCacheInfo;
import com.j_spaces.core.cache.blobStore.IBlobStoreEntryHolder;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;

import java.util.ArrayList;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 5.0
 */

/**
 * shadowEntry is an entry version under pending update- it contains the original entry values note
 * that shadow and master are connected via a mutual reference
 */
@com.gigaspaces.api.InternalApi
public class ShadowEntryHolder extends EntryHolder
        implements ILeasedEntryCacheInfo {
    //backrefs of indexes
    private ArrayList<IObjectInfo<IEntryCacheInfo>> _shadowBackRefs;
    //per each index corres' to the values array-
    //the position of it in the backrefs array of the shadow
    private int[] _backrefIndexPos;
    //backrefs to the lease manager (Note- an update command can turn a non leased entry to leased
    private IStoredList<Object> _leaseManagerListRef; //null if non-leased
    private IObjectInfo<Object> _leaseManagerPosRef;
    //# of times the lease was updated under this xtn
    private int _numOfLeaseUpdates;
    private int _numOfIndexesUpdated;
    private int _numOfUpdates;

    public ShadowEntryHolder(IEntryHolder master, ArrayList<IObjectInfo<IEntryCacheInfo>> backRefs, int[] backrefIndexPos,
                             IStoredList<Object> leaseManagerListRef, IObjectInfo<Object> leaseManagerPosRef) {
        super(master);
        this._backrefIndexPos = backrefIndexPos;
        resetXidOriginated();
        setOtherUpdateUnderXtnEntry(master);

        if (backRefs != null) {
            this._shadowBackRefs = new ArrayList<IObjectInfo<IEntryCacheInfo>>(backRefs.size());
            for (IObjectInfo<IEntryCacheInfo> o : backRefs)
                this._shadowBackRefs.add(o);
        }
        _leaseManagerListRef = leaseManagerListRef; //null if non-leased
        _leaseManagerPosRef = leaseManagerPosRef;
    }

    public ArrayList<IObjectInfo<IEntryCacheInfo>> getBackRefs() {
        return _shadowBackRefs;
    }

    public void setShadowBackRefs(ArrayList<IObjectInfo<IEntryCacheInfo>> backRefs) {
        _shadowBackRefs = backRefs;
    }


    public int[] getBackrefIndexPos() {
        return _backrefIndexPos;
    }

    public void setBackrefIndexPos(int[] val) {
        _backrefIndexPos = val;
    }

    @Override
    public boolean isShadow() {
        return true;
    }

    @Override
    public boolean hasShadow(boolean safeEntry) {
        return false;
    }

    @Override
    public ShadowEntryHolder getShadow() {
        return this;
    }

    @Override
    public IEntryHolder getMaster() {
        return getOtherUpdateUnderXtnEntry();
    }

    @Override
    public boolean isUnderPendingUpdate() {
        return true;
    }

    public void setLeaseManagerListRefAndPosition(IStoredList<Object> entriesList, IObjectInfo<Object> entryPos) {
        _leaseManagerListRef = entriesList;
        _leaseManagerPosRef = entryPos;
    }

    @Override
    public IStoredList<Object> getLeaseManagerListRef() {
        return _leaseManagerListRef;
    }

    @Override
    public IObjectInfo<Object> getLeaseManagerPosition() {
        return _leaseManagerPosRef;
    }

    @Override
    public boolean isConnectedToLeaseManager() {
        if (isBlobStoreEntry())
            return getExpirationTime() != Long.MAX_VALUE;
        return (getLeaseManagerListRef() != null);
    }

    @Override
    public boolean isSameLeaseManagerRef(ILeasedEntryCacheInfo other) {
        if (isBlobStoreEntry())
            return other.getLeaseManagerPosition() == getLeaseManagerPosition();
        else
            return other.getLeaseManagerListRef() == getLeaseManagerListRef() &&
                    other.getLeaseManagerPosition() == getLeaseManagerPosition();
    }


    @Override
    public boolean isBlobStoreEntry() {
        return getOtherUpdateUnderXtnEntry().isBlobStoreEntry();
    }

    public Object getObjectStoredInLeaseManager() {
        return isBlobStoreEntry() ? ((IBlobStoreEntryHolder) getOtherUpdateUnderXtnEntry()).getBlobStoreResidentPart().getObjectStoredInLeaseManager() : getOtherUpdateUnderXtnEntry();
    }

    public void incrementNumOfLeaseUpdates() {
        _numOfLeaseUpdates++;
    }

    public int getNumOfLeaseUpdates() {
        return _numOfLeaseUpdates;
    }


    public void resetNumOfIndexesUpdated() {
        _numOfIndexesUpdated = 0;
    }

    public void incrementNumOfIndexesUpdated() {
        _numOfIndexesUpdated++;
    }

    public int getNumOfIndexesUpdated() {
        return _numOfIndexesUpdated;
    }

    public void incrementNumOfUpdates() {
        _numOfUpdates++;
    }

    public int getNumOfUpdates() {
        return _numOfUpdates;
    }
}
