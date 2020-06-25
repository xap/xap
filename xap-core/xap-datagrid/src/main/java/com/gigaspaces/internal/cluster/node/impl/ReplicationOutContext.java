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

package com.gigaspaces.internal.cluster.node.impl;

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.cluster.node.IReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.IDirectPersistencyOpInfo;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.IDirectPersistencySyncHandler;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupOutContext;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.utils.threadlocal.AbstractResource;
import com.j_spaces.core.OperationID;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


@com.gigaspaces.api.InternalApi
public class ReplicationOutContext extends AbstractResource
        implements IReplicationOutContext,
        Iterable<IReplicationGroupOutContext> {
    private Map<String, IReplicationGroupOutContext> _groupsContext;
    private GroupMapping _mapping = GroupMapping.getAllMapping();
    private IReplicationGroupOutContext _singleGroupContext;
    //When adding new members, make sure to clean them at the clean method.
    private OperationID _operationID;
    private boolean[] _partialUpdatedValuesIndicators;
    public boolean[] _shouldReplicate;
    public Map<String, Object> _partialUpdatesAndInPlaceUpdatesInfo;
    public OperationID[] _operationIDs;
    private boolean _fromGateway;
    private Set<String> _overrideVersionUids;
    private IEntryData _previousUpdatedEntryData;
    private String _askedMarkerMembersGroupName;
    private IMarker _askedMarker;
    private Collection<SpaceEntryMutator> _spaceEntryMutators;
    private int _completed;
    //the following fields relates to directpersistency synchronization
    private IDirectPersistencyOpInfo _directPesistencyPendingEntry;
    private IDirectPersistencySyncHandler _directPersistencyHandler;
    // blobstore related fields
    private int _blobstoreReplicationBulkId;
    private boolean _blobstorePendingReplicationBulk; // indicates a blobstore bulk is flushed and waiting for replication
    private boolean _backupOnly;

    public IReplicationGroupOutContext getGroupContext(String groupName) {
        if (isSingleGroupParticipant()) {
            if (_singleGroupContext == null || groupName.equals(_singleGroupContext.getName()))
                return _singleGroupContext;
            return null;
        }

        return _groupsContext.get(groupName);
    }

    @Override
    protected void clean() {
        _singleGroupContext = null;
        _groupsContext = null;
        _operationID = null;
        _partialUpdatedValuesIndicators = null;
        _shouldReplicate = null;
        _partialUpdatesAndInPlaceUpdatesInfo = null;
        _operationIDs = null;
        _fromGateway = false;
        _overrideVersionUids = null;
        _previousUpdatedEntryData = null;
        _spaceEntryMutators = null;
        _askedMarkerMembersGroupName = null;
        _askedMarker = null;
        _directPesistencyPendingEntry = null;
        _directPersistencyHandler = null;
        _blobstoreReplicationBulkId = 0;
        _blobstorePendingReplicationBulk = false;
        _backupOnly = false;
    }

    public void setGroupContext(IReplicationGroupOutContext groupContext) {
        groupContext.setEntireContext(this);
        if (_singleGroupContext == null)
            _singleGroupContext = groupContext;
        else {
            if (_groupsContext == null) {
                _groupsContext = new HashMap<String, IReplicationGroupOutContext>();
                _groupsContext.put(_singleGroupContext.getName(), _singleGroupContext);
            }
            _groupsContext.put(groupContext.getName(), groupContext);
        }
    }

    public boolean isSingleGroupParticipant() {
        return _groupsContext == null;
    }

    public boolean isEmpty() {
        return _singleGroupContext == null && _directPersistencyHandler == null;
    }

    public IReplicationGroupOutContext getSingleGroupParticipantContext() {
        if (!isSingleGroupParticipant())
            throw new IllegalStateException("ReplicationOutContext has multiple participating groups");
        return _singleGroupContext;
    }

    public Iterator<IReplicationGroupOutContext> iterator() {
        if (isSingleGroupParticipant())
            throw new IllegalStateException("ReplicationOutContext has a single participating groups");
        return _groupsContext.values().iterator();
    }

    public GroupMapping getGroupMapping() {
        return _mapping;
    }

    public void setGroupMapping(GroupMapping mapping) {
        _mapping = mapping;
    }

    public OperationID getOperationID() {
        return _operationID;
    }

    public void setOperationID(OperationID operationID) {
        _operationID = operationID;
    }

    public boolean[] getPartialUpdatedValuesIndicators() {
        return _partialUpdatedValuesIndicators;
    }

    public void setPartialUpdatedValuesIndicators(
            boolean[] partialUpdatedValuesIndicators) {
        _partialUpdatedValuesIndicators = partialUpdatedValuesIndicators;
    }

    public boolean[] getShouldReplicate() {
        return _shouldReplicate;
    }

    public void setShouldReplicate(boolean[] shouldReplicate) {
        _shouldReplicate = shouldReplicate;
    }

    public Map<String, Object> getPartialUpdatesAndInPlaceUpdatesInfo() {
        return _partialUpdatesAndInPlaceUpdatesInfo;
    }

    public void setPartialUpdatesInfo(Map<String, Object> partialUpdatesAndInPlaceUpdatesInfo) {
        _partialUpdatesAndInPlaceUpdatesInfo = partialUpdatesAndInPlaceUpdatesInfo;
    }

    public OperationID[] getOperationIDs() {
        return _operationIDs;
    }

    public void setOperationIDs(OperationID[] operationIDs) {
        _operationIDs = operationIDs;
    }

    public boolean isFromGateway() {
        return _fromGateway;
    }

    public void setFromGateway(boolean fromGateway) {
        _fromGateway = fromGateway;
    }

    public int pendingSize() {
        if (isSingleGroupParticipant()) {
            if (_singleGroupContext == null)
                return 0;
            return _singleGroupContext.size();
        } else {
            int result = 0;
            for (IReplicationGroupOutContext groupContext : _groupsContext.values()) {
                result += groupContext.size();
            }
            return result;
        }
    }

    public void setOverrideVersionUids(Set<String> overrideVersionUids) {
        _overrideVersionUids = overrideVersionUids;
    }

    public boolean isOverrideVersion(String uid) {
        if (_overrideVersionUids == null)
            return false;
        return _overrideVersionUids.contains(uid);
    }

    public void setPreviousUpdatedEntryData(IEntryData PreviousUpdatedEntryData) {
        _previousUpdatedEntryData = PreviousUpdatedEntryData;
    }

    public IEntryData getPreviousUpdatedEntryData() {
        return _previousUpdatedEntryData;
    }

    public void setSpaceEntryMutators(
            Collection<SpaceEntryMutator> spaceEntryMutators) {
        _spaceEntryMutators = spaceEntryMutators;
    }

    public Collection<SpaceEntryMutator> getSpaceEntryMutators() {
        return _spaceEntryMutators;
    }

    @Override
    public void askForMarker(String membersGroupName) {
        _askedMarkerMembersGroupName = membersGroupName;
    }

    @Override
    public IMarker getRequestedMarker() {
        if (_askedMarker == null)
            throw new IllegalStateException("Cannot get requested marker without asking for one");
        IMarker askedMarker = _askedMarker;
        _askedMarker = null;
        _askedMarkerMembersGroupName = null;
        return askedMarker;
    }

    public String getAskedMarker() {
        return _askedMarkerMembersGroupName;
    }

    public void setMarker(IMarker marker) {
        _askedMarker = marker;
    }

    @Override
    public int setCompleted(int completed) {
        this._completed = completed;
        return this._completed;
    }

    @Override
    public int getCompleted() {
        return this._completed;
    }


    @Override
    public void setDirectPersistencyPendingEntry(IDirectPersistencyOpInfo directPesistencyEntry) {
        _directPesistencyPendingEntry = directPesistencyEntry;
    }

    @Override
    public IDirectPersistencyOpInfo getDirectPersistencyPendingEntry() {
        return _directPesistencyPendingEntry;
    }

    @Override
    public IDirectPersistencySyncHandler getDirectPesistencySyncHandler() {
        return _directPersistencyHandler;
    }

    @Override
    public void setDirectPesistencySyncHandler(IDirectPersistencySyncHandler directPesistencySyncHandler) {
        _directPersistencyHandler = directPesistencySyncHandler;
        ;
    }

    @Override
    public int getBlobstoreReplicationBulkId() {
        return _blobstoreReplicationBulkId;
    }

    @Override
    public void setBlobstoreReplicationBulkId(int blobstoreBulkId) {
        _blobstoreReplicationBulkId = blobstoreBulkId;
    }

    @Override
    public boolean isBlobstorePendingReplicationBulk() {
        return _blobstorePendingReplicationBulk;
    }

    @Override
    public void blobstorePendingReplicationBulk() {
        _blobstorePendingReplicationBulk = true;
    }


    public boolean isBackupOnly() {
        return _backupOnly;
    }

    public void setBackupOnly(boolean backupOnly) {
        _backupOnly = backupOnly;
    }

    @Override
    public String toString() {
        if (isEmpty())
            return "[empty]";
        if (isSingleGroupParticipant())
            return _singleGroupContext.toString();

        return _groupsContext.toString();
    }

}
