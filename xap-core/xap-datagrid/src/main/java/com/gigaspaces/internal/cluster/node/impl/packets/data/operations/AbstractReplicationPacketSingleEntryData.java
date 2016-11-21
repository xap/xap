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

package com.gigaspaces.internal.cluster.node.impl.packets.data.operations;

import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IExecutableReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.view.EntryPacketServerEntryAdapter;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.ExternalizableServerEntry;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.server.ServerEntry;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cluster.IReplicationFilterEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A base class for {@link IReplicationPacketData} implementations that contains only one {@link
 * IReplicationPacketEntryData}
 *
 * @author eitany
 * @since 8.0
 */
public abstract class AbstractReplicationPacketSingleEntryData
        implements
        IExecutableReplicationPacketData<IReplicationPacketEntryData>,
        IReplicationPacketEntryData {

    private static final long serialVersionUID = 1L;

    private boolean _fromGateway;

    private int _blobstoreBulkdId;

    private static final int FLAGS_GATEWAY = 1 << 0;
    private static final int FLAGS_BLOBSTORE_BULK = 1 << 1;

    private transient int _weight;
    //Externalizable
    public AbstractReplicationPacketSingleEntryData() {
    }

    public boolean isPartOfBlobstoreBulk() {
        return _blobstoreBulkdId != 0;
    }

    public void setBlobstoreBulkId(int blobStoreBulkReplicationId) {
        _blobstoreBulkdId = blobStoreBulkReplicationId;
    }

    public int getBlobstoreBulkId() {
        return _blobstoreBulkdId;
    }

    public AbstractReplicationPacketSingleEntryData(boolean fromGateway) {
        this._fromGateway = fromGateway;
    }

    public boolean isFromGateway() {
        return _fromGateway;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(buildFlags());
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v11_0_0)) {
            if (isPartOfBlobstoreBulk()) {
                out.writeInt(_blobstoreBulkdId);
            }
        }
    }

    private short buildFlags() {
        short flags = 0;
        if (_fromGateway)
            flags |= FLAGS_GATEWAY;
        if (isPartOfBlobstoreBulk())
            flags |= FLAGS_BLOBSTORE_BULK;
        return flags;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        short flags = in.readShort();
        _fromGateway = (flags & FLAGS_GATEWAY) != 0;
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v11_0_0)) {
            if ((flags & FLAGS_BLOBSTORE_BULK) != 0) {
                _blobstoreBulkdId = in.readInt();
            }
        }
    }

    public void writeToSwap(ObjectOutput out) throws IOException {
        out.writeShort(buildFlags());
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v11_0_0)) {
            if (isPartOfBlobstoreBulk()) {
                out.writeInt(_blobstoreBulkdId);
            }
        }
    }

    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        short flags = in.readShort();
        _fromGateway = (flags & FLAGS_GATEWAY) != 0;
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v11_0_0)) {
            if ((flags & FLAGS_BLOBSTORE_BULK) != 0) {
                _blobstoreBulkdId = in.readInt();
            }
        }
    }

    public class SingleEntryIterator
            implements Iterator<IReplicationPacketEntryData> {

        private boolean _nextCalled;

        public boolean hasNext() {
            if (_removed)
                return false;

            return !_nextCalled;
        }

        public IReplicationPacketEntryData next() {
            if (_nextCalled)
                throw new NoSuchElementException();
            _nextCalled = true;
            return AbstractReplicationPacketSingleEntryData.this;
        }

        public void remove() {
            _removed = true;
        }

    }

    public boolean isSingleEntryData() {
        return true;
    }

    public IReplicationPacketEntryData getSingleEntryData() {
        if (_removed)
            return null;
        return this;
    }

    public static class SingleFilterIterable
            implements Iterable<IReplicationFilterEntry>,
            Iterator<IReplicationFilterEntry> {

        private final IReplicationFilterEntry _filterEntry;
        private boolean _moved;

        public SingleFilterIterable(IReplicationFilterEntry filterEntry) {
            _filterEntry = filterEntry;
        }

        public Iterator<IReplicationFilterEntry> iterator() {
            return this;
        }

        public boolean hasNext() {
            return !_moved;
        }

        public IReplicationFilterEntry next() {
            if (_moved)
                throw new NoSuchElementException();
            _moved = true;
            return _filterEntry;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    private transient boolean _removed;

    public boolean add(IReplicationPacketEntryData o) {
        return false;
    }

    public boolean addAll(Collection<? extends IReplicationPacketEntryData> c) {
        return false;
    }

    public void clear() {
        _removed = true;
    }

    public boolean contains(Object o) {
        return !_removed && o == this;
    }

    public boolean containsAll(Collection<?> c) {
        return false;
    }

    public boolean isEmpty() {
        return _removed;
    }

    public Iterator<IReplicationPacketEntryData> iterator() {
        return new SingleEntryIterator();
    }

    public boolean remove(Object o) {
        if (o == this) {
            _removed = true;
            return true;
        }
        return false;
    }

    public boolean removeAll(Collection<?> c) {
        if (c.contains(this)) {
            _removed = true;
            return true;
        }
        return false;
    }

    public boolean retainAll(Collection<?> c) {
        if (_removed)
            return false;

        if (c.contains(this))
            return false;

        return true;
    }

    public int size() {
        return _removed ? 0 : 1;
    }

    public Object[] toArray() {
        return _removed ? new Object[]{} : new Object[]{this};
    }

    public <T> T[] toArray(T[] a) {
        return null;
    }

    @Override
    public AbstractReplicationPacketSingleEntryData clone() {
        try {
            return (AbstractReplicationPacketSingleEntryData) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError();
        }
    }

    public boolean supportsReplicationFilter() {
        return true;
    }

    public boolean requiresRecoveryDuplicationProtection() {
        return true;
    }

    @Override
    public boolean requiresRecoveryFiltering() {
        return false;
    }

    public Iterable<IReplicationFilterEntry> toFilterEntries(SpaceTypeManager spaceTypeManager) {
        return new SingleFilterIterable(toFilterEntry(spaceTypeManager));
    }

    protected abstract IReplicationFilterEntry toFilterEntry(SpaceTypeManager spaceTypeManager);

    public abstract IEntryData getMainEntryData();

    public abstract IEntryData getSecondaryEntryData();

    public String getMainTypeName() {
        ServerEntry mainServerEntry = getMainEntryData();
        if (mainServerEntry != null && mainServerEntry.getSpaceTypeDescriptor() != null)
            return mainServerEntry.getSpaceTypeDescriptor().getTypeName();

        return null;
    }

    @Override
    public IExecutableReplicationPacketData<IReplicationPacketEntryData> createEmptyMultipleEntryData() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OperationID getOperationId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMultiParticipantData() {
        return false;
    }

    @Override
    public ReplicationMultipleOperationType getMultipleOperationType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getRecoveryFilteringId() {
        throw new UnsupportedOperationException();
    }

    public Object getCustomContent() {
        return null;
    }

    public long updateTimeToLiveIfNeeded(long expirationTime, long currentTimeToLive) {
        if (expirationTime == Long.MAX_VALUE)
            return currentTimeToLive;

        long updatedTimeToLive = expirationTime - SystemTime.timeMillis();
        //If updated time to live is negative, this packet will be sent with lease of 1 millisecond. 
        if (updatedTimeToLive <= 0)
            updatedTimeToLive = Math.min(10000, currentTimeToLive);

        return updatedTimeToLive;
    }

    protected void serializeEntryData(IEntryData entryData, ObjectOutput out)
            throws IOException {
        if (entryData instanceof ExternalizableServerEntry || entryData instanceof EntryPacketServerEntryAdapter)
            IOUtils.writeObject(out, entryData);
        else if (entryData == null)
            IOUtils.writeObject(out, entryData);
        else
            IOUtils.writeObject(out, new ExternalizableServerEntry(entryData));
    }

    protected IEntryData deserializeEntryData(ObjectInput in) throws IOException,
            ClassNotFoundException {
        return IOUtils.readObject(in);
    }

    @Override
    public void setWeight(int weight) {
        _weight = weight;
    }

    @Override
    public int getWeight() {
        return _weight;
    }
}
