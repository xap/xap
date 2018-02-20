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

import com.gigaspaces.internal.cluster.node.impl.packets.data.filters.ReplicationFilterEntryDataWrapper;
import com.gigaspaces.internal.cluster.node.impl.view.EntryPacketServerEntryAdapter;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationFilterException;
import com.j_spaces.core.cluster.ReplicationOperationType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public abstract class SingleReplicationPacketData extends AbstractReplicationPacketSingleEntryData {
    private static final long serialVersionUID = 1L;

    private IEntryPacket _entryPacket;
    private volatile transient IEntryData _entryData;

    public SingleReplicationPacketData() {
    }

    public SingleReplicationPacketData(IEntryPacket entry, boolean fromGateway) {
        super(fromGateway);
        this._entryPacket = entry;
    }

    @Override
    public SingleReplicationPacketData clone() {
        return cloneWithEntryPacket(_entryPacket.clone());
    }

    public SingleReplicationPacketData cloneWithEntryPacket(IEntryPacket entryPacket) {
        SingleReplicationPacketData clone = (SingleReplicationPacketData) super.clone();
        clone._entryPacket = entryPacket;
        return clone;
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _entryPacket = (IEntryPacket) in.readObject();
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        _entryPacket = IOUtils.readNullableSwapExternalizableObject(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(_entryPacket);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        IOUtils.writeNullableSwapExternalizableObject(out, _entryPacket);
    }

    public String getUid() {
        return _entryPacket.getUID();
    }


    public IEntryPacket getEntryPacket() {
        return _entryPacket;
    }

    @Override
    public IReplicationFilterEntry toFilterEntry(
            SpaceTypeManager spaceTypeManager) {
        IServerTypeDesc serverTypeDesc = getTypeDescriptor(spaceTypeManager);
        ITypeDesc typeDesc = serverTypeDesc.getTypeDesc();
        return new ReplicationFilterEntryDataWrapper(this, getEntryPacket(), typeDesc, getFilterReplicationOpType(), getFilterObjectType(serverTypeDesc));
    }

    protected IServerTypeDesc getTypeDescriptor(SpaceTypeManager spaceTypeManager) {
        IServerTypeDesc serverTypeDesc = null;
        final String className = getEntryPacket().getTypeName();
        if (className != null) {
            try {
                serverTypeDesc = spaceTypeManager.loadServerTypeDesc(getEntryPacket());
            } catch (Exception e) {
                throw new ReplicationFilterException(e.getMessage(), e);
            }
        }
        if (serverTypeDesc == null)
            throw new ReplicationFilterException("Could not get server type descriptor for class " + className);

        return serverTypeDesc;
    }

    public int getOrderCode() {
        //In fifo, order code must be the same for all classes of the same name
        ITypeDesc typeDescriptor = _entryPacket.getTypeDescriptor();
        if (typeDescriptor != null && !typeDescriptor.isInactive() && typeDescriptor.isFifoSupported())
            return _entryPacket.getTypeName().hashCode();

        return getUid().hashCode();
    }

    public boolean isTransient() {
        return _entryPacket.isTransient();
    }

    protected abstract int getFilterObjectType(IServerTypeDesc serverTypeDesc);

    protected abstract ReplicationOperationType getFilterReplicationOpType();

    @Override
    public IEntryData getMainEntryData() {
        if (_entryData == null)
            _entryData = new EntryPacketServerEntryAdapter(getEntryPacket());

        return _entryData;
    }

    @Override
    public IEntryData getSecondaryEntryData() {
        return null;
    }

    public OperationID getOperationId() {
        return _entryPacket.getOperationID();
    }

    @Override
    public boolean containsFullEntryData() {
        return true;
    }

    protected boolean updateEntryPacketTimeToLiveIfNeeded(long expirationTime) {
        if (!getEntryPacket().isExternalizableEntryPacket()) {
            long currentTimeToLive = getEntryPacket().getTTL();
            long updateTimeToLiveIfNeeded = updateTimeToLiveIfNeeded(expirationTime, currentTimeToLive);
            if (updateTimeToLiveIfNeeded != currentTimeToLive)
                getEntryPacket().setTTL(updateTimeToLiveIfNeeded);
        }

        return true;
    }

}
