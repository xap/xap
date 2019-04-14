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

package com.gigaspaces.internal.cluster.node.impl.replica.data;

import com.gigaspaces.internal.cluster.node.impl.IIncomingReplicationFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.replica.ISpaceReplicaConsumeFacade;
import com.gigaspaces.internal.cluster.node.impl.replica.SpaceCopyIntermediateResult;
import com.gigaspaces.internal.cluster.node.impl.replica.data.filters.ReplicationFilterEntryReplicaDataWrapper;
import com.gigaspaces.internal.cluster.node.replica.SpaceCopyReplicaParameters.ReplicaType;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.client.EntryAlreadyInSpaceException;
import com.j_spaces.core.cluster.IReplicationFilterEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public abstract class AbstractEntryReplicaData extends AbstractReplicaData {
    private static final long serialVersionUID = 1L;
    private IEntryPacket _entryPacket;

    public AbstractEntryReplicaData() {
    }

    public AbstractEntryReplicaData(IEntryPacket entryPacket) {
        _entryPacket = entryPacket;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _entryPacket);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _entryPacket = IOUtils.readObject(in);
    }

    public String getUid() {
        return _entryPacket.getUID();
    }

    public IEntryPacket getEntryPacket() {
        return _entryPacket;
    }

    public void setEntryPacket(IEntryPacket _entryPacket) {
        this._entryPacket = _entryPacket;
    }

    public void execute(ISpaceReplicaConsumeFacade consumeFacade, SpaceCopyIntermediateResult intermediateResult, IIncomingReplicationFacade incomingReplicationFacade) throws Exception {
        try {
            consumeFacade.write(_entryPacket, getMarker(incomingReplicationFacade), getReplicaType());

            intermediateResult.increaseWritenTypeCount(_entryPacket.getTypeName());
        } catch (EntryAlreadyInSpaceException e) {
            intermediateResult.addDuplicateEntry(_entryPacket.getUID(), _entryPacket.getTypeName());
        }
    }

    protected abstract IMarker getMarker(IIncomingReplicationFacade incomingReplicationFacade);

    public boolean supportsReplicationFilter() {
        return true;
    }

    public IReplicationFilterEntry toFilterEntry(SpaceTypeManager typeManager) {
        ITypeDesc typeDesc = getTypeDescriptor(typeManager, getEntryPacket());
        return new ReplicationFilterEntryReplicaDataWrapper(getEntryPacket(), typeDesc);
    }

    public abstract ReplicaType getReplicaType();

    @Override
    public String toString() {
        return "EntryReplicaData " + _entryPacket;
    }

    @Override
    public boolean isEntryReplicaData() {
        return true;
    }
}