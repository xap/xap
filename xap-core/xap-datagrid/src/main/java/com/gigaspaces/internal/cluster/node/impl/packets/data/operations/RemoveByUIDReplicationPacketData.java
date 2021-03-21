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

import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.ReplicationInContentContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationTransactionalPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ITransactionalBatchExecutionCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ITransactionalExecutionCallback;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.j_spaces.core.ObjectTypes;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationOperationType;

import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


@com.gigaspaces.api.InternalApi
public class RemoveByUIDReplicationPacketData
        extends SingleUidReplicationPacketData implements IReplicationTransactionalPacketEntryData {
    private static final long serialVersionUID = 1L;
    private String _typeName;

    public RemoveByUIDReplicationPacketData() {
    }

    public RemoveByUIDReplicationPacketData(String typeName, String uid,
                                            boolean isTransient, IEntryData entryData, OperationID operationID, boolean fromGateway) {
        super(uid, operationID, isTransient, fromGateway, entryData);
        _typeName = typeName;
    }

    @Override
    protected void executeImpl(IReplicationInContext context,
                               IReplicationInFacade inReplicationHandler) throws Exception {
        inReplicationHandler.inRemoveEntryByUID(context, _typeName, getUid(), isTransient(), getOperationId());
    }

    public void executeTransactional(IReplicationInContext context,
                                     ITransactionalExecutionCallback transactionExecutionCallback,
                                     Transaction transaction, boolean twoPhaseCommit) throws Exception {
        try {
            transactionExecutionCallback.removeEntryByUID(context, transaction, twoPhaseCommit, getUid(), isTransient(), getOperationId(), _typeName);
        } finally {
            ReplicationInContentContext contentContext = context.getContentContext();
            if (contentContext != null) {
                _entryData = contentContext.getMainEntryData();
                contentContext.clear();
            }
        }
    }

    public boolean beforeDelayedReplication() {
        return true;
    }

    @Override
    public IReplicationFilterEntry toFilterEntry(
            SpaceTypeManager spaceTypeManager) {
        return super.toFilterEntry(spaceTypeManager);
    }

    @Override
    protected String getTypeName() {
        return _typeName;
    }

    @Override
    public RemoveByUIDReplicationPacketData clone() {
        return (RemoveByUIDReplicationPacketData) super.clone();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeRepetitiveString(out, _typeName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        _typeName = IOUtils.readRepetitiveString(in);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        IOUtils.writeString(out, _typeName);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        _typeName = IOUtils.readString(in);
    }

    @Override
    protected ReplicationOperationType getFilterOldReplicationOpType() {
        return ReplicationOperationType.TAKE;
    }

    public ReplicationSingleOperationType getOperationType() {
        return ReplicationSingleOperationType.REMOVE_ENTRY;
    }

    @Override
    protected int getFilterObjectType() {
        return ObjectTypes.ENTRY;
    }

    public void batchExecuteTransactional(IReplicationInBatchContext context,
                                          ITransactionalBatchExecutionCallback executionCallback)
            throws Exception {
        executionCallback.removeEntryByUID(context, getTypeName(), getUid(), getOperationId());
    }

    @Override
    public boolean filterIfNotPresentInReplicaState() {
        return true;
    }

    @Override
    public String toString() {
        return "REMOVE: " + "(uid=" + getUid() + ")";
    }

}
