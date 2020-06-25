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
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationTransactionalPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ITransactionalBatchExecutionCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ITransactionalExecutionCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataMediator;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.ObjectTypes;
import com.j_spaces.core.cluster.ReplicationOperationType;

import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


@com.gigaspaces.api.InternalApi
public class WriteReplicationPacketData
        extends SingleReplicationPacketData
        implements IReplicationTransactionalPacketEntryData {
    private static final long serialVersionUID = 1L;
    private boolean _backupOnly;
    private transient long _expirationTime;


    public WriteReplicationPacketData() {
    }

    @Override
    public WriteReplicationPacketData clone() {
        return (WriteReplicationPacketData) super.clone();
    }

    public WriteReplicationPacketData(IEntryPacket entry, boolean fromGateway, long expirationTime) {
        this(entry,fromGateway,expirationTime,false);
    }

    public WriteReplicationPacketData(IEntryPacket entry, boolean fromGateway, long expirationTime, boolean backupOnly) {
        super(entry, fromGateway);
        _expirationTime = expirationTime;
        _backupOnly= backupOnly;
    }

    public void execute(IReplicationInContext context,
                        IReplicationInFacade inReplicationHandler, ReplicationPacketDataMediator dataMediator) throws Exception {
        inReplicationHandler.inWriteEntry(context,
                getEntryPacket());

    }

    public void executeTransactional(IReplicationInContext context,
                                     ITransactionalExecutionCallback transactionExecutionCallback,
                                     Transaction transaction, boolean twoPhaseCommit) throws Exception {
        transactionExecutionCallback.writeEntry(context, transaction, twoPhaseCommit, getEntryPacket());
    }

    public void batchExecuteTransactional(IReplicationInBatchContext context,
                                          ITransactionalBatchExecutionCallback executionCallback)
            throws Exception {
        executionCallback.writeEntry(context, getEntryPacket());
    }

    public boolean beforeDelayedReplication() {
        return updateEntryPacketTimeToLiveIfNeeded(_expirationTime);
    }

    @Override
    protected int getFilterObjectType(IServerTypeDesc serverTypeDesc) {
        return ObjectTypes.ENTRY;
    }

    @Override
    protected ReplicationOperationType getFilterReplicationOpType() {
        return ReplicationOperationType.WRITE;
    }

    public ReplicationSingleOperationType getOperationType() {
        return ReplicationSingleOperationType.WRITE;
    }

    @Override
    public String toString() {
        return "WRITE: " + getEntryPacket();
    }

    @Override
    public boolean filterIfNotPresentInReplicaState() {
        return false;
    }

    @Override
    public boolean isBackupOnly() {
        return _backupOnly;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _expirationTime = !getEntryPacket().isExternalizableEntryPacket() && getEntryPacket().getTTL() != Long.MAX_VALUE ? getEntryPacket().getTTL() + SystemTime.timeMillis() : Long.MAX_VALUE;
        if(LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v15_5_0)){
            _backupOnly = in.readBoolean();
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        if(LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v15_5_0)){
            out.writeBoolean(_backupOnly);
        }
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        out.writeLong(_expirationTime);
        out.writeBoolean(_backupOnly);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readFromSwap(in);
        _expirationTime = in.readLong();
        _backupOnly = in.readBoolean();
    }

    public long getExpirationTime() {
        return _expirationTime;
    }

}
