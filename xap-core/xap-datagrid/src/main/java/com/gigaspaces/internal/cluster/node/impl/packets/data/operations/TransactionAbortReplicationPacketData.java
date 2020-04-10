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

import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IExecutableReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationTransactionalPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataMediator;
import com.j_spaces.core.OperationID;

import net.jini.core.transaction.server.ServerTransaction;



/**
 * Replication packet representing transaction abort
 *
 * @author eitany
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class TransactionAbortReplicationPacketData
        extends AbstractPrepareDependentTransactionReplicationPacketData {
    private static final long serialVersionUID = 1L;

    public TransactionAbortReplicationPacketData() {
    }

    public TransactionAbortReplicationPacketData(ServerTransaction transaction, OperationID operationID, boolean fromGateway) {
        super(transaction, operationID, fromGateway);
    }

    @Override
    public ReplicationMultipleOperationType getMultipleOperationType() {
        return ReplicationMultipleOperationType.TRANSACTION_TWO_PHASE_ABORT;
    }

    @Override
    public void execute(IReplicationInContext context,
                        IReplicationInFacade inReplicationHandler, ReplicationPacketDataMediator dataMediator) throws Exception {
        if (!attachPrepareContent(dataMediator) && context.getContextLogger() != null && context.getContextLogger().isWarnEnabled())
            context.getContextLogger().warn("No previous transaction pending state exists for replicated packet data " + toString() + ", ignoring operation");
        inReplicationHandler.inTransactionAbort(context, this);
    }

    @Override
    protected String getCustomToString() {
        if (getMetaData() == null)
            return super.getCustomToString();

        return super.getCustomToString() + "Participant ID=" + getMetaData().getParticipantId() + ", ";
    }

    @Override
    public IExecutableReplicationPacketData<IReplicationTransactionalPacketEntryData> createEmptyMultipleEntryData() {
        return new TransactionAbortReplicationPacketData(getTransaction(), getOperationID(), isFromGateway());
    }

    @Override
    public boolean isPartOfBlobstoreBulk() {
        return false;
    }

    @Override
    public boolean requiresRecoveryFiltering() {
        return true;
    }

    @Override
    public boolean isMultiParticipantData() {
        return false;
    }

}
