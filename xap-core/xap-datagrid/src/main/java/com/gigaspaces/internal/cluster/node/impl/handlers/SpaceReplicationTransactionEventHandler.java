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

package com.gigaspaces.internal.cluster.node.impl.handlers;

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.AbstractReplicationEntryEventHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInTransactionHandler;
import com.gigaspaces.internal.cluster.node.handlers.ITransactionInContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationTransactionalPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ITransactionalExecutionCallback;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transaction.DummyTransactionManager;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.OperationID;

import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.ServerTransaction;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.List;


@com.gigaspaces.api.InternalApi
public class SpaceReplicationTransactionEventHandler implements IReplicationInTransactionHandler, ITransactionalExecutionCallback {
    private final SpaceEngine _spaceEngine;
    private final AbstractReplicationEntryEventHandler _entryHandler;
    private final DummyTransactionManager _txnManager;

    public SpaceReplicationTransactionEventHandler(SpaceEngine spaceEngine, AbstractReplicationEntryEventHandler entryHandler) {
        this._spaceEngine = spaceEngine;
        this._entryHandler = entryHandler;
        this._txnManager = DummyTransactionManager.getInstance();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void inTransaction(IReplicationInContext context, ITransactionInContext transactionContext) throws Exception {
        final boolean fromReplication = true;
        final boolean supportsTwoPhase = false;
        final OperationID operationId = null;
        ServerTransaction txn = null;

        List<IReplicationTransactionalPacketEntryData> packetsData = (List<IReplicationTransactionalPacketEntryData>) transactionContext;
        try {
            txn = _txnManager.create();
            _spaceEngine.attachToXtn(txn, fromReplication);
            for (IReplicationTransactionalPacketEntryData packetEntryData : packetsData) {
                packetEntryData.executeTransactional(context, this, txn, supportsTwoPhase);
            }
            //We mimic the behavior of local transaction manager, this could probably be refactored to create the server transaction correct one time
            //and pass that transaction through this entire method. The problem with this method that it creates a lease of -1 and not Lease.Forever,
            //if we change that we may be able to use this method instead of creating a ServerTransaction in the above code
            ServerTransaction transactionWithParticipantContent = _spaceEngine.getSpaceImpl().createServerTransaction(_txnManager, txn.id, 1);
            _spaceEngine.prepareAndCommit(_txnManager, transactionWithParticipantContent, operationId);
            _spaceEngine.enableDuplicateExecutionProtection(transactionContext.getOperationID());
        } catch (Exception e) {
            if (txn != null) {
                try {
                    ServerTransaction transactionWithParticipantContent = _spaceEngine.getSpaceImpl().createServerTransaction(_txnManager, txn.id, 1);
                    _spaceEngine.abort(_txnManager, transactionWithParticipantContent, supportsTwoPhase, operationId);
                } catch (Exception ei) {
                }
            }
            throw e;
        }
    }

    @Override
    public void inTransactionPrepare(IReplicationInContext context,
                                     ITransactionInContext transactionContext) throws Exception {
        ServerTransaction transaction = transactionContext.getTransaction();
        _spaceEngine.attachToXtn(transaction, true /*fromReplication*/);
        List<IReplicationTransactionalPacketEntryData> packetsData = (List<IReplicationTransactionalPacketEntryData>) transactionContext;
        for (IReplicationTransactionalPacketEntryData packetEntryData : packetsData) {
            packetEntryData.executeTransactional(context, this, transaction, true);
        }
        //Currently we should reach here only in multi participant transactions
        if (_spaceEngine.getOperationLogger().isTraceEnabled())
            _spaceEngine.getOperationLogger().trace("preparing transaction [" + _spaceEngine.createTransactionDetailsString(transaction, null) + "]");
        int prepareResult = _spaceEngine.prepare(transaction.mgr, transaction, false/*single participant*/, false, null /*OperationID*/);
        if (_spaceEngine.getOperationLogger().isTraceEnabled())
            _spaceEngine.getOperationLogger().trace("prepared transaction [" + _spaceEngine.createTransactionDetailsString(transaction, null) + "] result=" + prepareResult);
    }

    @Override
    public void inTransactionCommit(IReplicationInContext context,
                                    ITransactionInContext transactionContext) throws UnknownTransactionException, RemoteException {
        ServerTransaction transaction = transactionContext.getTransaction();
        if (_spaceEngine.getCacheManager().requiresEvictionReplicationProtection()) {
            List<IReplicationTransactionalPacketEntryData> packetsData = (List<IReplicationTransactionalPacketEntryData>) transactionContext;
            for (IReplicationTransactionalPacketEntryData packetEntryData : packetsData) {
                final IMarker marker = context.getContextMarker(_spaceEngine.getCacheManager().getEvictionPolicyReplicationMembersGroupName());
                _spaceEngine.getCacheManager().getEvictionReplicationsMarkersRepository().insert(packetEntryData.getUid(), marker, false /*alreadyLocked*/);
            }
        }
        _spaceEngine.commit(transaction.mgr, transaction, false, null, true);
        _spaceEngine.enableDuplicateExecutionProtection(transactionContext.getOperationID());
    }

    @Override
    public void inTransactionAbort(IReplicationInContext context,
                                   ITransactionInContext transactionContext) {
        ServerTransaction transaction = transactionContext.getTransaction();
        try {
            _spaceEngine.abort(transaction.mgr, transaction, false, null);
            _spaceEngine.enableDuplicateExecutionProtection(transactionContext.getOperationID());
        } catch (UnknownTransactionException e) {
            if (context.getContextLogger() != null && context.getContextLogger().isWarnEnabled()) {
                context.getContextLogger().warn("Replication of abort of two phase commit transaction [Metadata=" + transactionContext.getMetaData() + "] failed with unknown transaction exception, this could occur after failover where the transaction abort has already been replicated. ignoring operation");
            }
        }
    }

    @Override
    public void writeEntry(IReplicationInContext context, Transaction txn, boolean twoPhaseCommit, IEntryPacket entry)
            throws Exception {
        _entryHandler.writeEntry(context, txn, twoPhaseCommit, entry);
    }

    @Override
    public void updateEntry(IReplicationInContext context, Transaction txn, boolean twoPhaseCommit, IEntryPacket entry,
                            IEntryPacket previousEntryPacket, boolean partialUpdate, boolean overrideVersion, short flags)
            throws Exception {
        _entryHandler.updateEntry(context, txn, twoPhaseCommit, entry, previousEntryPacket, partialUpdate, overrideVersion);
    }

    @Override
    public void changeEntry(IReplicationInContext context,
                            Transaction transaction, boolean twoPhaseCommit, String typeName,
                            String uid, Object id, int version,
                            int previousVersion,
                            long timeToLive,
                            int routingHash, Collection<SpaceEntryMutator> spaceEntryMutators,
                            boolean isTransient, OperationID operationID, IEntryData previousEntryData) throws Exception {
        _entryHandler.changeEntry(context,
                transaction,
                twoPhaseCommit,
                typeName,
                uid,
                id,
                routingHash,
                version,
                previousVersion,
                timeToLive,
                spaceEntryMutators,
                isTransient,
                operationID,
                previousEntryData);
    }

    @Override
    public void removeEntry(IReplicationInContext context, Transaction txn, boolean twoPhaseCommit, IEntryPacket entry)
            throws Exception {
        _entryHandler.removeEntry(context, txn, twoPhaseCommit, entry);
    }

    @Override
    public void removeEntryByUID(IReplicationInContext context, Transaction txn, boolean twoPhaseCommit, String uid, boolean isTransient, OperationID operationID, String typeName)
            throws Exception {
        _entryHandler.removeEntryByUid(context, txn, twoPhaseCommit, uid, isTransient, operationID, typeName);
    }
}
