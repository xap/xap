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

package com.gigaspaces.internal.cluster.node.impl.packets.data;


import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilterCallback;
import com.gigaspaces.internal.cluster.node.impl.handlers.UnknownEntryLeaseException;
import com.gigaspaces.internal.cluster.node.impl.handlers.UnknownNotifyTemplateLeaseException;
import com.gigaspaces.internal.cluster.node.impl.packets.data.errors.UnknownConsumeErrorResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.errors.UnknownEntryLeaseConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.errors.UnknownNotifyTemplateLeaseConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.errors.UnknownTransactionConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.errors.UnknownTypeConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractTransactionReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.TransactionOnePhaseReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.TransactionReplicationPacketData;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.transaction.TransactionUniqueId;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationFilterException;
import com.j_spaces.core.exception.ClosedResourceException;
import com.j_spaces.core.exception.SpaceUnavailableException;

import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.TransactionParticipantDataImpl;

import java.util.Iterator;
import java.util.logging.Level;


@com.gigaspaces.api.InternalApi
public class ReplicationPacketDataConsumer
        implements
        IReplicationPacketDataBatchConsumer<IExecutableReplicationPacketData<?>> {

    private final IDataConsumeFixFacade _fixFacade;
    private final SpaceTypeManager _typeManager;
    private final ReplicationPacketDataMediator _packetDataMediator;

    public ReplicationPacketDataConsumer(SpaceTypeManager typeManager, IDataConsumeFixFacade fixFacade, ReplicationPacketDataMediator packetDataMediator) {
        _typeManager = typeManager;
        _fixFacade = fixFacade;
        _packetDataMediator = packetDataMediator;
    }

    public IDataConsumeResult consume(IReplicationInContext context,
                                      IExecutableReplicationPacketData<?> data,
                                      IReplicationInFacade replicationInFacade,
                                      IReplicationInFilterCallback inFilterCallback) {
        try {

            //invoke filter through callback
            if (inFilterCallback != null) {
                inFilterCallback.invokeInFilter(context, data);
                // entry can filtered (discarded) by the input filter, hence the isEmpty check
                if (data.isEmpty())
                    return DataConsumeOkResult.OK;
            }

            data.execute(context, replicationInFacade, _packetDataMediator);

            return DataConsumeOkResult.OK;
        } catch (ReplicationFilterException filterEx) {
            //get the cause of the exception
            Throwable cause = filterEx.getCause();
            return createErrorResult(cause != null ? cause : filterEx, context);
        } catch (Throwable e) {
            return createErrorResult(e, context);
        }
    }

    private AbstractDataConsumeErrorResult createErrorResult(Throwable e, IReplicationInContext context) {
        if (context.getContextLogger().isLoggable(Level.FINEST))
            context.getContextLogger().finest("received error result when consuming incoming replication [" + e + "], creating corresponding error result");

        if (e instanceof UnknownTypeException)
            return new UnknownTypeConsumeResult(((UnknownTypeException) e).getUnknownClassName(), (UnknownTypeException) e);

        if (e instanceof UnknownEntryLeaseException) {
            UnknownEntryLeaseException unknownLeaseException = (UnknownEntryLeaseException) e;
            return new UnknownEntryLeaseConsumeResult(unknownLeaseException.getClassName(), unknownLeaseException.getUID(), unknownLeaseException.getOperationID(), (UnknownEntryLeaseException) e);
        }
        if (e instanceof UnknownNotifyTemplateLeaseException) {
            UnknownNotifyTemplateLeaseException unknownLeaseException = (UnknownNotifyTemplateLeaseException) e;
            return new UnknownNotifyTemplateLeaseConsumeResult(unknownLeaseException.getClassName(), unknownLeaseException.getUID(), (UnknownNotifyTemplateLeaseException) e);
        }
        if (e instanceof SpaceUnavailableException) {
            throw new ClosedResourceException(e.getMessage(), e);
        }
        if (e instanceof UnknownTransactionException) {
            return new UnknownTransactionConsumeResult((UnknownTransactionException) e);
        }

        return new UnknownConsumeErrorResult(e);
    }

    public IExecutableReplicationPacketData<?> applyFix(
            IReplicationInContext context, IExecutableReplicationPacketData<?> data, IDataConsumeFix fix)
            throws Exception {
        if (fix instanceof AbstractDataConsumeFix) {
            if (context.getContextLogger().isLoggable(Level.FINEST))
                context.getContextLogger().finest("applying fix [" + fix.getClass().getName() + "], due previous to consumption error");

            // Apply the fix
            return ((AbstractDataConsumeFix) fix).fix(context, _fixFacade, this, data);
        }

        // not supported fix
        throw new UnsupportedOperationException("Unsupported consumer fix - "
                + fix);
    }

    public Iterable<IReplicationFilterEntry> toFilterEntries(
            IReplicationInContext context, IReplicationPacketData<?> data) {
        IExecutableReplicationPacketData<?> executableData = (IExecutableReplicationPacketData<?>) data;
        return executableData.toFilterEntries(_typeManager);
    }

    public IReplicationParticipantsMetadata extractParticipantsMetadata(
            IReplicationPacketData<?> data) {
        if (data instanceof AbstractTransactionReplicationPacketData) {
            AbstractTransactionReplicationPacketData typedData = (AbstractTransactionReplicationPacketData) data;

            if (typedData.getMetaData() != null)
                return (IReplicationParticipantsMetadata) typedData.getMetaData();
        }
        if (data instanceof TransactionReplicationPacketData) {
            TransactionReplicationPacketData typedData = (TransactionReplicationPacketData) data;

            if (typedData.getMetaData() != null)
                return (IReplicationParticipantsMetadata) typedData.getMetaData();

        }
        return SingleParticipantMetadata.INSTANCE;
    }

    @Override
    public IReplicationPacketData<?> merge(
            IReplicationPacketData<?>[] allParticipantsData, IReplicationParticipantsMetadata participantsMetadata) {
        final TransactionOnePhaseReplicationPacketData mergedData = new TransactionOnePhaseReplicationPacketData();
        // For a consolidated distributed transaction data we set the participantId to -1
        final TransactionParticipantDataImpl transactionParticipantData = new TransactionParticipantDataImpl((TransactionUniqueId) participantsMetadata.getContextId(),
                -1,
                participantsMetadata.getTransactionParticipantsCount());

        // Create iterator for each participant entry data list
        final Iterator<?>[] iterators = new Iterator[allParticipantsData.length];
        final IReplicationPacketEntryData[] pendingDataPerParticipant = new IReplicationPacketEntryData[allParticipantsData.length];
        int listsToMerge = 0;
        for (int i = 0; i < iterators.length; i++) {
            iterators[i] = allParticipantsData[i].iterator();
            // Handle empty participant data
            if (iterators[i].hasNext()) {
                pendingDataPerParticipant[i] = (IReplicationPacketEntryData) iterators[i].next();
                listsToMerge++;
            }
        }

        // Merge participants entry data lists
        // Each participant data is considered sorted
        while (listsToMerge > 0) {
            // Find next temporary entry data
            int index = 0;
            while (index < iterators.length && pendingDataPerParticipant[index] == null)
                index++;
            int minimumOperationIdIndex = index;

            index++;
            // Find next entry data to insert (with minimal operation id)
            while (index < iterators.length) {
                if (pendingDataPerParticipant[index] != null) {
                    if (pendingDataPerParticipant[index].getOperationId().getOperationID() < pendingDataPerParticipant[minimumOperationIdIndex].getOperationId()
                            .getOperationID()) {
                        minimumOperationIdIndex = index;
                    }
                }
                index++;
            }
            mergedData.add((IReplicationTransactionalPacketEntryData) pendingDataPerParticipant[minimumOperationIdIndex]);
            if (iterators[minimumOperationIdIndex].hasNext()) {
                pendingDataPerParticipant[minimumOperationIdIndex] = (IReplicationPacketEntryData) iterators[minimumOperationIdIndex].next();
            } else {
                pendingDataPerParticipant[minimumOperationIdIndex] = null;
                listsToMerge--;
            }
        }

        mergedData.setMetaData(transactionParticipantData);
        return mergedData;
    }

    @Override
    public IDataConsumeResult consumePendingPackets(
            IReplicationInBatchContext context, IReplicationInFacade replicationInFacade) {
        try {
            replicationInFacade.consumePendingOperationsInBatch(context);
            return DataConsumeOkResult.OK;
        } catch (Throwable e) {
            return createErrorResult(e, context);
        }
    }

    @Override
    public SpaceTypeManager getTypeManager() {
        return _typeManager;
    }


}
