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

package com.gigaspaces.internal.sync.mirror;

import com.gigaspaces.cluster.replication.async.mirror.MirrorStatisticsImpl;
import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInTransactionHandler;
import com.gigaspaces.internal.cluster.node.handlers.ITransactionInContext;
import com.gigaspaces.internal.sync.ConsolidationParticipantDataImpl;
import com.gigaspaces.sync.DataSyncOperation;

import java.util.List;



/**
 * Handles replicated transaction on mirror. Each transaction is executed separately. non
 * transactional operations are aggregated for better performance
 *
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class MirrorReplicationInTransactionHandler extends MirrorReplicationInHandler
        implements IReplicationInTransactionHandler {
    public MirrorReplicationInTransactionHandler(MirrorBulkExecutor bulkExecutor,
                                                 MirrorStatisticsImpl operationStatisticsHandler) {
        super(bulkExecutor, operationStatisticsHandler);
    }

    public void inTransaction(IReplicationInContext context,
                              ITransactionInContext transactionContext) throws Exception {
        IReplicationInBatchContext batchContext = (IReplicationInBatchContext) context;

        //execute all accumulated operations 
        execute(batchContext, null);

        //add transaction and execute it separately
        addTransaction(batchContext, transactionContext);

        if (batchContext.getPendingContext().isEmpty()) {
            batchContext.currentConsumed();
            return;
        }

        if (context.supportsDistributedTransactionConsolidation() && transactionContext.getMetaData() != null && transactionContext.getMetaData().isUnconsoliated()) {
            if (_syncEndpoint != null) {
                List<DataSyncOperation> dataItems = batchContext.getPendingContext();
                ConsolidationParticipantDataImpl consolidationParticipantData = new ConsolidationParticipantDataImpl(dataItems, transactionContext.getMetaData(), context);
                try {
                    _syncEndpoint.onTransactionConsolidationFailure(consolidationParticipantData);
                } catch (Throwable t) {
                    if (context.getContextLogger().isWarnEnabled())
                        context.getContextLogger().warn("exception caught when executing transaction consolidation interceptor on consolidation aborted [" + consolidationParticipantData + "] - ignoring exception", t);
                }
                //If the user aborted the operation, we do not execute this transaction participant
                if (consolidationParticipantData.isAborted()) {
                    if (context.getContextLogger().isWarnEnabled())
                        context.getContextLogger().warn("aborting unconsolidated distributed transaction [" + consolidationParticipantData + "]");
                    batchContext.pendingConsumed();
                    // Clear the current bloom filter (a new one will be created for next execution)
                    batchContext.setTagObject(null);
                    return;
                } else if (context.getContextLogger().isWarnEnabled())
                    context.getContextLogger().warn("executing unconsolidated distributed transaction [" + consolidationParticipantData + "]");
            }
        }

        execute(batchContext,
                transactionContext.getMetaData());
    }

    @Override
    public void inTransactionPrepare(IReplicationInContext context,
                                     ITransactionInContext transactionContext) throws Exception {
        //Should not reach here, we convert this to one phase at the source
        throw new UnsupportedOperationException();
    }

    @Override
    public void inTransactionCommit(IReplicationInContext context,
                                    ITransactionInContext transactionContext) throws Exception {
        //Should not reach here, we convert this to one phase at the source
        throw new UnsupportedOperationException();
    }

    @Override
    public void inTransactionAbort(IReplicationInContext context,
                                   ITransactionInContext transactionContext) throws Exception {
        //Should not reach here, we convert this to one phase at the source
        throw new UnsupportedOperationException();
    }
}
