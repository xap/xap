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

import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.sync.OperationsDataBatchImpl;
import com.gigaspaces.internal.sync.TransactionDataImpl;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.j_spaces.sadapter.datasource.EntryPacketDataConverter;
import com.j_spaces.sadapter.datasource.IDataConverter;
import com.j_spaces.sadapter.datasource.InternalBulkItem;

import net.jini.core.transaction.server.TransactionParticipantDataImpl;

import java.util.List;


/**
 * @author Niv Ingberg
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class MirrorBulkExecutor {

    private final SpaceSynchronizationEndpoint _syncEndpoint;
    private final SpaceTypeManager _typeManager;
    private final IDataConverter<IEntryPacket> _converter;

    public MirrorBulkExecutor(SpaceSynchronizationEndpoint syncEndpoint, SpaceTypeManager typeManager, Class<?> dataClass) {
        _syncEndpoint = syncEndpoint;
        _typeManager = typeManager;
        _converter = new EntryPacketDataConverter(typeManager, dataClass);
    }

    public SpaceSynchronizationEndpoint getSynchronizationInterceptor() {
        return _syncEndpoint;
    }


    public SpaceTypeManager getTypeManager() {
        return _typeManager;
    }

    public void execute(List<BulkItem> entries, TransactionParticipantDataImpl transactionMetaData, IReplicationInBatchContext batchContext) {
        if (entries.isEmpty())
            return;

        final DataSyncOperation[] operations = entries.toArray(new DataSyncOperation[entries.size()]);
        for (DataSyncOperation operation : operations) {
            final InternalBulkItem bulkItem = (InternalBulkItem) operation;
            bulkItem.setConverter(_converter);
        }
        if (transactionMetaData == null) {
            final OperationsDataBatchImpl batchData = new OperationsDataBatchImpl(operations, batchContext.getSourceLookupName());
            _syncEndpoint.onOperationsBatchSynchronization(batchData);
            try {
                _syncEndpoint.afterOperationsBatchSynchronization(batchData);
            } catch (Throwable t) {
                if (batchContext.getContextLogger().isWarnEnabled())
                    batchContext.getContextLogger().warn("Synchronization endpoint interceptor afterOperationsBatchSynchronization caused an exception", t);
            }
        } else {
            final TransactionDataImpl transactionData = new TransactionDataImpl(operations, transactionMetaData,
                    batchContext.getSourceLookupName());
            _syncEndpoint.onTransactionSynchronization(transactionData);
            try {
                _syncEndpoint.afterTransactionSynchronization(transactionData);
            } catch (Throwable t) {
                if (batchContext.getContextLogger().isWarnEnabled())
                    batchContext.getContextLogger().warn("Synchronization endpoint interceptor afterSynchronizingTransaction caused an exception", t);
            }
        }
    }
}
