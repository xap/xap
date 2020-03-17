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

package com.gigaspaces.internal.server.space.operations;

import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.RemoteOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.j_spaces.core.exception.ClosedResourceException;

import java.rmi.RemoteException;
import java.util.logging.Level;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceOperationsExecutor {
    private final AbstractSpaceOperation<?, ?>[] _operations;

    public SpaceOperationsExecutor() {
        this._operations = new AbstractSpaceOperation[SpaceOperationsCodes.NUM_OF_OPERATIONS];
        this._operations[SpaceOperationsCodes.EXECUTE_TASK] = new ExecuteTaskSpaceOperation();
        this._operations[SpaceOperationsCodes.ABORT_TRANSACTION] = new AbortPreparedTransactionSpaceOperation();
        this._operations[SpaceOperationsCodes.COMMIT_TRANSACTION] = new CommitPreparedTransactionSpaceOperation();
        this._operations[SpaceOperationsCodes.PREPARE_AND_COMMIT_TRANSACTION] = new PrepareAndCommitTransactionSpaceOperation();
        this._operations[SpaceOperationsCodes.GET_ENTRY_TYPE_DESCRIPTOR] = new GetEntryTypeDescriptorSpaceOperation();
        this._operations[SpaceOperationsCodes.REGISTER_ENTRY_TYPE_DESCRIPTOR] = new RegisterEntryTypeDescriptorSpaceOperation();
        this._operations[SpaceOperationsCodes.ADD_ENTRY_TYPE_INDEXES] = new AddEntryTypeIndexesSpaceOperation();
        this._operations[SpaceOperationsCodes.UPDATE_LEASE] = new UpdateLeaseSpaceOperation();
        this._operations[SpaceOperationsCodes.UPDATE_LEASES] = new UpdateLeasesSpaceOperation();
        this._operations[SpaceOperationsCodes.WRITE_ENTRY] = new WriteEntrySpaceOperation();
        this._operations[SpaceOperationsCodes.WRITE_ENTRIES] = new WriteEntriesSpaceOperation();
        this._operations[SpaceOperationsCodes.READ_TAKE_ENTRY] = new ReadTakeEntrySpaceOperation();
        this._operations[SpaceOperationsCodes.READ_TAKE_ENTRIES] = new ReadTakeEntriesSpaceOperation();
        this._operations[SpaceOperationsCodes.READ_TAKE_ENTRIES_BY_IDS] = new ReadTakeEntriesByIdsSpaceOperation();
        this._operations[SpaceOperationsCodes.READ_TAKE_ENTRIES_UIDS] = new ReadTakeEntriesUidsSpaceOperation();
        this._operations[SpaceOperationsCodes.COUNT_CLEAR_ENTRIES] = new CountClearEntriesSpaceOperation();
        this._operations[SpaceOperationsCodes.REGISTER_ENTRIES_LISTENER] = new RegisterEntriesListenerSpaceOperation();
        this._operations[SpaceOperationsCodes.CHANGE_ENTRIES] = new ChangeEntriesSpaceOperation();
        this._operations[SpaceOperationsCodes.REGISTER_LOCAL_VIEW] = new RegisterLocalViewSpaceOperation();
        this._operations[SpaceOperationsCodes.UNREGISTER_LOCAL_VIEW] = new UnregisterLocalViewSpaceOperation();
        this._operations[SpaceOperationsCodes.AGGREGATE_ENTRIES] = new AggregateEntriesSpaceOperation();
    }

    public SpaceOperationsExecutor(AbstractSpaceOperation<?, ?>[] operations) {
        this._operations = operations;
    }

    public <T extends RemoteOperationResult> T executeOperation(RemoteOperationRequest<T> request, SpaceImpl space, boolean oneway) {
        // Get operation code from request:
        int operationCode = request.getOperationCode();

        // Get operation by operation code:
        if (operationCode < 0 || operationCode >= _operations.length)
            throw new UnsupportedOperationException("Unsupported space operation code: " + operationCode + ", request class: " + request.getClass().getName());
        AbstractSpaceOperation<T, RemoteOperationRequest<T>> operation = (AbstractSpaceOperation<T, RemoteOperationRequest<T>>) _operations[operationCode];

        // Initialize operation result:
        T result = request.createRemoteOperationResult();
        try {
            // TODO: Handle availability aspect.
            // TODO: Handle security aspect.
            // TODO: Handle memory management aspect.
            final boolean loggable = operation.isGenericLogging() && space.getOperationLogger().isLoggable(Level.FINEST);
            if (loggable)
                space.getOperationLogger().finest("executing " + operation.getLogName(request, result) + " operation" + (oneway ? "(oneway)" : "") + " [" + request + "]");
            // Execute operation:
            operation.execute(request, result, space, oneway);

            if (loggable)
                space.getOperationLogger().finest("operation " + operation.getLogName(request, result) + " executed" + (oneway ? "(oneway)" : "") + " [" + request + "] result [" + result + "]");
        } catch (ClosedResourceException e) {
            result.setExecutionException(new RemoteException(e.getMessage(), e));
        } catch (Exception e) {
            // TODO: Consider logging this exception.
            // Attach execution exception to result:
            result.setExecutionException(e);
        }

        return result;
    }
}
