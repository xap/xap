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
package com.gigaspaces.sync.serializable;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.OperationsBatchData;
import com.gigaspaces.sync.SynchronizationSourceDetails;

import static com.gigaspaces.sync.serializable.ExternalizableDataSyncOperation.convertDataSyncOperations;

@InternalApi
public class OperationsBatchEndpointData implements OperationsBatchData, EndpointData {

    private static final long serialVersionUID = 258825999388261637L;

    private final DataSyncOperation[] batchDataItems;
    private final SpaceSyncEndpointMethod spaceSyncEndpointMethod;
    private final SynchronizationSourceDetails synchronizationSourceDetails;

    public OperationsBatchEndpointData(OperationsBatchData operationsBatchData, SpaceSyncEndpointMethod spaceSyncEndpointMethod) {
        this.batchDataItems = convertDataSyncOperations(operationsBatchData.getBatchDataItems());
        this.spaceSyncEndpointMethod = spaceSyncEndpointMethod;
        this.synchronizationSourceDetails = new SerializableSynchronizationSourceDetails(operationsBatchData.getSourceDetails());
    }

    @Override
    public DataSyncOperation[] getBatchDataItems() {
        return batchDataItems;
    }

    @Override
    public SynchronizationSourceDetails getSourceDetails() {
        return synchronizationSourceDetails;
    }

    @Override
    public SpaceSyncEndpointMethod getSyncEndpointMethod() {
        return spaceSyncEndpointMethod;
    }
}
