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
