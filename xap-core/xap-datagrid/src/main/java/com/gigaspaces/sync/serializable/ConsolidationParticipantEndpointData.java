package com.gigaspaces.sync.serializable;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.sync.ConsolidationParticipantData;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.SynchronizationSourceDetails;
import com.gigaspaces.transaction.TransactionParticipantMetaData;

import static com.gigaspaces.sync.serializable.ExternalizableDataSyncOperation.convertDataSyncOperations;

@InternalApi
public class ConsolidationParticipantEndpointData implements ConsolidationParticipantData, EndpointData {

    private static final long serialVersionUID = 258825999388261637L;

    private final DataSyncOperation[] dataSyncOperations;
    private final SpaceSyncEndpointMethod spaceSyncEndpointMethod;
    private final SynchronizationSourceDetails synchronizationSourceDetails;

    public ConsolidationParticipantEndpointData(ConsolidationParticipantData consolidationParticipantData, SpaceSyncEndpointMethod spaceSyncEndpointMethod) {
        this.dataSyncOperations = convertDataSyncOperations(consolidationParticipantData.getTransactionParticipantDataItems());
        this.spaceSyncEndpointMethod = spaceSyncEndpointMethod;
        synchronizationSourceDetails = new SerializableSynchronizationSourceDetails(consolidationParticipantData.getSourceDetails());
    }

    @Override
    public DataSyncOperation[] getTransactionParticipantDataItems() {
        return dataSyncOperations;
    }

    @Override
    public TransactionParticipantMetaData getTransactionParticipantMetadata() {
        return null;
    }

    @Override
    public SynchronizationSourceDetails getSourceDetails() {
        return synchronizationSourceDetails;
    }

    @Override
    public void commit() {

    }

    @Override
    public void abort() {

    }

    @Override
    public SpaceSyncEndpointMethod getSyncEndpointMethod() {
        return spaceSyncEndpointMethod;
    }
}
