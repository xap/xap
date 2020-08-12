package com.gigaspaces.sync.serializable;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.SynchronizationSourceDetails;
import com.gigaspaces.sync.TransactionData;
import com.gigaspaces.transaction.ConsolidatedDistributedTransactionMetaData;
import com.gigaspaces.transaction.TransactionParticipantMetaData;

import static com.gigaspaces.sync.serializable.ExternalizableDataSyncOperation.convertDataSyncOperations;

@InternalApi
public class TransactionEndpointData implements TransactionData, EndpointData {

    private static final long serialVersionUID = 258825999388261637L;

    private final DataSyncOperation[] dataSyncOperations;
    private final SpaceSyncEndpointMethod spaceSyncEndpointMethod;
    private final SynchronizationSourceDetails synchronizationSourceDetails;

    public TransactionEndpointData(com.gigaspaces.sync.TransactionData transactionData, SpaceSyncEndpointMethod spaceSyncEndpointMethod) {
        this.dataSyncOperations = convertDataSyncOperations(transactionData.getTransactionParticipantDataItems());
        this.spaceSyncEndpointMethod = spaceSyncEndpointMethod;
        synchronizationSourceDetails = new SerializableSynchronizationSourceDetails(transactionData.getSourceDetails());
    }


    @Override
    public boolean isConsolidated() {
        return false;
    }

    @Override
    public TransactionParticipantMetaData getTransactionParticipantMetaData() {
        return null;
    }

    @Override
    public ConsolidatedDistributedTransactionMetaData getConsolidatedDistributedTransactionMetaData() {
        return null;
    }

    @Override
    public DataSyncOperation[] getTransactionParticipantDataItems() {
        return dataSyncOperations;
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
