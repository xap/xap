package org.openspaces.persistency.space;

import com.gigaspaces.datasource.SpaceTypeSchemaAdapter;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.sync.*;
import org.openspaces.core.GigaSpace;

import java.util.Arrays;
import java.util.Map;

public class GigaSpaceSynchronizationEndpoint extends SpaceSynchronizationEndpoint {
    private final GigaSpace targetSpace;
    private final Map<String, SpaceTypeSchemaAdapter> spaceTypeSchemaAdapters;

    public GigaSpaceSynchronizationEndpoint(GigaSpace targetSpace, Map<String, SpaceTypeSchemaAdapter> spaceTypeSchemaAdapters) {
        this.targetSpace = targetSpace;
        this.spaceTypeSchemaAdapters = spaceTypeSchemaAdapters;
    }

    @Override
    public void onTransactionSynchronization(TransactionData transactionData) {
        targetSpace.writeMultiple(adaptDataSyncOperations(transactionData.getTransactionParticipantDataItems()));
    }

    @Override
    public void onOperationsBatchSynchronization(OperationsBatchData batchData) {
        targetSpace.writeMultiple(adaptDataSyncOperations(batchData.getBatchDataItems()));
    }

    @Override
    public void onAddIndex(AddIndexData addIndexData) {
        targetSpace.getTypeManager().asyncAddIndexes(addIndexData.getTypeName(), addIndexData.getIndexes(), null);
    }

    @Override
    public void onIntroduceType(IntroduceTypeData introduceTypeData) {
        targetSpace.getTypeManager().registerTypeDescriptor(adaptSpaceTypeDescriptor(introduceTypeData));
    }

    private SpaceDocument[] adaptDataSyncOperations(DataSyncOperation[] dataSyncOperations){
        return Arrays.stream(dataSyncOperations).map(this::adaptSingleDataSyncOperation).toArray(SpaceDocument[]::new);
    }

    private SpaceDocument adaptSingleDataSyncOperation(DataSyncOperation dataSyncOperation) {
        if(!dataSyncOperation.supportsDataAsDocument())
            throw new UnsupportedOperationException("Only Space Document is supported");
        String typeName = dataSyncOperation.getTypeDescriptor().getTypeName();
        if(spaceTypeSchemaAdapters.containsKey(typeName))
            return spaceTypeSchemaAdapters.get(typeName).adaptEntry(dataSyncOperation.getDataAsDocument());
        return dataSyncOperation.getDataAsDocument();
    }

    private SpaceTypeDescriptor adaptSpaceTypeDescriptor(IntroduceTypeData introduceTypeData){
        String typeName = introduceTypeData.getTypeDescriptor().getTypeName();
        if(spaceTypeSchemaAdapters.containsKey(typeName))
            return spaceTypeSchemaAdapters.get(typeName).adaptTypeDescriptor(introduceTypeData.getTypeDescriptor());
        return introduceTypeData.getTypeDescriptor();
    }
}
