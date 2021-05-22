package com.gigaspaces.sync.serializable;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.DataSyncOperationType;

import java.io.*;
import java.util.Arrays;

@InternalApi
public class ExternalizableDataSyncOperation implements SmartExternalizable, DataSyncOperation {
    private static final long serialVersionUID = 6617861583815580942L;
    private boolean supportsDocument,supportsTypeDescriptor,supportsGetSpaceId;
    private SpaceDocument dataAsDocument;
    private ITypeDesc spaceTypeDescriptor;
    private Object spaceId;
    private String uid;
    private DataSyncOperationType dataSyncOperationType;

    public ExternalizableDataSyncOperation() {
    }

    public ExternalizableDataSyncOperation(DataSyncOperation dataSyncOperation) {
        supportsDocument = dataSyncOperation.supportsDataAsDocument();
        supportsGetSpaceId = dataSyncOperation.supportsGetSpaceId();
        supportsTypeDescriptor = dataSyncOperation.supportsGetTypeDescriptor();
        if(supportsDocument)
            dataAsDocument = dataSyncOperation.getDataAsDocument();
        if(supportsGetSpaceId)
            spaceId = dataSyncOperation.getSpaceId();
        if(supportsTypeDescriptor)
            spaceTypeDescriptor = (ITypeDesc) dataSyncOperation.getTypeDescriptor();
        uid = dataSyncOperation.getUid();
        dataSyncOperationType = dataSyncOperation.getDataSyncOperationType();
    }

    @Override
    public Object getSpaceId() {
        return spaceId;
    }

    @Override
    public String getUid() {
        return uid;
    }

    @Override
    public DataSyncOperationType getDataSyncOperationType() {
        return dataSyncOperationType;
    }

    @Override
    public Object getDataAsObject() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SpaceDocument getDataAsDocument() {
        return dataAsDocument;
    }

    @Override
    public SpaceTypeDescriptor getTypeDescriptor() {
        return spaceTypeDescriptor;
    }

    @Override
    public boolean supportsGetTypeDescriptor() {
        return supportsTypeDescriptor;
    }

    @Override
    public boolean supportsDataAsObject() {
        return false;
    }

    @Override
    public boolean supportsDataAsDocument() {
        return supportsDocument;
    }

    @Override
    public boolean supportsGetSpaceId() {
        return supportsGetSpaceId;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(supportsDocument);
        out.writeBoolean(supportsGetSpaceId);
        out.writeBoolean(supportsTypeDescriptor);
        if(supportsDocument)
            out.writeObject(dataAsDocument);
        if(supportsGetSpaceId)
            out.writeObject(spaceId);
        if(supportsTypeDescriptor)
            out.writeObject(spaceTypeDescriptor);
        IOUtils.writeString(out, uid);
        IOUtils.writeString(out, dataSyncOperationType.toString());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        supportsDocument = in.readBoolean();
        supportsGetSpaceId = in.readBoolean();
        supportsTypeDescriptor = in.readBoolean();
        if(supportsDocument)
            dataAsDocument = (SpaceDocument) in.readObject();
        if(supportsGetSpaceId)
            spaceId = in.readObject();
        if(supportsTypeDescriptor)
            spaceTypeDescriptor = (ITypeDesc) in.readObject();
        uid = IOUtils.readString(in);
        dataSyncOperationType = DataSyncOperationType.valueOf(IOUtils.readString(in));
    }

    public static DataSyncOperation[] convertDataSyncOperations(DataSyncOperation[] dataSyncOperations){
        return Arrays.stream(dataSyncOperations).map(ExternalizableDataSyncOperation::new).toArray(DataSyncOperation[]::new);
    }
}
