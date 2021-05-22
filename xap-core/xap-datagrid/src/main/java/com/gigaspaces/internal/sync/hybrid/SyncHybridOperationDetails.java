/*******************************************************************************
 * Copyright (c) 2012 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces.
 * You may use the software source code solely under the terms and limitations of
 * The license agreement granted to you by GigaSpaces.
 *******************************************************************************/
package com.gigaspaces.internal.sync.hybrid;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.sync.DataSyncOperationType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author yaeln
 * @since 11.0.1
 */
public class SyncHybridOperationDetails implements SmartExternalizable {
    private static final long serialVersionUID = 1L;
    private String spaceName;
    private DataSyncOperationType dataSyncOperationType;
    private SpaceDocument dataDocument;
    private Object dataObject;
    private IEntryPacket entryPacket;

    public SyncHybridOperationDetails(){}

    public SyncHybridOperationDetails(String spaceName, DataSyncOperationType dataSyncOperationType, IEntryPacket entryPacket) {

        this.spaceName = spaceName;
        this.dataSyncOperationType = dataSyncOperationType;
        this.entryPacket = entryPacket.clone();
        this.entryPacket.setSerializeTypeDesc(true);
    }

    public SpaceDocument getDataDocument() {
        if(dataDocument == null){
            dataDocument = (SpaceDocument) entryPacket.toObject(QueryResultTypeInternal.DOCUMENT_ENTRY);
        }
        return  dataDocument;
    }

    public Object getDataObject() {
        if(dataObject == null){
            dataObject = entryPacket.toObject(QueryResultTypeInternal.OBJECT_JAVA);
        }
        return dataObject;
    }

    public String getSpaceName() {
        return spaceName;
    }

    public String getUid() {
        return entryPacket.getUID();
    }

    public DataSyncOperationType getDataSyncOperationType() {
        return dataSyncOperationType;
    }

    @Override
    public String toString() {
        return "SyncHybridOperationDetails{" +
                "spaceName='" + spaceName + '\'' +
                ", uid='" + getUid() + '\'' +
                ", dataSyncOperationType=" + dataSyncOperationType +
                '}';
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, spaceName);
        IOUtils.writeObject(out, dataSyncOperationType);
        IOUtils.writeObject(out, entryPacket);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        spaceName = IOUtils.readString(in);
        dataSyncOperationType = IOUtils.readObject(in);
        entryPacket = IOUtils.readObject(in);
    }

}
