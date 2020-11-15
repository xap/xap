package com.gigaspaces.internal.server.storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.server.ServerEntry;

class PropertiesIterator {

    private final ServerEntry entryData;
    private final boolean isHybrid;
    private int nonSerializedIndex;
    private int serializedIndex;


    PropertiesIterator(ServerEntry entryData) {
        this.entryData = entryData;
        this.isHybrid = entryData instanceof HybridBinaryEntryData || entryData instanceof HybridViewEntryData;
        this.nonSerializedIndex = 0;
        this.serializedIndex = 0;
    }

    boolean hasNextI() {
        if (isHybrid) {
            ITypeDesc typeDesc = (ITypeDesc) entryData.getSpaceTypeDescriptor();
            return nonSerializedIndex < typeDesc.getNonSerializedProperties().length
                    && serializedIndex < typeDesc.getSerializedProperties().length;
        }

        return nonSerializedIndex < entryData.getSpaceTypeDescriptor().getNumOfFixedProperties();
    }

    int getNextI() {
        ITypeDesc typeDesc = (ITypeDesc) entryData.getSpaceTypeDescriptor();
        int i;
        if (isHybrid) {
            if (nonSerializedIndex < typeDesc.getNonSerializedProperties().length) {
                i = typeDesc.getNonSerializedProperties()[nonSerializedIndex].getOriginalIndex();
                nonSerializedIndex++;
            } else {
                i = typeDesc.getSerializedProperties()[serializedIndex].getOriginalIndex();
                serializedIndex++;
            }
        } else {
            i = nonSerializedIndex;
            nonSerializedIndex++;
        }
        return i;
    }
}
