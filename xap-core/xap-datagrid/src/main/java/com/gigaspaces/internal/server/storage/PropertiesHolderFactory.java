package com.gigaspaces.internal.server.storage;

import com.gigaspaces.internal.metadata.ITypeDesc;

public class PropertiesHolderFactory {

    public static PropertiesHolder create(ITypeDesc typeDesc, IEntryData entryData){
        if(entryData.isHybrid()){
            return new HybridPropertiesHolder(entryData.getEntryTypeDesc().getTypeDesc(),
                    ((HybridEntryData) entryData).getNonSerializedProperties(), ((HybridEntryData)entryData).getPackedSerializedProperties());
        } else {
            return new FlatPropertiesHolder(entryData.getFixedPropertiesValues());
        }
    }

    public static PropertiesHolder create(ITypeDesc typeDesc, Object[] fields){
        if(typeDesc.getClassBinaryStorageAdapter() != null){
            return new HybridPropertiesHolder(typeDesc, fields);
        } else {
            return new FlatPropertiesHolder(fields);
        }
    }

    public static PropertiesHolder create(){
        return new FlatPropertiesHolder();
    }

}
