package com.gigaspaces.internal.server.storage;

import com.gigaspaces.annotation.pojo.BinaryStorageAdapterType;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;

import java.io.*;

public class HybridBinaryData implements Externalizable {
    private Object[] serializedProperties;
    private Object[] nonSerializedProperties;
    private byte[] serialized;
    private boolean isDeserialized;

    public HybridBinaryData() {
    }

    public HybridBinaryData(ITypeDesc typeDesc, Object[] nonSerializedProperties, byte[] serialized) {
        this.nonSerializedProperties = nonSerializedProperties;
        this.serialized = serialized;
        this.isDeserialized = false;
    }

    public HybridBinaryData(Object[] serializedProperties, Object[] nonSerializedProperties, byte[] serialized) {
        this.serializedProperties = serializedProperties;
        this.nonSerializedProperties = nonSerializedProperties;
        this.serialized = serialized;
        this.isDeserialized = true;
    }

    public HybridBinaryData(ITypeDesc typeDesc, byte[] serializedFields) {
        this.serialized = serializedFields;
        this.serializedProperties = new Object[typeDesc.getProperties().length];
        this.isDeserialized = false;
    }

    public Object[] getFixedProperties(ITypeDesc typeDesc) {
        if (!isDeserialized) {
            unpackSerializedProperties(typeDesc);
        }

        if (typeDesc.getBinaryStorageType().equals(BinaryStorageAdapterType.ALL)) {
            return serializedProperties;
        }else {
            Object[] fields = new Object[typeDesc.getProperties().length];
            int i = 0;
            for (PropertyInfo property : typeDesc.getProperties()) {
                if (property.isBinarySpaceProperty()) {
                    fields[i] = this.serializedProperties[typeDesc.findNewIndex(i)];
                } else {
                    fields[i] = this.nonSerializedProperties[typeDesc.findNewIndex(i)];
                }
                i++;
            }
            return fields;
        }
    }

    public Object getFixedProperty(ITypeDesc typeDesc, int position) {
        if (typeDesc.getBinaryStorageType().equals(BinaryStorageAdapterType.ALL)) {
            if (!isDeserialized) {
                unpackSerializedProperties(typeDesc);
            }
            return serializedProperties;
        }else {
            if(typeDesc.isBinaryProperty(position)){
                if (!isDeserialized) {
                    unpackSerializedProperties(typeDesc);
                }
                return serializedProperties[typeDesc.findNewIndex(position)];
            } else {
                return nonSerializedProperties[typeDesc.findNewIndex(position)];
            }
        }
    }

    private void unpackSerializedProperties(ITypeDesc typeDesc) {
        try {
            this.serializedProperties = typeDesc.getClassBinaryStorageAdapter().fromBinary(typeDesc, serialized);
            isDeserialized = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }



    public HybridBinaryData clone(){
        return new HybridBinaryData(serializedProperties.clone(), nonSerializedProperties.clone(), serialized.clone());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if(isDeserialized){
            throw new IllegalStateException("trying to write external HybridBinaryData but isDeserialized = true");
        } else {
            IOUtils.writeObjectArrayCompressed(out, nonSerializedProperties);
            IOUtils.writeInt(out, serializedProperties.length);
            IOUtils.writeByteArray(out, serialized);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nonSerializedProperties = IOUtils.readObjectArrayCompressed(in);
        serializedProperties = new Object[IOUtils.readInt(in)];
        serialized = IOUtils.readByteArray(in);
        isDeserialized = false;
    }
}
