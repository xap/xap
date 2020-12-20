package com.gigaspaces.internal.server.storage;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;

import java.io.*;
import java.util.Arrays;

public class HybridPayload implements Externalizable {
    private static final Object[] EMPTY_OBJECTS_ARRAY = new Object[0];
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private Object[] unpackedSerializedProperties;
    private Object[] nonSerializedProperties;
    private byte[] packedSerializedProperties;
    private boolean dirty;
    private boolean unpacked;

    public HybridPayload() {
        this.unpackedSerializedProperties = EMPTY_OBJECTS_ARRAY;
        this.nonSerializedProperties = EMPTY_OBJECTS_ARRAY;
        this.packedSerializedProperties = EMPTY_BYTE_ARRAY;
        this.unpacked = true;
    }

    public HybridPayload(ITypeDesc typeDesc, Object[] nonSerializedProperties, byte[] packedSerializedProperties) {
        this.nonSerializedProperties = nonSerializedProperties;
        this.unpackedSerializedProperties = new Object[typeDesc.getSerializedProperties().length];
        this.packedSerializedProperties = packedSerializedProperties;
        this.unpacked = false;
        this.dirty = false;
    }

    public HybridPayload(Object[] unpackedSerializedProperties, Object[] nonSerializedProperties, byte[] packedSerializedProperties, boolean unpacked, boolean dirty) {
        this.unpackedSerializedProperties = unpackedSerializedProperties;
        this.nonSerializedProperties = nonSerializedProperties;
        this.packedSerializedProperties = packedSerializedProperties;
        this.unpacked = unpacked;
        this.dirty = dirty;
    }


    //wrap an object array with HybridBinaryData
    public HybridPayload(ITypeDesc typeDesc, Object[] values) {
        splitProperties(typeDesc, values);
        this.packedSerializedProperties = typeDesc.getClassBinaryStorageAdapter() != null ?
                serializeFields(typeDesc, this.unpackedSerializedProperties) : EMPTY_BYTE_ARRAY;
        this.unpacked = true;
        this.dirty = false;
    }

    static byte[] serializeFields(ITypeDesc typeDesc, Object[] fieldsValues) {
        try {
            if (fieldsValues == null || fieldsValues.length == 0) {
                return EMPTY_BYTE_ARRAY;
            }
            return typeDesc.getClassBinaryStorageAdapter().toBinary(typeDesc, fieldsValues);
        } catch (IOException e) {
            throw new UncheckedIOException("com.gigaspaces.internal.server.storage.HybridPayload.serializeFields failed", e);
        }
    }

    public Object[] getFixedProperties(ITypeDesc typeDesc) {
        if (unpacked && this.nonSerializedProperties.length == 0 && this.unpackedSerializedProperties.length == 0) {
            return this.nonSerializedProperties;
        }

        if (!unpacked && this.packedSerializedProperties.length > 0) {
            unpackSerializedProperties(typeDesc);
        }

        if (unpackedSerializedProperties.length == 0) {
            return this.nonSerializedProperties;
        }

        Object[] fields = new Object[typeDesc.getProperties().length];
        int i = 0;
        for (PropertyInfo property : typeDesc.getProperties()) {
            if (property.isBinarySpaceProperty(typeDesc)) {
                fields[i] = this.unpackedSerializedProperties[typeDesc.findHybridIndex(i)];
            } else {
                fields[i] = this.nonSerializedProperties[typeDesc.findHybridIndex(i)];
            }
            i++;
        }
        return fields;

    }

    public Object getFixedProperty(ITypeDesc typeDesc, int position) {
        if (typeDesc.isSerializedProperty(position)) {
            if (!unpacked) {
                unpackSerializedProperties(typeDesc);
            }
            return unpackedSerializedProperties[typeDesc.findHybridIndex(position)];
        } else {
            return nonSerializedProperties[typeDesc.findHybridIndex(position)];
        }
    }

    private void unpackSerializedProperties(ITypeDesc typeDesc) {
        try {
            this.unpackedSerializedProperties = typeDesc.getClassBinaryStorageAdapter().fromBinary(typeDesc, packedSerializedProperties);
            unpacked = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    public HybridPayload clone() {
        Object[] serializedProps = this.unpackedSerializedProperties == null ? null : Arrays.copyOf(this.unpackedSerializedProperties, this.unpackedSerializedProperties.length);
        Object[] nonSerializedProps = this.nonSerializedProperties == null ? null : Arrays.copyOf(this.nonSerializedProperties, this.nonSerializedProperties.length);
        byte[] packedBinaryProps = this.packedSerializedProperties == null ? null : Arrays.copyOf(this.packedSerializedProperties, this.packedSerializedProperties.length);
        return new HybridPayload(serializedProps, nonSerializedProps, packedBinaryProps, this.unpacked, this.dirty);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(unpacked);
        out.writeBoolean(dirty);
        if (unpacked && dirty) {
            IOUtils.writeObjectArrayCompressed(out, nonSerializedProperties);
            IOUtils.writeObjectArrayCompressed(out, unpackedSerializedProperties);
        } else {
            IOUtils.writeObjectArrayCompressed(out, nonSerializedProperties);
            IOUtils.writeInt(out, unpackedSerializedProperties.length);
            out.writeInt(packedSerializedProperties.length);
            if (packedSerializedProperties.length > 0) {
                out.write(packedSerializedProperties);
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        unpacked = in.readBoolean();
        dirty = in.readBoolean();
        if (unpacked && dirty) {
            nonSerializedProperties = IOUtils.readObjectArrayCompressed(in);
            unpackedSerializedProperties = IOUtils.readObjectArrayCompressed(in);
            this.packedSerializedProperties = EMPTY_BYTE_ARRAY;
        } else {
            nonSerializedProperties = IOUtils.readObjectArrayCompressed(in);
            int unpackedSize = IOUtils.readInt(in);
            if (unpackedSize == 0) {
                unpackedSerializedProperties = EMPTY_OBJECTS_ARRAY;
            } else {
                unpackedSerializedProperties = new Object[unpackedSize];
            }
            int packedSize = in.readInt();
            if(packedSize == 0){
                packedSerializedProperties = EMPTY_BYTE_ARRAY;
            } else {
                packedSerializedProperties = new byte[packedSize];
                in.readFully(packedSerializedProperties);
            }
            unpacked = false;
        }
    }

    boolean isUnpacked() {
        return unpacked;
    }

    public Object[] getNonSerializedProperties() {
        return nonSerializedProperties;
    }

    public byte[] getPackedSerializedProperties() {
        return packedSerializedProperties;
    }

    Object[] getUnpackedSerializedProperties() {
        return unpackedSerializedProperties;
    }

    @Override
    public String toString() {
        return "HybridBinaryData{" +
                "unpackedSerializedProperties=" + Arrays.toString(unpackedSerializedProperties) +
                ", nonSerializedProperties=" + Arrays.toString(nonSerializedProperties) +
                ", packedSerializedProperties=" + Arrays.toString(packedSerializedProperties) +
                ", unpacked=" + unpacked +
                ", dirty=" + dirty +
                '}';
    }

    public boolean allNulls() {
        return (this.unpackedSerializedProperties == null || this.unpackedSerializedProperties.length == 0)
                && (this.packedSerializedProperties == null || this.packedSerializedProperties.length == 0)
                && (this.nonSerializedProperties == null || this.nonSerializedProperties.length == 0);
    }

    public void copyFieldsArray() {
        Object[] src = nonSerializedProperties;
        Object[] target;
        if (src.length > 0) {
            target = new Object[src.length];
            System.arraycopy(src, 0, target, 0, src.length);
            this.nonSerializedProperties = target;
        }
        if (unpacked && unpackedSerializedProperties.length > 0) {
            src = unpackedSerializedProperties;
            target = new Object[src.length];
            System.arraycopy(src, 0, target, 0, src.length);
            this.unpackedSerializedProperties = target;
        }
    }

    public void setFixedProperties(ITypeDesc typeDescriptor, Object[] values) {
        splitProperties(typeDescriptor, values);
        this.unpacked = true;
        this.dirty = true;
    }

    public void setFixedProperty(ITypeDesc typeDesc, int position, Object value) {
        if (typeDesc.isSerializedProperty(position)) {
            if (!unpacked) {
                unpackSerializedProperties(typeDesc);
            }
            unpackedSerializedProperties[typeDesc.findHybridIndex(position)] = value;
            dirty = true;
        } else {
            nonSerializedProperties[typeDesc.findHybridIndex(position)] = value;
        }
    }

    private void splitProperties(ITypeDesc typeDesc, Object[] values) {
        if (typeDesc.getSerializedProperties().length == 0) {
            this.nonSerializedProperties = values == null ? EMPTY_OBJECTS_ARRAY : values;
            this.unpackedSerializedProperties = EMPTY_OBJECTS_ARRAY;
            return;
        }

        this.nonSerializedProperties = new Object[typeDesc.getNonSerializedProperties().length];
        this.unpackedSerializedProperties = new Object[typeDesc.getSerializedProperties().length];
        if (values != null && values.length > 0) {
            int nonSerializedFieldsIndex = 0;
            int serializedFieldsIndex = 0;
            for (PropertyInfo property : typeDesc.getProperties()) {
                if (property.getStorageType() != null && property.isBinarySpaceProperty(typeDesc)) {
                    this.unpackedSerializedProperties[serializedFieldsIndex] = values[typeDesc.getFixedPropertyPosition(property.getName())];
                    serializedFieldsIndex++;
                } else {
                    this.nonSerializedProperties[nonSerializedFieldsIndex] = values[typeDesc.getFixedPropertyPosition(property.getName())];
                    nonSerializedFieldsIndex++;
                }
            }
        }
    }

    public void setFixedProperties(Object[] values) {
        this.nonSerializedProperties = values;
        this.unpackedSerializedProperties = EMPTY_OBJECTS_ARRAY;
        this.unpacked = true;
        this.dirty = true;
    }

    public void setFixedProperty(int position, Object value) {
        this.nonSerializedProperties[position] = value;
    }

    public boolean isDirty() {
        return dirty;
    }
}
