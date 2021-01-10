package com.gigaspaces.internal.server.storage;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;

import java.io.*;
import java.util.Arrays;

public class HybridPropertiesHolder implements Externalizable {
    private static final Object[] EMPTY_OBJECTS_ARRAY = new Object[0];
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private Object[] unpackedSerializedProperties;
    private Object[] nonSerializedProperties;
    private byte[] packedSerializedProperties;
    private boolean dirty;
    private boolean unpacked;

    public HybridPropertiesHolder() {
        this.unpackedSerializedProperties = EMPTY_OBJECTS_ARRAY;
        this.nonSerializedProperties = EMPTY_OBJECTS_ARRAY;
        this.packedSerializedProperties = EMPTY_BYTE_ARRAY;
        this.unpacked = true;
    }

    public HybridPropertiesHolder(ITypeDesc typeDesc, Object[] nonSerializedProperties, byte[] packedSerializedProperties) {
        this.nonSerializedProperties = nonSerializedProperties;
        this.unpackedSerializedProperties = new Object[typeDesc.getSerializedProperties().length];
        this.packedSerializedProperties = packedSerializedProperties;
    }

    public HybridPropertiesHolder(Object[] unpackedSerializedProperties, Object[] nonSerializedProperties, byte[] packedSerializedProperties, boolean unpacked, boolean dirty) {
        this.unpackedSerializedProperties = unpackedSerializedProperties;
        this.nonSerializedProperties = nonSerializedProperties;
        this.packedSerializedProperties = packedSerializedProperties;
        this.unpacked = unpacked;
        this.dirty = dirty;
    }

    public HybridPropertiesHolder(ITypeDesc typeDesc, Object[] values) {
        splitProperties(typeDesc, values);
        this.packedSerializedProperties = serializeFields(typeDesc, this.unpackedSerializedProperties);
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
            throw new UncheckedIOException(HybridPropertiesHolder.class.getSimpleName() + ": failed to serialized fields " + Arrays.toString(fieldsValues), e);
        }
    }

    public Object[] getFixedProperties(ITypeDesc typeDesc) {
        if (!unpacked && this.packedSerializedProperties.length != 0) {
            unpackSerializedProperties(typeDesc);
        }

        Object[] fields = new Object[typeDesc.getProperties().length];
        int[] optimizedPositions = typeDesc.getPositionsForSplitting();
        for (int i = 0; i < optimizedPositions.length; i++) {
            if (optimizedPositions[i] < 0) {
                fields[i] = this.unpackedSerializedProperties[(optimizedPositions[i] * -1) - 1];
            } else {
                fields[i] = this.nonSerializedProperties[optimizedPositions[i] - 1];
            }
        }
        return fields;

    }

    public Object getFixedProperty(ITypeDesc typeDesc, int position) {
        int[] optimizedPositions = typeDesc.getPositionsForSplitting();
        if (optimizedPositions[position] < 0) {
            if (!unpacked) {
                unpackSerializedProperties(typeDesc);
            }
            return unpackedSerializedProperties[(optimizedPositions[position] * -1) - 1];
        } else {
            return nonSerializedProperties[optimizedPositions[position] - 1];
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

    @Override
    public HybridPropertiesHolder clone() {
        Object[] serializedProps = unpackedSerializedProperties.length == 0 ? unpackedSerializedProperties : Arrays.copyOf(unpackedSerializedProperties, unpackedSerializedProperties.length);
        Object[] nonSerializedProps = nonSerializedProperties.length == 0 ? nonSerializedProperties : Arrays.copyOf(nonSerializedProperties, nonSerializedProperties.length);
        byte[] packedBinaryProps = packedSerializedProperties.length == 0 ? packedSerializedProperties : Arrays.copyOf(packedSerializedProperties, packedSerializedProperties.length);
        return new HybridPropertiesHolder(serializedProps, nonSerializedProps, packedBinaryProps, this.unpacked, this.dirty);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(dirty);
        if (dirty) {
            IOUtils.writeObjectArrayCompressed(out, nonSerializedProperties);
            IOUtils.writeObjectArrayCompressed(out, unpackedSerializedProperties);
        } else {
            IOUtils.writeObjectArrayCompressed(out, nonSerializedProperties);
            IOUtils.writeInt(out, unpackedSerializedProperties.length);
            out.writeInt(packedSerializedProperties.length);
            if (packedSerializedProperties.length != 0) {
                out.write(packedSerializedProperties);
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.dirty = in.readBoolean();
        if (dirty) {
            nonSerializedProperties = IOUtils.readObjectArrayCompressed(in);
            unpackedSerializedProperties = IOUtils.readObjectArrayCompressed(in);
            this.packedSerializedProperties = EMPTY_BYTE_ARRAY;
            this.unpacked = true;
        } else {
            nonSerializedProperties = IOUtils.readObjectArrayCompressed(in);
            int unpackedSize = IOUtils.readInt(in);
            if (unpackedSize == 0) {
                unpackedSerializedProperties = EMPTY_OBJECTS_ARRAY;
            } else {
                unpackedSerializedProperties = new Object[unpackedSize];
            }
            int packedSize = in.readInt();
            if (packedSize == 0) {
                packedSerializedProperties = EMPTY_BYTE_ARRAY;
            } else {
                packedSerializedProperties = new byte[packedSize];
                in.readFully(packedSerializedProperties);
            }
            this.unpacked = false;
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
        return this.unpackedSerializedProperties.length == 0
                && this.packedSerializedProperties.length == 0
                && this.nonSerializedProperties.length == 0;
    }


    public void copyFieldsArray() {
        Object[] src = nonSerializedProperties;
        Object[] target;
        if (src.length != 0) {
            target = new Object[src.length];
            System.arraycopy(src, 0, target, 0, src.length);
            this.nonSerializedProperties = target;
        }
        if (unpacked && unpackedSerializedProperties.length != 0) {
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
        int[] optimizedPositions = typeDesc.getPositionsForSplitting();
        if (optimizedPositions[position] < 0) {
            if (!unpacked) {
                unpackSerializedProperties(typeDesc);
            }
            unpackedSerializedProperties[(optimizedPositions[position] * -1) - 1] = value;
            dirty = true;
        } else {
            nonSerializedProperties[optimizedPositions[position] - 1] = value;
        }
    }

    private void splitProperties(ITypeDesc typeDesc, Object[] values) {
        if (typeDesc.getSerializedProperties().length == 0) {
            this.nonSerializedProperties = values == null ? EMPTY_OBJECTS_ARRAY : values;
            this.unpackedSerializedProperties = EMPTY_OBJECTS_ARRAY;
            return;
        }

        this.nonSerializedProperties = nonSerializedProperties == null ? new Object[typeDesc.getNonSerializedProperties().length] : nonSerializedProperties;
        this.unpackedSerializedProperties = unpackedSerializedProperties == null ? new Object[typeDesc.getSerializedProperties().length] : unpackedSerializedProperties;
        int[] optimizedPositions = typeDesc.getPositionsForSplitting();
        if (values != null && values.length != 0) {
            for (int i = 0; i < optimizedPositions.length; i++) {
                if (optimizedPositions[i] < 0) {
                    this.unpackedSerializedProperties[(optimizedPositions[i] * -1) - 1] = values[i];
                } else {
                    this.nonSerializedProperties[optimizedPositions[i] - 1] = values[i];
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

    public void setNonSerialized(Object[] nonSerialized) {
        this.nonSerializedProperties = nonSerialized;
    }


    public void setSerialized(Object[] serialized) {
        this.unpackedSerializedProperties = serialized;
        this.unpacked = true;
        this.dirty = true;
    }
}
