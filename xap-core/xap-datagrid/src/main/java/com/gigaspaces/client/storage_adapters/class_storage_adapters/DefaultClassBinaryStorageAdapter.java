package com.gigaspaces.client.storage_adapters.class_storage_adapters;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

import java.io.*;
import java.util.Map;

public class DefaultClassBinaryStorageAdapter extends ClassBinaryStorageAdapter {

    @Override
    public byte[] toBinary(SpaceTypeDescriptor typeDescriptor, Object[] fields) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(bos)) {
            IOUtils.writeObjectArrayCompressed(out, fields);
            out.flush();
            return bos.toByteArray();
        }
    }

    @Override
    public Object[] fromBinary(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serializedFields); ObjectInput in = new ObjectInputStream(bis)) {
            return IOUtils.readObjectArrayCompressed(in);
        }
    }

    @Override
    public Object getFieldAtIndex(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields, int index) throws IOException, ClassNotFoundException {
        return fromBinary(typeDescriptor, serializedFields)[index];
    }

    @Override
    public Object[] getFieldsAtIndexes(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields, int... indexes) throws IOException, ClassNotFoundException {
        Object[] result = new Object[indexes.length];
        Object[] fields = fromBinary(typeDescriptor, serializedFields);
        int i = 0;
        for (int index : indexes) {
            result[i++] = fields[index];
        }
        return result;
    }

    @Override
    public byte[] modifyField(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields, int index, Object newValue) throws IOException, ClassNotFoundException {
        Object[] fields = fromBinary(typeDescriptor, serializedFields);
        fields[index] = newValue;
        return toBinary(typeDescriptor, fields);
    }

    @Override
    public byte[] modifyFields(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields, Map<Integer, Object> newValues) throws IOException, ClassNotFoundException {
        Object[] fields = fromBinary(typeDescriptor, serializedFields);
        for (Map.Entry<Integer, Object> entry : newValues.entrySet()) {
            fields[entry.getKey()] = entry.getValue();
        }
        return toBinary(typeDescriptor, fields);
    }

    @Override
    public boolean isDirectFieldAccessOptimized() {
        return false;
    }
}
