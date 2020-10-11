package com.gigaspaces.client.storage_adapters.class_storage_adapters;

import com.gigaspaces.internal.io.IOUtils;

import java.io.*;

public class SerializedStorageAdapter extends ClassBinaryStorageAdapter {

    @Override
    public byte[] toBinary(Object[] fields) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(bos)) {
            IOUtils.writeObjectArrayCompressed(out, fields);
            out.flush();
            return bos.toByteArray();
        }
    }

    @Override
    public Object[] fromBinary(byte[] serializedFields) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serializedFields); ObjectInput in = new ObjectInputStream(bis)) {
            return IOUtils.readObjectArrayCompressed(in);
        }
    }

    @Override
    public Object getFieldAtIndex(byte[] serializedFields, int index) throws IOException, ClassNotFoundException {
        return fromBinary(serializedFields)[index];
    }

    @Override
    public Object[] getFieldsAtIndexes(byte[] serializedFields, int... indexes) throws IOException, ClassNotFoundException {
        Object[] result = new Object[indexes.length];
        Object[] fields = fromBinary(serializedFields);
        int i = 0;
        for (int index : indexes) {
            result[i++] = fields[index];
        }
        return result;
    }

    @Override
    public byte[] modifyField(byte[] serializedFields, int index, Object newValue) throws IOException, ClassNotFoundException {
        Object[] fields = fromBinary(serializedFields);
        fields[index] = newValue;
        return toBinary(fields);
    }
}
