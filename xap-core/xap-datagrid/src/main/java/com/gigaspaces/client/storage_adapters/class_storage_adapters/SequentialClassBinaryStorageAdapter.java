package com.gigaspaces.client.storage_adapters.class_storage_adapters;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.io.GSByteArrayInputStream;
import com.gigaspaces.internal.io.GSByteArrayOutputStream;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.metadata.TypeDesc;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

import java.io.*;
import java.util.Map;

@ExperimentalApi
public class SequentialClassBinaryStorageAdapter extends ClassBinaryStorageAdapter {
    private static final int HEADER_BYTES = 1;

    private static final byte VERSION = 1;

    @Override
    public byte[] toBinary(SpaceTypeDescriptor typeDescriptor, Object[] fields) throws IOException {
        try (GSByteArrayOutputStream bos = new GSByteArrayOutputStream(); GSObjectOutputStream out = new GSObjectOutputStream(bos)) {
            out.writeByte(VERSION);

            int numOfFields = fields.length;
            int modulo = numOfFields % 8 > 0 ? 1 : 0;
            byte[] bitMapNonDefaultFields = new byte[numOfFields / 8 + modulo];

            for (byte b : bitMapNonDefaultFields) {
                out.writeByte(b);
            }

            for (int i = 0; i < numOfFields; ++i) {
                PropertyInfo propertyInfo = ((TypeDesc)typeDescriptor).getSerializedProperties()[i];
                if (hasValue(propertyInfo, fields[i])) {
                    int byteIndex = i / 8;
                    int bitIndex = i % 8;
                    bitMapNonDefaultFields[byteIndex] |= (byte)1 << (7 - bitIndex); //set bit as 1 (non default field)
                    serialize(out, propertyInfo, fields[i]);
                }
            }

            for (int i = 0; i < bitMapNonDefaultFields.length; i++) {
                bos.writeByte(bitMapNonDefaultFields[i], i + HEADER_BYTES);
            }
            return bos.toByteArray();
        }
    }

    @Override
    public Object[] fromBinary(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields) throws IOException, ClassNotFoundException {
        try (GSByteArrayInputStream bis = new GSByteArrayInputStream(serializedFields); GSObjectInputStream in = new GSObjectInputStream(bis)) {
            byte version = in.readByte();
            if (version != VERSION)
                throw new IllegalStateException("Unsupported version: " + version);
            int numOfFields = ((TypeDesc)typeDescriptor).getSerializedProperties().length;
            int modulo = numOfFields % 8 > 0 ? 1 : 0;
            byte[] bitMapNonDefaultFields = new byte[numOfFields / 8 + modulo];

            for(int i = 0; i < bitMapNonDefaultFields.length; ++i){
                bitMapNonDefaultFields[i] = in.readByte();
            }

            Object[] objects = new Object[numOfFields];
            for (int i = 0; i < numOfFields; ++i){
                int byteIndex = i / 8;
                int bitIndex = i % 8;

                byte mask = (byte) ((byte)1 << (7 - bitIndex));
                byte result= (byte) (bitMapNonDefaultFields[byteIndex] & mask);

                PropertyInfo propertyInfo = ((TypeDesc)typeDescriptor).getSerializedProperties()[i];
                objects[i] = result == mask ? deserialize(in, propertyInfo) : getDefaultValue(propertyInfo);
            }
            return objects;
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
