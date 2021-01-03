package com.gigaspaces.client.storage_adapters.class_storage_adapters;

import com.gigaspaces.internal.io.GSByteArrayInputStream;
import com.gigaspaces.internal.io.GSByteArrayOutputStream;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.metadata.TypeDesc;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

import java.io.*;
import java.util.Map;

public class DefaultClassBinaryStorageAdapter extends ClassBinaryStorageAdapter {

    @Override
    public byte[] toBinary(SpaceTypeDescriptor typeDescriptor, Object[] fields) throws IOException {
        try (GSByteArrayOutputStream bos = new GSByteArrayOutputStream(); GSObjectOutputStream out = new GSObjectOutputStream(bos)) {
            int numOfFields = fields.length;
            int modulo = numOfFields % 8 > 0 ? 1 : 0;
            byte[] bitMapNonDefaultFields = new byte[numOfFields / 8 + modulo];

            for (byte b : bitMapNonDefaultFields) {
                out.writeByte(b);
            }

            for (int i = 0; i < numOfFields; ++i) {
                PropertyInfo propertyInfo = ((TypeDesc)typeDescriptor).getSerializedProperties()[i];
                if(fields[i] != null && !fields[i].equals(propertyInfo.getClassSerializer().getDefaultValue())){
                    int byteIndex = i / 8;
                    int bitIndex = i % 8;
                    bitMapNonDefaultFields[byteIndex] |= (byte)1 << (7 - bitIndex); //set bit as 1 (non default field)
                    propertyInfo.getClassSerializer().write(out, fields[i]);
                }
            }

            byte[] serializedFields = bos.toByteArray();
            System.arraycopy(bitMapNonDefaultFields, 0, serializedFields, 1, bitMapNonDefaultFields.length);
            return serializedFields;
        }
    }

    @Override
    public Object[] fromBinary(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields) throws IOException, ClassNotFoundException {
        try (GSByteArrayInputStream bis = new GSByteArrayInputStream(serializedFields); GSObjectInputStream in = new GSObjectInputStream(bis)) {
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
                if (result == mask){ // field with no default value
                    objects[i] = propertyInfo.getClassSerializer().read(in);
                } else if (propertyInfo.isPrimitive()){
                    objects[i] = propertyInfo.getClassSerializer().getDefaultValue();
                }
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
