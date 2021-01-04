package com.gigaspaces.client.storage_adapters.class_storage_adapters;

import com.gigaspaces.internal.io.GSByteArrayInputStream;
import com.gigaspaces.internal.io.GSByteArrayOutputStream;

import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.metadata.TypeDesc;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

import java.io.*;
import java.util.Map;

public class SelectiveClassBinaryStorageAdapter extends ClassBinaryStorageAdapter {

    private static final int POSITION_BYTES = 2; // short == 2 bytes

    @Override
    public byte[] toBinary(SpaceTypeDescriptor typeDescriptor, Object[] fields) throws IOException {
        try (GSByteArrayOutputStream bos = new GSByteArrayOutputStream();GSObjectOutputStream out = new GSObjectOutputStream(bos)) {
            int numOfFields = fields.length;
            short[] positions = new short[numOfFields];

            for (int l = 0; l < numOfFields; ++l) {
                positions[l] = 0;
                out.writeShort(positions[l]);
            }

            for (int i = 0; i < numOfFields; ++i) {
                PropertyInfo propertyInfo = ((TypeDesc)typeDescriptor).getSerializedProperties()[i];
                if (hasValue(propertyInfo, fields[i])) {
                    int count = bos.getCount();
                    if (count > Short.MAX_VALUE){
                        throw new IOException("Property [" + propertyInfo.getType().getName() + "] overflows serialized buffer. Position is [" + count + "]");
                    }
                    positions[i] = (short) count;
                    serialize(out, propertyInfo, fields[i]);
                } else {
                    positions[i] = -1;
                }
            }

            byte[] serializedFields = bos.toByteArray();
            bos.reset();
            for (int j = 0; j < numOfFields; ++j) {
                out.writeShort(positions[j]);
            }

            byte[] positionsByteMap = bos.toByteArray();
            System.arraycopy(positionsByteMap, 0, serializedFields, 1, numOfFields * POSITION_BYTES);

            return serializedFields;
        }
    }

    @Override
    public Object[] fromBinary(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields) throws IOException, ClassNotFoundException {
        try (GSByteArrayInputStream bis = new GSByteArrayInputStream(serializedFields); GSObjectInputStream in = new GSObjectInputStream(bis)) {
            int length = ((TypeDesc)typeDescriptor).getSerializedProperties().length;
            Object[] objects = new Object[length];
            for (int i = 0; i < length; ++i)
                objects[i] = getFieldAtIndex(typeDescriptor, bis, in, i);
            return objects;
        }
    }

    @Override
    public Object getFieldAtIndex(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields, int index) throws IOException, ClassNotFoundException {
        try (GSByteArrayInputStream bis = new GSByteArrayInputStream(serializedFields);GSObjectInputStream in = new GSObjectInputStream(bis)) {
            return getFieldAtIndex(typeDescriptor, bis, in, index);
        }
    }


    @Override
    public Object[] getFieldsAtIndexes(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields, int... indexes) throws IOException, ClassNotFoundException {
        try (GSByteArrayInputStream bis = new GSByteArrayInputStream(serializedFields); GSObjectInputStream in = new GSObjectInputStream(bis)) {
            int length = indexes.length;
            Object[] objects = new Object[length];
            for (int i = 0; i < length; ++i)
                objects[i] = getFieldAtIndex(typeDescriptor, bis, in, indexes[i]);
            return objects;
        }
    }

    protected Object getFieldAtIndex(SpaceTypeDescriptor typeDescriptor, GSByteArrayInputStream bis, GSObjectInputStream in , int index)
            throws IOException, ClassNotFoundException {
        bis.setPosition(index * POSITION_BYTES + 1);
        short position = in.readShort();

        PropertyInfo propertyInfo = ((TypeDesc)typeDescriptor).getSerializedProperties()[index];
        if (position == -1) {
            return getDefaultValue(propertyInfo);
        } else {
            bis.setPosition(position);
            return deserialize(in, propertyInfo);
        }
    }

    @Override
    public byte[] modifyField(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields, int index, Object newValue) throws IOException, ClassNotFoundException {
        Object[] objects = fromBinary(typeDescriptor, serializedFields);
        objects[index] = newValue;
        return toBinary(typeDescriptor, objects);
    }

    @Override
    public byte[] modifyFields(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields, Map<Integer, Object> newValues) throws IOException, ClassNotFoundException {
        Object[] objects = fromBinary(typeDescriptor, serializedFields);
        for (Map.Entry<Integer,Object> entry : newValues.entrySet()){
            objects[entry.getKey()] = entry.getValue();
        }
        return toBinary(typeDescriptor, objects);
    }

    @Override
    public boolean isDirectFieldAccessOptimized() {
        return true;
    }
}
