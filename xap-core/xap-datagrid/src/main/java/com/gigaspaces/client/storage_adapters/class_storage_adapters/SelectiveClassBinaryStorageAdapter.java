package com.gigaspaces.client.storage_adapters.class_storage_adapters;

import com.gigaspaces.internal.io.GSByteArrayInputStream;
import com.gigaspaces.internal.io.GSByteArrayOutputStream;
import com.gigaspaces.internal.io.IOUtils;

import com.gigaspaces.metadata.SpaceTypeDescriptor;


import java.io.*;
import java.util.Map;


public class SelectiveClassBinaryStorageAdapter extends ClassBinaryStorageAdapter {

    @Override
    public byte[] toBinary(SpaceTypeDescriptor typeDescriptor, Object[] fields) throws IOException {
        try (GSByteArrayOutputStream bos = new GSByteArrayOutputStream();GSObjectOutputStream out = new GSObjectOutputStream(bos)) {
            int numOfFields = fields.length;
            short[] positions = new short[numOfFields];

            for (int l = 0; l < numOfFields; ++l) {
                positions[l] = 0;
                IOUtils.getIClassSerializer(Short.class).write(out, positions[l]);
            }

            for (int i = 0; i < numOfFields; ++i) {
                if (fields[i] == null) {
                    positions[i] = -1;
                } else {
                    int count = bos.getCount();
                    if (count > Short.MAX_VALUE){
                        throw new IOException("Property [" + typeDescriptor.getFixedProperty(i).getType().getName() + "] overflows serialized buffer. Position is [" + count + "]");
                    }
                    positions[i] = (short) count;
                    IOUtils.getIClassSerializer(typeDescriptor.getFixedProperty(i).getType()).write(out, fields[i]);
                }
            }

            byte[] serializedFields = bos.toByteArray();
            bos.reset();
            for (int j = 0; j < numOfFields; ++j) {
                IOUtils.getIClassSerializer(Short.class).write(out, positions[j]);
            }

            byte[] positionsByteMap = bos.toByteArray();
            System.arraycopy(positionsByteMap, 0, serializedFields, 1, numOfFields * 2);

            return serializedFields;
        }
    }

    @Override
    public Object[] fromBinary(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields) throws IOException, ClassNotFoundException {
        try (GSByteArrayInputStream bis = new GSByteArrayInputStream(serializedFields); GSObjectInputStream in = new GSObjectInputStream(bis)) {
            int numOfFields = typeDescriptor.getNumOfFixedProperties();
            short[] positions = new short[numOfFields];

            for(int i = 0; i < numOfFields; ++i){
                positions[i] = (short)IOUtils.getIClassSerializer(Short.class).read(in);
            }

            Object[] obj = new Object[numOfFields];
            for (int i = 0; i < numOfFields; ++i){
                if (positions[i] == -1){
                    obj[i] = null;
                } else {
                    obj[i] = IOUtils.getIClassSerializer(typeDescriptor.getFixedProperty(i).getType()).read(in);
                }
            }
            return obj;
        }
    }


    @Override
    public Object getFieldAtIndex(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields, int index) throws IOException, ClassNotFoundException {
        try (GSByteArrayInputStream bis = new GSByteArrayInputStream(serializedFields);GSObjectInputStream in = new GSObjectInputStream(bis)) {
           bis.skip(index * 2);
           short position = (short) IOUtils.getIClassSerializer(Short.class).read(in);
           if (position == -1){
               return null;
           }

           bis.setPosition(position);
           return IOUtils.getIClassSerializer(typeDescriptor.getFixedProperty(index).getType()).read(in);
        }
    }


    @Override
    public Object[] getFieldsAtIndexes(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields, int... indexes) throws IOException, ClassNotFoundException {
        try (GSByteArrayInputStream bis = new GSByteArrayInputStream(serializedFields); GSObjectInputStream in = new GSObjectInputStream(bis)) {
            short[] positions = new short[indexes.length];
            Object[] objects = new Object[indexes.length];

            for (int i = 0; i < indexes.length; ++i){
                bis.setPosition(indexes[i] * 2 + 1);
                positions[i] = (short) IOUtils.getIClassSerializer(Short.class).read(in);
            }

            for (int i = 0; i < indexes.length; ++i){
                if (positions[i] == -1){
                    objects[i] = null;
                } else {
                    bis.setPosition(positions[i]);
                    objects[i] = IOUtils.getIClassSerializer(typeDescriptor.getFixedProperty(indexes[i]).getType()).read(in);
                }
            }
            return objects;
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
