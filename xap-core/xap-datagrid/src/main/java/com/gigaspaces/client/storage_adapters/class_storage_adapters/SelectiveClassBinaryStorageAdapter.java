package com.gigaspaces.client.storage_adapters.class_storage_adapters;

import com.gigaspaces.internal.io.GSByteArrayInputStream;
import com.gigaspaces.internal.io.GSByteArrayOutputStream;
import com.gigaspaces.internal.io.IOUtils;

import com.gigaspaces.metadata.SpaceTypeDescriptor;


import java.io.*;
import java.util.Arrays;
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

            print(bos.toByteArray(), bos.getCount());

            for (int i = 0; i < numOfFields; ++i) {
                if (fields[i] == null) {
                    positions[i] = -1;
                } else {
                    positions[i] = (short) bos.getCount();
                    IOUtils.getIClassSerializer(typeDescriptor.getFixedProperty(i).getType()).write(out, fields[i]);
                }
            }

            print(bos.toByteArray(), bos.getCount());

            GSByteArrayOutputStream positionsStream = new GSByteArrayOutputStream();
            GSObjectOutputStream positionOutput = new GSObjectOutputStream(positionsStream);

            for (int j = 0; j < numOfFields; ++j) {
                IOUtils.getIClassSerializer(Short.class).write(positionOutput, positions[j]);
            }

            byte[] positionsArr = positionsStream.toByteArray();

            byte[] buffArr = bos.getBuffer();

            for (int m = 0; m < positionsArr.length; ++m) { //todo- more efficient- reset?
                buffArr[m] = positionsArr[m];
            }

            print(bos.toByteArray(), bos.getCount());
            return bos.toByteArray();
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

            print(bis.getBuffer(), bis.getPosition());
            Object[] obj = new Object[numOfFields];
            for (int i = 0; i < numOfFields; ++i){
                if (positions[i] == -1){
                    obj[i] = null;
                } else {
                    obj[i] = IOUtils.getIClassSerializer(typeDescriptor.getFixedProperty(i).getType()).read(in);
                }
            }

            print(bis.getBuffer(), bis.getPosition());
            return obj;
        }
    }


    @Override
    public Object getFieldAtIndex(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields, int index) throws IOException, ClassNotFoundException {
        try (GSByteArrayInputStream bis = new GSByteArrayInputStream(serializedFields);GSObjectInputStream in = new GSObjectInputStream(bis)) {
           bis.skip(index * 2); //todo- skip bytes of in?
           short position = (short) IOUtils.getIClassSerializer(Short.class).read(in);
           print(bis.getBuffer(), bis.getPosition());
           if (position == -1){
               return null;
           }
           print(bis.getBuffer(), bis.getPosition());

           bis.setPosition(position);
           return IOUtils.getIClassSerializer(typeDescriptor.getFixedProperty(index).getType()).read(in);
        }
    }

    @Override
    public Object[] getFieldsAtIndexes(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields, int... indexes) throws IOException, ClassNotFoundException {
        Object[] objects = new Object[indexes.length];

        for (int i = 0; i < indexes.length; ++i){
            objects[i] = getFieldAtIndex(typeDescriptor, serializedFields, i); //todo - can be more efficient with loop during the iterating on the stream
        }
        return objects;
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

    private void print(byte[] arr, int pos){
        System.out.println("Pos is: " + pos + ".  Byte arr is: " + Arrays.toString(arr));
    }
}
