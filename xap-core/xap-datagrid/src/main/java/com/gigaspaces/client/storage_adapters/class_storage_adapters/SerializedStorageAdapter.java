/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
