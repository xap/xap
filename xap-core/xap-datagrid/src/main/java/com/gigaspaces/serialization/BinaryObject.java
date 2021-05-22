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


package com.gigaspaces.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Contains an object serialized in a binary format. This class is used internally to store user
 * defined objects in the space without reconstructing the original object.
 *
 * @author Niv
 * @version 1.0
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class BinaryObject implements SmartExternalizable {
    private static final long serialVersionUID = -7441531324541073950L;

    private byte formatCode;
    private byte[] data;

    private transient Integer hashCode;

    /**
     * Default constructor.
     */
    public BinaryObject() {
    }

    /**
     * Constructor with data.
     */
    public BinaryObject(byte formatCode, byte[] data) {
        this.formatCode = formatCode;
        this.data = data;
    }

    @Override
    public int hashCode() {
        if (hashCode == null)
            hashCode = Arrays.hashCode(data);

        return hashCode.intValue();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BinaryObject))
            return false;

        BinaryObject other = (BinaryObject) obj;

        if (formatCode != other.formatCode)
            return false;
        if (!Arrays.equals(data, other.data))
            return false;

        return true;
    }

    /**
     * Gets the serialization format code.
     */
    public byte getFormatCode() {
        return formatCode;
    }

    /**
     * Sets the serialization format code.
     */
    public void setFormatCode(byte value) {
        formatCode = value;
    }

    /**
     * Gets the object's binary data.
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Sets the object's binary data.
     */
    public void setData(byte[] value) {
        data = value;
        // Reset hash code:
        hashCode = null;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // Read the format code:
        formatCode = in.readByte();
        // Read the data:
        int length = in.readInt();
        data = new byte[length];
        in.readFully(data, 0, length);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        // Write the format code:
        out.writeByte(formatCode);
        // Write the data:
        out.writeInt(data.length);
        out.write(data);
    }
}