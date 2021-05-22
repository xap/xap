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

package com.gigaspaces.internal.io;

import com.gigaspaces.client.storage_adapters.BinaryWrapper;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * MarshObject is similar to java.rmi.MarshalledObject. However, it does not support codebased
 * annotations. MarshObject is Externelizable and thus should be transferred faster through RMI or
 * LRMI. MarshObject also has a public getBytes() method.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
@com.gigaspaces.api.InternalApi
public class MarshObject implements BinaryWrapper, SmartExternalizable {
    private static final long serialVersionUID = 1L;
    private static final int NULL_HASHCODE = 13;

    private byte[] _bytes;
    private int _hashCode;

    public MarshObject() {
    }

    public MarshObject(byte[] bytes) {
        _bytes = bytes;
        _hashCode = bytes != null ? hashCode(bytes) : NULL_HASHCODE;
    }

    @Override
    public byte[] getBytes() {
        return _bytes;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MarshObject) {
            MarshObject mo = (MarshObject) obj;
            return Arrays.equals(_bytes, mo._bytes);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return _hashCode;
    }

    public static int hashCode(byte[] bytes) {
        int h = 0;
        for (byte b : bytes)
            h = 31 * h + b;
        return h;
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        IOUtils.writeByteArray(out, _bytes);
        if (_bytes != null)
            out.writeInt(_hashCode);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        _bytes = IOUtils.readByteArray(in);
        if (_bytes != null)
            _hashCode = in.readInt();
        else
            _hashCode = NULL_HASHCODE;
    }

    @Override
    public String toString() {
        return "MarshObject [" + _bytes.length + " bytes]";
    }
}
