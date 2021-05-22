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

package org.openspaces.memcached;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Represents a given key for lookup in the cache.
 *
 * Wraps a byte array with a precomputed hashCode.
 */
public class Key implements SmartExternalizable {

    private static final long serialVersionUID = -1131139214094910487L;

    public byte[] bytes;

    private transient int hashCode;

    public Key() {
    }

    public Key(byte[] bytes) {
        this.bytes = bytes;
        this.hashCode = Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        Key key1 = (Key) o;
        return Arrays.equals(bytes, key1.bytes);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private static final ThreadLocal<StringBuilder> sbCache = new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
            return new StringBuilder();
        }
    };

    @Override
    public String toString() {
        StringBuilder sb = sbCache.get();
        sb.setLength(0);
        for (byte b : bytes) {
            sb.append(b);
        }
        return sb.toString();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        bytes = new byte[in.readInt()];
        in.readFully(bytes);
        this.hashCode = Arrays.hashCode(bytes);
    }
}
