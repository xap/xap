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
package com.gigaspaces.internal.serialization.primitives;

import com.gigaspaces.internal.serialization.IClassSerializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class BytePrimitiveClassSerializer  implements IClassSerializer<Byte> {
    private static final Byte DEFAULT_VALUE = 0;

    public static final BytePrimitiveClassSerializer instance = new BytePrimitiveClassSerializer();

    private BytePrimitiveClassSerializer() {
    }

    public byte getCode() {
        return CODE_BYTE;
    }

    public Byte read(ObjectInput in)
            throws IOException, ClassNotFoundException {
        return in.readByte();
    }

    public void write(ObjectOutput out, Byte obj)
            throws IOException {
        out.writeByte(obj.byteValue());
    }

    @Override
    public Byte getDefaultValue() {
        return DEFAULT_VALUE;
    }
}
