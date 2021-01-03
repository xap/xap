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

package com.gigaspaces.internal.serialization;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 7.1
 */
public interface IClassSerializer<T> {
    byte getCode();

    void write(ObjectOutput out, T obj)
            throws IOException;

    T read(ObjectInput in)
            throws IOException, ClassNotFoundException;

    default T getDefaultValue(){
        return null;
    }

    public static final byte CODE_NULL = 0;

    public static final byte CODE_OBJECT = -1;
    public static final byte CODE_STRING = -2;

    public static final byte CODE_BYTE = -11;
    public static final byte CODE_SHORT = -12;
    public static final byte CODE_INTEGER = -13;
    public static final byte CODE_LONG = -14;
    public static final byte CODE_FLOAT = -15;
    public static final byte CODE_DOUBLE = -16;
    public static final byte CODE_BOOLEAN = -17;
    public static final byte CODE_CHARACTER = -18;

    public static final byte CODE_BYTE_ARRAY = -21;
    //public static final byte CODE_SHORT_ARRAY	= -22;
    //public static final byte CODE_INTEGER_ARRAY	= -23;
    //public static final byte CODE_LONG_ARRAY	= -24;
    //public static final byte CODE_FLOAT_ARRAY	= -25;
    //public static final byte CODE_DOUBLE_ARRAY	= -26;
    //public static final byte CODE_BOOLEAN_ARRAY	= -27;
    //public static final byte CODE_CHAR_ARRAY	= -28;

    public static final byte CODE_HASHMAP = 10;

}
