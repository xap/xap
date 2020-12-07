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

package com.gigaspaces.internal.utils.math;

import java.io.Externalizable;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
public abstract class MutableNumber implements Externalizable {

    private static final long serialVersionUID = 1L;

    public abstract Number toNumber();

    public abstract void add(Number x);

    public abstract void remove(Number x);

    public Number cast(Class<?> newType) {
        throw new UnsupportedOperationException("Not supported for " + this.getClass());
    }

    public abstract Number calcDivision(long count);

    public static MutableNumber fromClass(Class<?> type, boolean widest) {
        if (widest) {
            if (type == Byte.class || type == Short.class || type == Integer.class || type == Long.class)
                return new MutableLong();
            if (type == Float.class || type == Double.class)
                return new MutableDouble();
        } else {
            if (type == Byte.class) return new MutableByte();
            if (type == Short.class) return new MutableShort();
            if (type == Integer.class) return new MutableInteger();
            if (type == Long.class) return new MutableLong();
            if (type == Float.class) return new MutableFloat();
            if (type == Double.class) return new MutableDouble();
        }

        if (type == BigInteger.class) return new MutableBigInteger();
        if (type == BigDecimal.class) return new MutableBigDecimal();
        throw new IllegalArgumentException("Unsupported number type: " + type);
    }
}
