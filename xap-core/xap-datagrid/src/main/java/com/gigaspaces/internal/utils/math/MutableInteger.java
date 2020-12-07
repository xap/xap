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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class MutableInteger extends MutableNumber {

    private static final long serialVersionUID = 1L;

    private int value;

    @Override
    public Number toNumber() {
        return Integer.valueOf(value);
    }

    @Override
    public void add(Number x) {
        if (x != null)
            value += x.intValue();
    }

    @Override
    public void remove(Number x) {
        if (x != null)
            value -= x.intValue();
    }

    @Override
    public Number cast(Class<?> newType) {
        if (newType.equals(Integer.class) || newType.equals(int.class)) return value;
        if (newType.equals(Float.class) || newType.equals(float.class)) return (float)value;
        if (newType.equals(Double.class) || newType.equals(double.class)) return (double)value;

        return super.cast(newType);
    }

    @Override
    public Number calcDivision(long count) {
        return (double) value / count;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(value);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.value = in.readInt();
    }
}
