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

import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class MutableBigInteger extends MutableNumber {

    private static final long serialVersionUID = 1L;

    private BigInteger value;

    @Override
    public void add(Number x) {
        if (x == null)
            return;
        value = value == null ? (BigInteger) x : value.add((BigInteger) x);
    }

    @Override
    public void remove(Number x) {
        value = value == null ? (BigInteger) x : value.add(((BigInteger) x).negate());
    }

    @Override
    public Number calcDivision(long count) {
        return new BigDecimal(value).divide(BigDecimal.valueOf(count));
    }

    @Override
    public Number toNumber() {
        return value;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, value);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.value = IOUtils.readObject(in);
    }
}
