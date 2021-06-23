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


package com.gigaspaces.query.aggregators;

import com.gigaspaces.internal.io.IOUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * @author Sagiv Michael
 * @since 16.0.0
 */

public class OrderByValues implements Externalizable{
    static final long serialVersionUID = 4898114069437778368L;
    private Object[] values;

    public OrderByValues() {
    }

    public OrderByValues(Object[] values) {
        this.values = values;
    }

    public Object[] getValues() {
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OrderByValues)) return false;
        OrderByValues that = (OrderByValues) o;
        return Arrays.equals(getValues(), that.getValues());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getValues());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObjectArray(out, values);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.values = IOUtils.readObjectArray(in);

    }
}
