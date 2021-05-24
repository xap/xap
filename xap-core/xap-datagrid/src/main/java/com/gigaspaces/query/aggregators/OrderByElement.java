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
import com.gigaspaces.internal.query.RawEntry;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Sagiv Michael
 * @since 16.0.0
 */

public class OrderByElement implements Externalizable {

    private RawEntry rawEntry;
    private Object[] orderByValues;

    public OrderByElement(List<OrderByPath> orderByPaths, SpaceEntriesAggregatorContext context) {
        this.rawEntry = context.getRawEntry();
        this.orderByValues = new Object[orderByPaths.size()];
        for (int i = 0; i < orderByPaths.size(); i++) {
            this.orderByValues[i] = context.getPathValue(orderByPaths.get(i).getPath());
        }
    }

    public OrderByElement() {
    }

    public RawEntry getRawEntry() {
        return rawEntry;
    }

    public Object getValue(int index) {
        return this.orderByValues[index];
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, rawEntry);
        IOUtils.writeObjectArray(out, orderByValues);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.rawEntry = (RawEntry) IOUtils.readObject(in);
        this.orderByValues = IOUtils.readObjectArray(in);
    }
}
