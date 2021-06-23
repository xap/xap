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
import java.util.ArrayList;
import java.util.List;

/**
 * @author Sagiv Michael
 * @since 16.0.0
 */

public class OrderByElement implements Externalizable {
    static final long serialVersionUID = 6212805774245544178L;
    private List<RawEntry> rawEntries = new ArrayList<>();
    private OrderByValues orderByValues;

    public OrderByElement(OrderByValues orderByValues) {
        this.orderByValues = orderByValues;
    }

    public OrderByElement() {
    }

    public void addRawEntry(RawEntry rawEntry) {
        this.rawEntries.add(rawEntry);
    }

    public void addRawEntries(List<RawEntry> rawEntries) {
        this.rawEntries.addAll(rawEntries);
    }

    public List<RawEntry> getRawEntries() {
        return rawEntries;
    }

    public Object getValue(int index) {
        return this.orderByValues.getValues()[index];
    }

    public OrderByValues getOrderByValues() {
        return this.orderByValues;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeList(out, rawEntries);
        IOUtils.writeObject(out, orderByValues);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.rawEntries = IOUtils.readList(in);
        this.orderByValues = IOUtils.readObject(in);
    }
}
