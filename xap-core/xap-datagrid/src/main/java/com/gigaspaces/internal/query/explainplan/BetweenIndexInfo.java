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
package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.metadata.index.SpaceIndexType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
public class BetweenIndexInfo extends IndexInfo {
    private static final long serialVersionUID = -4360120624992897015L;
    private Comparable min;
    private Comparable max;
    private boolean includeMin;
    private boolean includeMax;

    public BetweenIndexInfo () {}

    public BetweenIndexInfo(String name, Integer size, SpaceIndexType type, Comparable min, boolean includeMin, Comparable max, boolean includeMax, QueryOperator operator, boolean b) {
        super(name, size, type, null, operator);
        this.max = max;
        this.min = min;
        this.includeMax = includeMax;
        this.includeMin = includeMin;
        setUsable(b);
    }

    public Comparable getMin() {
        return min;
    }

    public void setMin(Comparable min) {
        this.min = min;
    }

    public Comparable getMax() {
        return max;
    }

    public void setMax(Comparable max) {
        this.max = max;
    }

    public boolean isIncludeMin() {
        return includeMin;
    }

    public void setIncludeMin(boolean includeMin) {
        this.includeMin = includeMin;
    }

    public boolean isIncludeMax() {
        return includeMax;
    }

    public void setIncludeMax(boolean includeMax) {
        this.includeMax = includeMax;
    }

    @Override
    protected String getCriteriaDesc() {
        return getName() + " BETWEEN " + min + " AND " + max;
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        IOUtils.writeString(objectOutput, getName());
        objectOutput.writeInt(getSize());
        objectOutput.writeObject(getType());
        objectOutput.writeObject(getOperator());
        objectOutput.writeObject(min);
        objectOutput.writeBoolean(includeMin);
        objectOutput.writeObject(max);
        objectOutput.writeBoolean(includeMax);
        objectOutput.writeBoolean(isUsable());
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        setName(IOUtils.readString(objectInput));
        setSize(objectInput.readInt());
        setType((SpaceIndexType) objectInput.readObject());
        setOperator((QueryOperator) objectInput.readObject());
        min = (Comparable) objectInput.readObject();
        includeMin = objectInput.readBoolean();
        max = (Comparable) objectInput.readObject();
        includeMax = objectInput.readBoolean();
        setUsable(objectInput.readBoolean());
    }


}

