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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
// NOTE: SmartExternalizable is not used here since explain plan relies on reference identity
public class IndexInfo implements Externalizable {
    private static final long serialVersionUID = -6754173754999682054L;
    private String name;
    private Integer size;
    private SpaceIndexType type;
    private Object value;
    private QueryOperator operator;
    private boolean usable;

    public IndexInfo() {
    }

    public IndexInfo(String name, Integer size, SpaceIndexType type, Object value, QueryOperator operator) {
        this.name = name;
        this.size = size;
        this.type = type;
        this.value = value;
        this.operator= operator;
        if(type.isOrdered()){
            this.size = -1;
        }
        this.usable =true;
    }

    public IndexInfo(String name, Integer size, SpaceIndexType type, Object value, QueryOperator operator, boolean usable) {
        this.name = name;
        this.size = size;
        this.type = type;
        this.value = value;
        this.operator = operator;
        this.usable = usable;
    }

    public IndexInfo(String name) {
    }

    public QueryOperator getOperator() {
        return operator;
    }

    public void setOperator(QueryOperator operator) {
        this.operator = operator;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getSize() {
        return size;
    }

    protected String getSizeDesc() {
        return  size == null || size == -1 ? "unknown" : String.valueOf(size);
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public void setType(SpaceIndexType type) {
        this.type = type;
    }

    public SpaceIndexType getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public boolean isUsable() {
        return usable;
    }

    public void setUsable(boolean usable) {
        this.usable = usable;
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        IOUtils.writeString(objectOutput, this.name);
        objectOutput.writeObject(this.size);
        objectOutput.writeObject(this.type);
        objectOutput.writeObject(this.value);
        objectOutput.writeObject(this.operator);
        objectOutput.writeBoolean(this.usable);
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        this.name = IOUtils.readString(objectInput);
        this.size = (Integer) objectInput.readObject();
        this.type = (SpaceIndexType) objectInput.readObject();
        this.value = objectInput.readObject();
        this.operator = (QueryOperator) objectInput.readObject();
        this.usable = objectInput.readBoolean();
    }

    @Override
    public String toString() {
        return getCriteriaDesc() +
               ", size=" + getSizeDesc() +
               ", type=" + getType() +
               (isUsable() ? "" : ", UNUSABLE");
    }


    protected String getCriteriaDesc() {
        return operator + "(" + name + ", " + value + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexInfo indexInfo = (IndexInfo) o;

        if (name != null ? !name.equals(indexInfo.name) : indexInfo.name != null) return false;
        if (size != null ? !size.equals(indexInfo.size) : indexInfo.size != null) return false;
        if (type != indexInfo.type) return false;
        if (value != null ? !value.equals(indexInfo.value) : indexInfo.value != null) return false;
        return operator == indexInfo.operator;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (size != null ? size.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (operator != null ? operator.hashCode() : 0);
        return result;
    }
}
