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


package com.gigaspaces.query;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
public abstract class CompoundResult implements SmartExternalizable, Cloneable {

    private static final long serialVersionUID = 1L;

    protected Object[] values;
    protected Map<String, Integer> nameIndexMap;
    protected transient int hashCode;

    /**
     * Required for Externalizable
     */
    public CompoundResult() {
    }

    public CompoundResult(Object[] values, Map<String, Integer> nameIndexMap) {
        this.values = values;
        this.nameIndexMap = nameIndexMap;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof CompoundResult))
            return false;
        CompoundResult other = (CompoundResult) obj;
        return Arrays.equals(this.values, other.values);
    }

    @Override
    public int hashCode() {
        if (hashCode == 0)
            hashCode = Arrays.hashCode(values);
        return hashCode;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName());
        printValues(sb, "(", ")");
        return sb.toString();
    }

    protected void printValues(StringBuilder sb, String prefix, String suffix) {
        if (prefix != null)
            sb.append(prefix);

        if (nameIndexMap != null) {
            boolean isFirst = true;
            for (Map.Entry<String, Integer> entry : nameIndexMap.entrySet()) {
                if (isFirst)
                    isFirst = false;
                else
                    sb.append(",");
                sb.append(entry.getKey()).append('=').append(values[entry.getValue()]);
            }
        } else {
            boolean isFirst = true;
            for (Object value : values) {
                if (isFirst)
                    isFirst = false;
                else
                    sb.append(",");
                sb.append(value);
            }
        }

        if (suffix != null)
            sb.append(suffix);
    }

    @Override
    public CompoundResult clone() {
        try {
            CompoundResult copy = (CompoundResult) super.clone();
            copy.values = Arrays.copyOf(this.values, this.values.length);
            return copy;
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("Failed to clone a cloneable object", e);
        }
    }

    protected int indexOf(String name) {
        Integer result = nameIndexMap.get(name);
        if (result == null)
            throw new IllegalArgumentException("No such name - '" + name + "'");
        return result;
    }

    public int size() {
        return values.length;
    }

    public Object get(int index) {
        return values[index];
    }

    public void set(int index, Object value) {
        values[index] = value;
    }

    public Object get(String name) {
        return values[indexOf(name)];
    }

    public Boolean getBoolean(int index) {
        return (Boolean) get(index);
    }

    public Boolean getBoolean(String name) {
        return (Boolean) get(name);
    }

    public Byte getByte(int index) {
        return (Byte) get(index);
    }

    public Byte getByte(String name) {
        return (Byte) get(name);
    }

    public Short getShort(int index) {
        return (Short) get(index);
    }

    public Short getShort(String name) {
        return (Short) get(name);
    }

    public Integer getInt(int index) {
        return (Integer) get(index);
    }

    public Integer getInt(String name) {
        return (Integer) get(name);
    }

    public Long getLong(int index) {
        return (Long) get(index);
    }

    public Long getLong(String name) {
        return (Long) get(name);
    }

    public Float getFloat(int index) {
        return (Float) get(index);
    }

    public Float getFloat(String name) {
        return (Float) get(name);
    }

    public Double getDouble(int index) {
        return (Double) get(index);
    }

    public Double getDouble(String name) {
        return (Double) get(name);
    }

    public String getString(int index) {
        return (String) get(index);
    }

    public String getString(String name) {
        return (String) get(name);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObjectArray(out, values);
        IOUtils.writeObject(out, nameIndexMap);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.values = IOUtils.readObjectArray(in);
        this.nameIndexMap = IOUtils.readObject(in);
    }
}
