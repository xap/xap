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
package com.gigaspaces.internal.server.storage;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class FlatPropertiesHolder implements PropertiesHolder {

    Object[] fields;

    public FlatPropertiesHolder() {
    }

    public FlatPropertiesHolder(Object[] fields) {
        this.fields = fields;
    }

    @Override
    public PropertiesHolder clone() {
        FlatPropertiesHolder holder = new FlatPropertiesHolder();
        if (fields != null)
            holder.setFixedProperties(fields.clone());
        return holder;
    }

    @Override
    public Object[] getFixedProperties(ITypeDesc typeDesc) {
        return fields;
    }

    @Override
    public void setFixedProperties(ITypeDesc typeDescriptor, Object[] values) {
        this.fields = values;
    }

    @Override
    public void setFixedProperties(Object[] values) {
        this.fields = values;
    }

    @Override
    public Object getFixedProperty(ITypeDesc typeDesc, int position) {
        return fields[position];
    }

    @Override
    public void setFixedProperty(ITypeDesc typeDesc, int position, Object value) {
        this.fields[position] = value;
    }

    @Override
    public void setFixedProperty(int position, Object value) {
        this.fields[position] = value;
    }

    @Override
    public boolean allNulls() {
        return this.fields == null;
    }

    @Override
    public void copyFieldsArray() {
        Object[] src = fields;
        Object[] target = new Object[src.length];
        System.arraycopy(src, 0, target, 0, src.length);
        this.fields = target;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObjectArrayCompressed(out, fields);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fields = IOUtils.readObjectArrayCompressed(in);
    }
}
