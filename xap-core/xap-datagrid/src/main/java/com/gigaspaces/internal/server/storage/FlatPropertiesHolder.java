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
