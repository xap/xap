package com.gigaspaces.internal.server.storage;

import com.gigaspaces.internal.metadata.ITypeDesc;

import java.io.Externalizable;

public interface PropertiesHolder extends Externalizable {
    PropertiesHolder clone();
    Object[] getFixedProperties(ITypeDesc typeDesc);
    void setFixedProperties(ITypeDesc typeDescriptor, Object[] values);
    void setFixedProperties(Object[] values);
    Object getFixedProperty(ITypeDesc typeDesc, int position);
    void setFixedProperty(ITypeDesc typeDesc, int position, Object value);
    void setFixedProperty(int position, Object value);
    boolean allNulls();
    void copyFieldsArray();
}
