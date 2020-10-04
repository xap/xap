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

import com.gigaspaces.document.DocumentProperties;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.EntryTypeDesc;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ISwapExternalizable;
import com.gigaspaces.metadata.SpaceMetadataException;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class ExternalizableServerEntry implements IEntryData, Externalizable, ICustomTypeDescLoader, ISwapExternalizable {
    private static final long serialVersionUID = 1L;

    private transient EntryTypeDesc _entryTypeDesc;
    private transient ITypeDesc _typeDesc;
    private String _typeName;
    private int _typeChecksum;
    private Object[] _fixedProperties;
    private Map<String, Object> _dynamicProperties;
    private int _version;
    private long _expirationTime;
    private EntryType _entryType;

    /**
     * Required for Externalizable and ISwapExternalizable
     */
    public ExternalizableServerEntry() {
    }

    public ExternalizableServerEntry(IEntryData entry) {
        this._entryTypeDesc = entry.getEntryTypeDesc();
        this._entryType = entry.getEntryTypeDesc().getEntryType();
        this._typeDesc = entry.getEntryTypeDesc().getTypeDesc();
        this._typeName = _typeDesc.getTypeName();
        this._typeChecksum = _typeDesc.getChecksum();
        this._fixedProperties = entry.getFixedPropertiesValues();
        this._dynamicProperties = entry.getDynamicProperties();
        this._version = entry.getVersion();
        this._expirationTime = entry.getExpirationTime();
    }

    @Override
    public void loadTypeDescriptor(SpaceTypeManager typeManager) {
        ITypeDesc typeDesc = typeManager.getTypeDesc(_typeName);
        if (typeDesc == null)
            throw new SpaceMetadataException("Failed to load type descriptor for type [" + _typeName + "].");
        if (typeDesc.getChecksum() != _typeChecksum)
            throw new SpaceMetadataException("Inconsistent checksum for type [" + _typeName + "] - " +
                    " stream checksum: " + _typeChecksum +
                    ", space checksum: " + typeDesc.getChecksum());
        _typeDesc = typeDesc;
        _entryTypeDesc = _typeDesc.getEntryTypeDesc(_entryType);
    }

    @Override
    public EntryTypeDesc getEntryTypeDesc() {
        return _entryTypeDesc;
    }

    @Override
    public ITypeDesc getSpaceTypeDescriptor() {
        return _typeDesc;
    }

    @Override
    public Object getFixedPropertyValue(int position) {
        return _fixedProperties[position];
    }

    @Override
    public int getVersion() {
        return _version;
    }

    @Override
    public long getExpirationTime() {
        return _expirationTime;
    }

    @Override
    public EntryDataType getEntryDataType() {
        return EntryDataType.FLAT;
    }

    @Override
    public void setFixedPropertyValue(int index, Object value) {
        _fixedProperties[index] = value;
    }

    @Override
    public void setFixedPropertyValues(Object[] values) {
        if (values.length != _fixedProperties.length) {
            throw new IllegalArgumentException("Cannot substitute fixed property values with array of different size!");
        }
        for (int i = 0; i < values.length; i++) {
            _fixedProperties[i] = values[i];
        }
    }

    @Override
    public Object[] getFixedPropertiesValues() {
        return _fixedProperties;
    }

    @Override
    public Map<String, Object> getDynamicProperties() {
        return _dynamicProperties;
    }

    @Override
    public void setDynamicProperties(Map<String, Object> dynamicProperties) {
        _dynamicProperties = dynamicProperties;
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        serialize(out);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException, ClassNotFoundException {
        deserialize(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        serialize(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        deserialize(in);
    }

    private void serialize(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, _typeName);
        out.writeInt(_typeChecksum);
        IOUtils.writeObjectArray(out, _fixedProperties);
        IOUtils.writeObject(out, _dynamicProperties);
        out.writeInt(_version);
        out.writeLong(_expirationTime);
        out.writeByte(_entryType.getTypeCode());
    }

    private void deserialize(ObjectInput in) throws IOException, ClassNotFoundException {
        _typeName = IOUtils.readString(in);
        _typeChecksum = in.readInt();
        _fixedProperties = IOUtils.readObjectArray(in);
        _dynamicProperties = IOUtils.readObject(in);
        _version = in.readInt();
        _expirationTime = in.readLong();
        _entryType = EntryType.fromByte(in.readByte());
    }
}
