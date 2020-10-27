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
import com.gigaspaces.internal.metadata.EntryTypeDesc;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.core.server.transaction.EntryXtnInfo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Map;

/***
 * @author Yael Nahon
 * @since 15.8
 */
@com.gigaspaces.api.InternalApi
public class BinaryEntryData implements ITransactionalEntryData {
    private final EntryTypeDesc _entryTypeDesc;
    private final int _versionID;
    private final long _expirationTime;
    private final EntryXtnInfo _entryTxnInfo;
    private byte[] serializedFields;
    private Map<String, Object> _dynamicProperties;

    public BinaryEntryData(Object[] fieldsValues, Map<String, Object> dynamicProperties, EntryTypeDesc entryTypeDesc, int version, long expirationTime, EntryXtnInfo entryXtnInfo) {
        this(serializeFields(fieldsValues, entryTypeDesc.getTypeDesc()), dynamicProperties, entryTypeDesc, version, expirationTime, entryXtnInfo);
    }

    public BinaryEntryData(byte[] fieldsValues, Map<String, Object> dynamicProperties, EntryTypeDesc entryTypeDesc, int version, long expirationTime, EntryXtnInfo entryXtnInfo) {
        this._entryTypeDesc = entryTypeDesc;
        this._versionID = version;
        this._expirationTime = expirationTime;
        this._entryTxnInfo = entryXtnInfo;
        this.serializedFields = fieldsValues;
        this._dynamicProperties = dynamicProperties;
    }

    private static byte[] serializeFields(Object[] fieldsValues, ITypeDesc typeDesc) {
        try {
            return typeDesc.getClassBinaryStorageAdapter().toBinary(typeDesc, fieldsValues);
        } catch (IOException e) {
            throw new UncheckedIOException("com.gigaspaces.internal.server.storage.BinaryEntryData.serializeFields failed", e);
        }
    }

    @Override
    public EntryTypeDesc getEntryTypeDesc() {
        return _entryTypeDesc;
    }

    @Override
    public int getVersion() {
        return _versionID;
    }

    @Override
    public long getExpirationTime() {
        return _expirationTime;
    }

    @Override
    public EntryXtnInfo getEntryXtnInfo() {
        return _entryTxnInfo;
    }

    @Override
    public ITransactionalEntryData createCopy(int newVersion, long newExpiration, EntryXtnInfo newEntryXtnInfo, boolean shallowCloneData) {
        byte[] data = shallowCloneData ? serializedFields : this.serializedFields.clone() ;
        return new BinaryEntryData(data,_dynamicProperties, this._entryTypeDesc, newVersion, newExpiration, newEntryXtnInfo);
    }

    @Override
    public ITransactionalEntryData createCopy(IEntryData newEntryData, long newExpirationTime) {
        if(newEntryData instanceof BinaryEntryData){
            return new BinaryEntryData(((BinaryEntryData) newEntryData).getSerializedFields(),newEntryData.getDynamicProperties(), newEntryData.getEntryTypeDesc(), newEntryData.getVersion(), newExpirationTime,
                    copyTxnInfo(false, false));
        } else {
            return new BinaryEntryData(newEntryData.getFixedPropertiesValues(),newEntryData.getDynamicProperties(), newEntryData.getEntryTypeDesc(), newEntryData.getVersion(), newExpirationTime,
                    copyTxnInfo(false, false));
        }
    }

    @Override
    public EntryDataType getEntryDataType() {
        return EntryDataType.FLAT;
    }

    @Override
    public Object getFixedPropertyValue(int index) {
        return getFixedPropertiesValues()[index];
    }

    @Override
    public void setFixedPropertyValue(int index, Object value) {
        modifyField(index, value);
    }

    @Override
    public Object[] getFixedPropertiesValues() {
        return deserializeFields(serializedFields);
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
    public void setDynamicPropertyValue(String propertyName, Object value) {
        if (!_entryTypeDesc.getTypeDesc().supportsDynamicProperties())
            throw new UnsupportedOperationException(_entryTypeDesc.getTypeDesc().getTypeName() + " does not support dynamic properties");

        if (_dynamicProperties == null)
            _dynamicProperties = new DocumentProperties();

        _dynamicProperties.put(propertyName, value);
    }

    @Override
    public void setFixedPropertyValues(Object[] values) {
        if (values.length != getNumOfFixedProperties()) {
            throw new IllegalArgumentException("Cannot substitute fixed property values with array of different size!");
        }
        serializedFields = serializeFields(values, getSpaceTypeDescriptor());
    }

    private Object[] deserializeFields(byte[] fieldsValues) {
        try {
            return (this.getSpaceTypeDescriptor()).getClassBinaryStorageAdapter().fromBinary(this.getSpaceTypeDescriptor(), fieldsValues);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private void modifyField(int index, Object value) {
        try {
            serializedFields = (this.getSpaceTypeDescriptor()).getClassBinaryStorageAdapter().modifyField(this.getSpaceTypeDescriptor(), serializedFields, index, value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private void modifyFields(Map<Integer, Object> map) {
        try {
            serializedFields = (this.getSpaceTypeDescriptor()).getClassBinaryStorageAdapter().modifyFields(this.getSpaceTypeDescriptor(), serializedFields, map);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    public byte[] getSerializedFields() {
        return serializedFields;
    }

    public void setSerializedFields(byte[] serializedFields) {
        this.serializedFields = serializedFields;
    }
}
