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
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.j_spaces.core.server.transaction.EntryXtnInfo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/***
 * @author Yael Nahon
 * @since 15.8
 */
@com.gigaspaces.api.InternalApi
public class HybridEntryData implements IBinaryEntryData {
    protected final EntryTypeDesc _entryTypeDesc;
    protected final int _versionID;
    protected final long _expirationTime;
    protected final EntryXtnInfo _entryTxnInfo;
    protected Map<String, Object> _dynamicProperties;
    private byte[] serializedProperties;
    private Object[] nonSerializedProperties;


    public HybridEntryData(Object[] fixedProperties, Map<String, Object> dynamicProperties, EntryTypeDesc entryTypeDesc, int version, long expirationTime, EntryXtnInfo entryXtnInfo) {
        this(new HybridPropertiesHolder(entryTypeDesc.getTypeDesc(), fixedProperties), dynamicProperties, entryTypeDesc, version, expirationTime, entryXtnInfo);
    }

    public HybridEntryData(HybridPropertiesHolder propertiesHolder, Map<String, Object> dynamicProperties, EntryTypeDesc entryTypeDesc, int version,
                           long expirationTime, EntryXtnInfo entryXtnInfo) {
        this._entryTypeDesc = entryTypeDesc;
        this._versionID = version;
        this._expirationTime = expirationTime;
        this._entryTxnInfo = entryXtnInfo;
        this._dynamicProperties = dynamicProperties;
        this.nonSerializedProperties = propertiesHolder.getNonSerializedProperties();
        if(propertiesHolder.isDirty()){
            this.serializedProperties = HybridPropertiesHolder.serializeFields(entryTypeDesc.getTypeDesc(), propertiesHolder.getUnpackedSerializedProperties());
        }else {
            this.serializedProperties = propertiesHolder.getPackedSerializedProperties();
        }
    }

    private HybridEntryData(Object[] nonSerializedProperties, byte[] packedSerializedProperties, Map<String, Object> dynamicProperties, EntryTypeDesc entryTypeDesc, int version,
                           long expirationTime, EntryXtnInfo entryXtnInfo) {
        this._entryTypeDesc = entryTypeDesc;
        this._versionID = version;
        this._expirationTime = expirationTime;
        this._entryTxnInfo = entryXtnInfo;
        this._dynamicProperties = dynamicProperties;
        this.nonSerializedProperties = nonSerializedProperties;
        this.serializedProperties = packedSerializedProperties;
    }


    @Override
    public ITransactionalEntryData createCopy(int newVersion, long newExpiration, EntryXtnInfo newEntryXtnInfo, boolean shallowCloneData) {
        byte[] packedSerializedProperties = shallowCloneData ? Arrays.copyOf(this.serializedProperties, this.serializedProperties.length) : serializedProperties;
        Object[] nonSerializeData = shallowCloneData ? Arrays.copyOf(this.nonSerializedProperties, this.nonSerializedProperties.length) : nonSerializedProperties;
        Map<String, Object> dynamicProperties = shallowCloneData && _dynamicProperties != null ? new HashMap<>(_dynamicProperties) : _dynamicProperties;
        return new HybridEntryData(nonSerializeData, packedSerializedProperties, dynamicProperties, this._entryTypeDesc, newVersion, newExpiration, newEntryXtnInfo);
    }

    @Override
    public ITransactionalEntryData createCopy(IEntryData newEntryData, long newExpirationTime) {
        if (newEntryData instanceof HybridEntryData) {
            HybridEntryData data = (HybridEntryData) newEntryData;
            return new HybridEntryData(data.getNonSerializedProperties(), data.getPackedSerializedProperties(),
                    newEntryData.getDynamicProperties(), newEntryData.getEntryTypeDesc(), newEntryData.getVersion(), newExpirationTime,
                    copyTxnInfo(false, false));
        } else {
            Thread.dumpStack();
            throw new IllegalStateException("Code should be unreachable");
        }
    }

    public Object[] getNonSerializedProperties() {
        return this.nonSerializedProperties;
    }


    @Override
    public Object getFixedPropertyValue(int index) {
        if (_entryTypeDesc.getTypeDesc().isSerializedProperty(index)) {
            try {
                return _entryTypeDesc.getTypeDesc().getClassBinaryStorageAdapter()
                        .getFieldAtIndex(_entryTypeDesc.getTypeDesc(), serializedProperties, _entryTypeDesc.getTypeDesc().findHybridIndex(index));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
        } else {
            return nonSerializedProperties[_entryTypeDesc.getTypeDesc().findHybridIndex(index)];
        }
    }

    @Override
    public void setFixedPropertyValue(int index, Object value) {
        if (_entryTypeDesc.getTypeDesc().isSerializedProperty(index)) {
            try {
                this.serializedProperties = _entryTypeDesc.getTypeDesc().getClassBinaryStorageAdapter()
                        .modifyField(_entryTypeDesc.getTypeDesc(), serializedProperties, _entryTypeDesc.getTypeDesc().findHybridIndex(index), value);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
        } else {
            nonSerializedProperties[_entryTypeDesc.getTypeDesc().findHybridIndex(index)] = value;
        }
    }

    @Override
    public Object[] getFixedPropertiesValues() {
        ITypeDesc typeDesc = _entryTypeDesc.getTypeDesc();

        if(typeDesc.getSerializedProperties().length > 0) {
            Object[] fields = new Object[typeDesc.getProperties().length];
            try {
                Object[] deserializedFields = typeDesc.getClassBinaryStorageAdapter().fromBinary(typeDesc, serializedProperties);
                int i = 0;
                for (PropertyInfo property : typeDesc.getProperties()) {
                    if (property.isBinarySpaceProperty(typeDesc)) {
                        fields[i] = deserializedFields[typeDesc.findHybridIndex(i)];
                    } else {
                        fields[i] = nonSerializedProperties[typeDesc.findHybridIndex(i)];
                    }
                    i++;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
            return fields;
        } else {
            return this.nonSerializedProperties;
        }
    }

    @Override
    public void setFixedPropertyValues(Object[] values) {
        if (values.length != getNumOfFixedProperties()) {
            throw new IllegalArgumentException("Cannot substitute fixed property values with array of different size!");
        }
        HybridPropertiesHolder payload = new HybridPropertiesHolder(getEntryTypeDesc().getTypeDesc(), values);
        nonSerializedProperties = payload.getNonSerializedProperties();
        serializedProperties = payload.getPackedSerializedProperties();
    }


    public void setFixedPropertyValues(Object[] nonSerializedFields, byte[] serializedFields) {
        this.nonSerializedProperties = nonSerializedFields;
        this.serializedProperties = serializedFields;
    }

    @Override
    public byte[] getPackedSerializedProperties() {
        return serializedProperties;
    }

    @Override
    public boolean isEqualProperties(IBinaryEntryData old) {
        return serializedProperties == old.getPackedSerializedProperties() &&
                nonSerializedProperties == ((HybridEntryData) old).nonSerializedProperties;
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
    public EntryDataType getEntryDataType() {
        return EntryDataType.FLAT;
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
    public boolean isHybrid() {
        return true;
    }
}
