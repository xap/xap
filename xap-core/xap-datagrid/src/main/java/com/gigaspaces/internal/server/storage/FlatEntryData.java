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
import com.j_spaces.core.server.transaction.EntryXtnInfo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class FlatEntryData implements ITransactionalEntryData {
    private final EntryTypeDesc _entryTypeDesc;
    private final int _versionID;
    private final long _expirationTime;
    private final EntryXtnInfo _entryTxnInfo;
    private final Object[] _fieldsValues;
    private Map<String, Object> _dynamicProperties;

    public FlatEntryData(Object[] fieldsValues, Map<String, Object> dynamicProperties, EntryTypeDesc entryTypeDesc, int version, long expirationTime, EntryXtnInfo entryXtnInfo) {
        this._entryTypeDesc = entryTypeDesc;
        this._versionID = version;
        this._expirationTime = expirationTime;
        this._entryTxnInfo = entryXtnInfo;
        this._fieldsValues = fieldsValues;
        this._dynamicProperties = dynamicProperties;
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
        Object[] fieldValues = shallowCloneData ? Arrays.copyOf(_fieldsValues, _fieldsValues.length) : _fieldsValues;
        Map<String, Object> dynamicProperties = shallowCloneData && _dynamicProperties != null ? new HashMap<>(_dynamicProperties) : _dynamicProperties;
        return new FlatEntryData(fieldValues, dynamicProperties, this._entryTypeDesc, newVersion, newExpiration, newEntryXtnInfo);
    }

    @Override
    public ITransactionalEntryData createCopy(IEntryData newEntryData, long newExpirationTime) {
        return new FlatEntryData(newEntryData.getFixedPropertiesValues(), newEntryData.getDynamicProperties(), newEntryData.getEntryTypeDesc(), newEntryData.getVersion(), newExpirationTime,
                copyTxnInfo(false, false));
    }

    @Override
    public EntryDataType getEntryDataType() {
        return EntryDataType.FLAT;
    }

    @Override
    public Object getFixedPropertyValue(int index) {
        return _fieldsValues[index];
    }

    @Override
    public void setFixedPropertyValue(int index, Object value) {
        _fieldsValues[index] = value;
    }

    @Override
    public Object[] getFixedPropertiesValues() {
        return _fieldsValues;
    }

    @Override
    public Map<String, Object> getDynamicProperties() {
        return _dynamicProperties;
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
        if (values.length != _fieldsValues.length) {
            throw new IllegalArgumentException("Cannot substitute fixed property values with array of different size!");
        }
        for (int i = 0; i < values.length; i++) {
            _fieldsValues[i] = values[i];
        }
    }

    @Override
    public void setDynamicProperties(Map<String, Object> dynamicProperties) {
        _dynamicProperties = dynamicProperties;
    }
}
