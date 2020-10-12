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

import java.util.HashMap;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class FlatEntryData extends AbstractEntryData {
    private final Object[] _fieldsValues;
    private Map<String, Object> _dynamicProperties;

    public FlatEntryData(Object[] fieldsValues, Map<String, Object> dynamicProperties, EntryTypeDesc entryTypeDesc, int version, long expirationTime, boolean createEmptyTxnInfoIfNon) {
        super(entryTypeDesc, version, expirationTime, createEmptyTxnInfoIfNon ? new EntryXtnInfo() : null);
        this._fieldsValues = fieldsValues;
        this._dynamicProperties = dynamicProperties;
    }

    private FlatEntryData(Object[] fieldsValues, Map<String, Object> dynamicProperties, EntryTypeDesc entryTypeDesc, int version, long expirationTime,
                          boolean cloneXtnInfo, ITransactionalEntryData other, boolean createEmptyTxnInfoIfNon) {
        super(entryTypeDesc, version, expirationTime, copyTxnInfo(other.getEntryXtnInfo(), cloneXtnInfo, createEmptyTxnInfoIfNon));
        this._fieldsValues = fieldsValues;
        this._dynamicProperties = dynamicProperties;
    }

    private FlatEntryData(FlatEntryData other, EntryXtnInfo xtnInfo) {
        super(other.getEntryTypeDesc(), other.getVersion(), other.getExpirationTime(), xtnInfo);
        this._fieldsValues = other._fieldsValues;
        this._dynamicProperties = other._dynamicProperties;

    }

    @Override
    public ITransactionalEntryData createCopyWithoutTxnInfo(long newExpirationTime) {
        return new FlatEntryData(this._fieldsValues, this._dynamicProperties, this._entryTypeDesc, this._versionID, newExpirationTime, false);
    }

    @Override
    public ITransactionalEntryData createCopyWithTxnInfo(int versionID, long newExpirationTime) {
        return new FlatEntryData(this._fieldsValues, this._dynamicProperties, this._entryTypeDesc, versionID, newExpirationTime, true, this, false);
    }

    @Override
    public ITransactionalEntryData createShallowClonedCopyWithSuppliedVersion(int versionID) {
        return createShallowClonedCopyWithSuppliedVersionAndExpiration(versionID, _expirationTime);
    }

    @Override
    public ITransactionalEntryData createShallowClonedCopyWithSuppliedVersionAndExpiration(int versionID, long expirationTime) {
        Object[] clonedfieldsValues = new Object[_fieldsValues.length];
        System.arraycopy(_fieldsValues, 0, clonedfieldsValues, 0, _fieldsValues.length);

        Map<String, Object> clonedDynamicProperties = _dynamicProperties != null ? new HashMap<String, Object>(_dynamicProperties) : null;

        return new FlatEntryData(clonedfieldsValues, clonedDynamicProperties, this._entryTypeDesc, versionID, expirationTime, true, this, false);

    }

    @Override
    public ITransactionalEntryData createCopyWithTxnInfo(boolean createEmptyTxnInfoIfNon) {
        return new FlatEntryData(this._fieldsValues, this._dynamicProperties, this._entryTypeDesc, this._versionID, this._expirationTime, true, this, createEmptyTxnInfoIfNon);
    }

    @Override
    public ITransactionalEntryData createCopy(boolean cloneXtnInfo, IEntryData newEntryData, long newExpirationTime) {
        return new FlatEntryData(newEntryData.getFixedPropertiesValues(), newEntryData.getDynamicProperties(), newEntryData.getEntryTypeDesc(), newEntryData.getVersion(), newExpirationTime, cloneXtnInfo, this, false);
    }

    @Override
    public ITransactionalEntryData createCopyWithSuppliedTxnInfo(EntryXtnInfo ex) {
        return new FlatEntryData(this, ex);
    }

    @Override
    public EntryDataType getEntryDataType() {
        return EntryDataType.FLAT;
    }

    @Override
    public int getNumOfFixedProperties() {
        return _fieldsValues.length;
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
    public void unsetDynamicPropertyValue(String propertyName) {
        if (_dynamicProperties != null)
            _dynamicProperties.remove(propertyName);
    }

    @Override
    public void setDynamicProperties(Map<String, Object> dynamicProperties) {
        _dynamicProperties = dynamicProperties;
    }

}
