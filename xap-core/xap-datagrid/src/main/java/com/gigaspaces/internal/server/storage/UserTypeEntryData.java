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

import com.gigaspaces.internal.metadata.EntryTypeDesc;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.ITypeIntrospector;
import com.gigaspaces.internal.transport.EntryPacketFactory;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.server.transaction.EntryXtnInfo;

import net.jini.space.InternalSpaceException;

import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class UserTypeEntryData extends AbstractEntryData {
    private final Object _data;
    private final Object[] _indexedPropertiesValues;

    public UserTypeEntryData(Object data, EntryTypeDesc entryTypeDesc,
                             int version, long expirationTime, boolean createEmptyTxnInfoIfNon) {
        super(entryTypeDesc, version, expirationTime, createEmptyTxnInfoIfNon);
        this._data = data;
        this._indexedPropertiesValues = initIndexedPropertiesValues();
    }

    private UserTypeEntryData(Object data, EntryTypeDesc entryTypeDesc,
                              int version, long expirationTime, boolean cloneXtnInfo, ITransactionalEntryData other, boolean createEmptyTxnInfoIfNon) {
        super(entryTypeDesc, version, expirationTime, cloneXtnInfo, other, createEmptyTxnInfoIfNon);
        this._data = data;
        this._indexedPropertiesValues = initIndexedPropertiesValues();
    }

    private UserTypeEntryData(UserTypeEntryData other, EntryXtnInfo xtnInfo) {
        super(other, xtnInfo);
        this._data = other._data;
        this._indexedPropertiesValues = other._indexedPropertiesValues;
    }

    @Override
    public ITransactionalEntryData createCopyWithoutTxnInfo() {
        return new UserTypeEntryData(this._data, this._entryTypeDesc,
                this._versionID, this._expirationTime, false);
    }

    @Override
    public ITransactionalEntryData createCopyWithoutTxnInfo(long newExpirationTime) {
        return new UserTypeEntryData(this._data, this._entryTypeDesc,
                this._versionID, newExpirationTime, false);
    }

    @Override
    public ITransactionalEntryData createCopyWithTxnInfo(int newVersion, long newExpirationTime) {
        return new UserTypeEntryData(this._data, this._entryTypeDesc,
                newVersion, newExpirationTime, true, this, false);
    }

    public ITransactionalEntryData createCopyWithTxnInfo(Object data, boolean createEmptyTxnInfoIfNon) {
        return new UserTypeEntryData(data, this._entryTypeDesc, this._versionID, this._expirationTime, true, this, createEmptyTxnInfoIfNon);
    }

    @Override
    public ITransactionalEntryData createCopyWithTxnInfo(boolean createEmptyTxnInfoIfNon) {
        return new UserTypeEntryData(this._data, this._entryTypeDesc, this._versionID, this._expirationTime, true, this, createEmptyTxnInfoIfNon);
    }

    @Override
    public ITransactionalEntryData createCopy(boolean cloneXtnInfo, IEntryData newEntryData, long newExpirationTime) {
        if (newEntryData instanceof UserTypeEntryData) {
            UserTypeEntryData other = (UserTypeEntryData) newEntryData;
            return new UserTypeEntryData(other._data, other._entryTypeDesc, other._versionID, newExpirationTime, cloneXtnInfo, this, false);
        } else
            throw new InternalSpaceException("Unable to create copy of IEntryData - unsupported type " + newEntryData.getClass().getName());
    }

    @Override
    public ITransactionalEntryData createCopyWithSuppliedTxnInfo(EntryXtnInfo ex) {
        return new UserTypeEntryData(this, ex);
    }

    @Override
    public ITransactionalEntryData createShallowClonedCopyWithSuppliedVersion(int versionID) {
        return createShallowClonedCopyWithSuppliedVersionAndExpiration(versionID, getExpirationTime());

    }

    @Override
    public ITransactionalEntryData createShallowClonedCopyWithSuppliedVersionAndExpiration(int versionID, long expirationTime) {
        IEntryPacket entryPacket = EntryPacketFactory.createFromObject(_data, _entryTypeDesc.getTypeDesc(), _entryTypeDesc.getEntryType(), true);
        Object clonedData = entryPacket.toObject(_entryTypeDesc.getEntryType());
        _entryTypeDesc.getIntrospector().setVersion(clonedData, versionID);
        return new UserTypeEntryData(clonedData, _entryTypeDesc, versionID, expirationTime, false /*createEmptyTxnInfoIfNon*/);
    }

    @Override
    public EntryDataType getEntryDataType() {
        return EntryDataType.USER_TYPE;
    }

    public Object getUserObject() {
        return _data;
    }

    @Override
    public int getNumOfFixedProperties() {
        return _entryTypeDesc.getTypeDesc().getNumOfFixedProperties();
    }

    @Override
    public Object getFixedPropertyValue(int propertyID) {
        int indexedPropertyID = _entryTypeDesc.getTypeDesc().getIndexedPropertyID(propertyID);
        return (indexedPropertyID == -1
                ? _entryTypeDesc.getIntrospector().getValue(_data, propertyID)
                : _indexedPropertiesValues[indexedPropertyID]);
    }

    @Override
    public void setFixedPropertyValue(int propertyID, Object value) {
        _entryTypeDesc.getIntrospector().setValue(_data, value, propertyID);
        int indexedPropertyID = _entryTypeDesc.getTypeDesc().getIndexedPropertyID(propertyID);
        if (indexedPropertyID != -1)
            _indexedPropertiesValues[indexedPropertyID] = value;
    }

    @Override
    public void setFixedPropertyValues(Object[] values) {
        if (values.length != _indexedPropertiesValues.length) {
            throw new IllegalArgumentException("Cannot substitute fixed property values with array of different size!");
        }
        for (int i = 0; i < values.length; i++) {
            _indexedPropertiesValues[i] = values[i];
        }
    }

    @Override
    public Object[] getFixedPropertiesValues() {
        Object[] values = _entryTypeDesc.getIntrospector().getValues(_data);

        int length = values.length;
        ITypeDesc typeDesc = _entryTypeDesc.getTypeDesc();
        for (int propertyID = 0; propertyID < length; propertyID++) {
            int indexedPropertyID = typeDesc.getIndexedPropertyID(propertyID);
            if (indexedPropertyID != -1)
                values[propertyID] = _indexedPropertiesValues[indexedPropertyID];
        }

        return values;
    }

    @Override
    public Map<String, Object> getDynamicProperties() {
        return _entryTypeDesc.getIntrospector().getDynamicProperties(_data);
    }

    @Override
    public void setDynamicProperties(Map<String, Object> dynamicProperties) {
        _entryTypeDesc.getIntrospector().setDynamicProperties(_data, dynamicProperties);
    }

    @Override
    public void setDynamicPropertyValue(String propertyName, Object value) {
        _entryTypeDesc.getIntrospector().setDynamicProperty(_data, propertyName, value);
    }

    @Override
    public void unsetDynamicPropertyValue(String propertyName) {
        _entryTypeDesc.getIntrospector().unsetDynamicProperty(_data, propertyName);
    }

    private Object[] initIndexedPropertiesValues() {
        ITypeDesc typeDesc = _entryTypeDesc.getTypeDesc();
        ITypeIntrospector<Object> introspector = _entryTypeDesc.getIntrospector();
        Object[] result = new Object[typeDesc.getNumOfIndexedProperties()];

        int propertiesLength = typeDesc.getNumOfFixedProperties();
        for (int propertyID = 0; propertyID < propertiesLength; propertyID++) {
            int indexedPropertyID = typeDesc.getIndexedPropertyID(propertyID);
            if (indexedPropertyID != -1)
                result[indexedPropertyID] = introspector.getValue(_data, propertyID);
        }

        return result;
    }

}
