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
public class BinaryEntryData extends AbstractEntryData {
    private byte[] serializedFields;

    public BinaryEntryData(Object[] fieldsValues, EntryTypeDesc entryTypeDesc, int version, long expirationTime, EntryXtnInfo entryXtnInfo) {
        this(serializeFields(fieldsValues, entryTypeDesc.getTypeDesc()), entryTypeDesc, version, expirationTime, entryXtnInfo);
    }

    public BinaryEntryData(byte[] fieldsValues, EntryTypeDesc entryTypeDesc, int version, long expirationTime, EntryXtnInfo entryXtnInfo) {
        super(entryTypeDesc, version, expirationTime, entryXtnInfo);
        this.serializedFields = fieldsValues;
    }

    @Override
    public ITransactionalEntryData createCopy(int newVersion, long newExpiration, EntryXtnInfo newEntryXtnInfo, boolean shallowCloneData) {
        byte[] data = shallowCloneData ? Arrays.copyOf(this.serializedFields, this.serializedFields.length) : serializedFields;
        return new BinaryEntryData(data, this._entryTypeDesc, newVersion, newExpiration, newEntryXtnInfo);
    }

    @Override
    public ITransactionalEntryData createCopy(IEntryData newEntryData, long newExpirationTime) {
        return new BinaryEntryData(newEntryData.getFixedPropertiesValues(), newEntryData.getEntryTypeDesc(), newEntryData.getVersion(), newExpirationTime,
                copyTxnInfo(false, false));
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
        return null;
    }

    @Override
    public void setDynamicProperties(Map<String, Object> dynamicProperties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDynamicPropertyValue(String propertyName, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFixedPropertyValues(Object[] values) {
        if (values.length != getNumOfFixedProperties()) {
            throw new IllegalArgumentException("Cannot substitute fixed property values with array of different size!");
        }
        serializedFields = serializeFields(values, getSpaceTypeDescriptor());
    }

    private static byte[] serializeFields(Object[] fieldsValues, ITypeDesc typeDesc) {
        try {
            return typeDesc.getClassBinaryStorageAdapter().toBinary(fieldsValues);
        } catch (IOException e) {
            throw new UncheckedIOException("com.gigaspaces.internal.server.storage.FlatEntryData.serializeFields failed", e);
        }
    }

    private Object[] deserializeFields(byte[] fieldsValues) {
        try {
            return (this.getSpaceTypeDescriptor()).getClassBinaryStorageAdapter().fromBinary(fieldsValues);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private void modifyField(int index, Object value) {
        try {
            serializedFields = (this.getSpaceTypeDescriptor()).getClassBinaryStorageAdapter().modifyField(serializedFields, index, value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private void modifyFields(Map<Integer, Object> map) {
        try {
            serializedFields = (this.getSpaceTypeDescriptor()).getClassBinaryStorageAdapter().modifyFields(serializedFields, map);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }
}
