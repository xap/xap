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
public class BinaryEntryData extends AbstractBinaryEntryData {
    private byte[] serializedFields;

    public BinaryEntryData(Object[] fieldsValues, Map<String, Object> dynamicProperties, EntryTypeDesc entryTypeDesc, int version, long expirationTime, EntryXtnInfo entryXtnInfo) {
        this(serializeFields(fieldsValues, entryTypeDesc.getTypeDesc()), dynamicProperties, entryTypeDesc, version, expirationTime, entryXtnInfo);
    }

    public BinaryEntryData(byte[] fieldsValues, Map<String, Object> dynamicProperties, EntryTypeDesc entryTypeDesc, int version, long expirationTime, EntryXtnInfo entryXtnInfo) {
        super(entryTypeDesc, version, expirationTime, entryXtnInfo, dynamicProperties);
        this.serializedFields = fieldsValues;
    }

    @Override
    public ITransactionalEntryData createCopy(int newVersion, long newExpiration, EntryXtnInfo newEntryXtnInfo, boolean shallowCloneData) {
        byte[] data = shallowCloneData ? Arrays.copyOf(this.serializedFields, this.serializedFields.length) : serializedFields;
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
    public void setFixedPropertyValues(Object[] values) {
        if (values.length != getNumOfFixedProperties()) {
            throw new IllegalArgumentException("Cannot substitute fixed property values with array of different size!");
        }
        serializedFields = serializeFields(values, getSpaceTypeDescriptor());
    }

    @Override
    public byte[] getSerializedFields() {
        return serializedFields;
    }

    @Override
    public void setSerializedFields(byte[] serializedFields) {
        this.serializedFields = serializedFields;
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

}
