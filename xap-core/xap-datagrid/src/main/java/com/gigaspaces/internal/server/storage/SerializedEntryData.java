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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.EntryTypeDesc;
import com.j_spaces.core.server.transaction.EntryXtnInfo;

import java.io.*;
import java.util.Map;


@com.gigaspaces.api.InternalApi
public class SerializedEntryData extends AbstractEntryData {
    private final byte[] _fieldsValues;
    private final short numOfFixedProperties;


    public SerializedEntryData(Object[] fieldsValues, EntryTypeDesc entryTypeDesc, int version, long expirationTime, boolean createEmptyTxnInfoIfNon) {
        super(entryTypeDesc, version, expirationTime, createEmptyTxnInfoIfNon);
        this._fieldsValues = serializeFields(fieldsValues);
        this.numOfFixedProperties = (short) fieldsValues.length;
    }

    public SerializedEntryData(byte[] fieldsValues, EntryTypeDesc entryTypeDesc, int version, long expirationTime, boolean createEmptyTxnInfoIfNon) {
        super(entryTypeDesc, version, expirationTime, createEmptyTxnInfoIfNon);
        this._fieldsValues = fieldsValues;
        this.numOfFixedProperties = (short) fieldsValues.length;
    }

    private SerializedEntryData(Object[] fieldsValues, EntryTypeDesc entryTypeDesc, int version, long expirationTime,
                                boolean cloneXtnInfo, ITransactionalEntryData other, boolean createEmptyTxnInfoIfNon) {
        super(entryTypeDesc, version, expirationTime, cloneXtnInfo, other, createEmptyTxnInfoIfNon);
        this._fieldsValues = serializeFields(fieldsValues);
        this.numOfFixedProperties = (short) fieldsValues.length;
    }

    private SerializedEntryData(byte[] fieldsValues, EntryTypeDesc entryTypeDesc, int version, long expirationTime,
                                boolean cloneXtnInfo, ITransactionalEntryData other, boolean createEmptyTxnInfoIfNon) {
        super(entryTypeDesc, version, expirationTime, cloneXtnInfo, other, createEmptyTxnInfoIfNon);
        this._fieldsValues = fieldsValues;
        this.numOfFixedProperties = (short) fieldsValues.length;
    }

    private SerializedEntryData(SerializedEntryData other, EntryXtnInfo xtnInfo) {
        super(other, xtnInfo);
        this._fieldsValues = other._fieldsValues;
        this.numOfFixedProperties = other.numOfFixedProperties;
    }

    @Override
    public ITransactionalEntryData createCopyWithoutTxnInfo() {
        return new SerializedEntryData(this._fieldsValues, this._entryTypeDesc, this._versionID, this._expirationTime, false);
    }

    @Override
    public ITransactionalEntryData createCopyWithoutTxnInfo(long newExpirationTime) {
        return new SerializedEntryData(this._fieldsValues, this._entryTypeDesc, this._versionID, newExpirationTime, false);
    }

    @Override
    public ITransactionalEntryData createCopyWithTxnInfo(int versionID, long newExpirationTime) {
        return new SerializedEntryData(this._fieldsValues, this._entryTypeDesc, versionID, newExpirationTime, true, this, false);
    }

    @Override
    public ITransactionalEntryData createShallowClonedCopyWithSuppliedVersion(int versionID) {
        return createShallowClonedCopyWithSuppliedVersionAndExpiration(versionID, _expirationTime);
    }

    @Override
    public ITransactionalEntryData createShallowClonedCopyWithSuppliedVersionAndExpiration(int versionID, long expirationTime) {
        Object[] clonedfieldsValues = new Object[numOfFixedProperties];
        System.arraycopy(getFixedPropertiesValues(), 0, clonedfieldsValues, 0, numOfFixedProperties);

        return new SerializedEntryData(clonedfieldsValues, this._entryTypeDesc, versionID, expirationTime, true, this, false);

    }

    @Override
    public ITransactionalEntryData createCopyWithTxnInfo(boolean createEmptyTxnInfoIfNon) {
        return new SerializedEntryData(this._fieldsValues, this._entryTypeDesc, this._versionID, this._expirationTime, true, this, createEmptyTxnInfoIfNon);
    }

    @Override
    public ITransactionalEntryData createCopy(boolean cloneXtnInfo, IEntryData newEntryData, long newExpirationTime) {
        return new SerializedEntryData(newEntryData.getFixedPropertiesValues(), newEntryData.getEntryTypeDesc(), newEntryData.getVersion(), newExpirationTime, cloneXtnInfo, this, false);
    }

    @Override
    public ITransactionalEntryData createCopyWithSuppliedTxnInfo(EntryXtnInfo ex) {
        return new SerializedEntryData(this, ex);
    }

    @Override
    public EntryDataType getEntryDataType() {
        return EntryDataType.FLAT;
    }

    @Override
    public int getNumOfFixedProperties() {
        return numOfFixedProperties;
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
        return deserializeFields(_fieldsValues);
    }

    @Override
    public Map<String, Object> getDynamicProperties() {
        throw new UnsupportedOperationException();
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
        if (values.length != numOfFixedProperties) {
            throw new IllegalArgumentException("Cannot substitute fixed property values with array of different size!");
        }
        for (int i = 0; i < values.length; i++) {
            modifyField(i, values[i]);
        }
    }

    @Override
    public void unsetDynamicPropertyValue(String propertyName) {
        throw new UnsupportedOperationException();
    }

    private byte[] serializeFields(Object[] fieldsValues) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            ObjectOutputStream out = new ObjectOutputStream(bos);
            IOUtils.writeObjectArrayCompressed(out, fieldsValues);
            out.flush();
            byte[] bytes = bos.toByteArray();
            System.out.println("bytes.length = " + bytes.length);
            return bytes;
        } catch (IOException e) {
            throw new UncheckedIOException("com.gigaspaces.internal.server.storage.FlatEntryData.serializeFields failed", e);
        }
    }

    private Object[] deserializeFields(byte[] fieldsValues) {
        ByteArrayInputStream bis = new ByteArrayInputStream(fieldsValues);
        try (ObjectInput in = new ObjectInputStream(bis)) {
            return IOUtils.readObjectArrayCompressed(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private void modifyField(int index, Object value) {
        throw new UnsupportedOperationException();
    }
}
