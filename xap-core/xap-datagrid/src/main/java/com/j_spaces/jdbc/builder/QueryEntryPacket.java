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

package com.j_spaces.jdbc.builder;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.StorageTypeDeserialization;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.TransportPacketType;
import com.j_spaces.core.OperationID;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * @author anna
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class QueryEntryPacket implements IEntryPacket {
    private static final long serialVersionUID = 1L;

    private String _uid;
    private String[] _fieldNames;
    private Object[] _fieldValues;

    public QueryEntryPacket() {
    }

    public QueryEntryPacket(String[] fieldsNames, Object[] fieldsValues) {
        this._fieldNames = fieldsNames;
        this._fieldValues = fieldsValues;
    }

    public String getTypeName() {
        return null;
    }

    public String getCodebase() {
        return null;
    }

    public String getExternalEntryImplClassName() {
        return null;
    }

    public Object getFieldValue(int index) {
        return _fieldValues[index];
    }

    public Object getPropertyValue(String name) {
        final ITypeDesc typeDesc = getTypeDescriptor();
        int pos = typeDesc.getFixedPropertyPosition(name);

        if (pos != -1)
            return getFieldValue(pos);

        if (!typeDesc.supportsDynamicProperties())
            throw new IllegalArgumentException("Failed to get value of property '" + name + "' in type '" + typeDesc.getTypeName() +
                    "' - it does not exist and the type does not support dynamic properties.");

        return getDynamicProperties().get(name);
    }

    public void setPropertyValue(String name, Object value) {
        final ITypeDesc typeDesc = getTypeDescriptor();
        int pos = typeDesc.getFixedPropertyPosition(name);

        if (pos != -1)
            setFieldValue(pos, value);
        else {
            if (getDynamicProperties() == null)
                throw new IllegalArgumentException("Failed to set value of property '" + name + "' in type '" + typeDesc.getTypeName() +
                        "' - it does not exist and the type does not support dynamic properties.");

            getDynamicProperties().put(name, value);
        }
    }

    public Object[] getFieldValues() {
        return _fieldValues;
    }

    public Map<String, Object> getDynamicProperties() {
        return null;
    }

    public void setDynamicProperties(Map<String, Object> properties) {

    }

    public String[] getFieldNames() {
        return _fieldNames;
    }

    public Object getID() {
        return null;
    }

    public String[] getMultipleUIDs() {
        return null;
    }

    public void setMultipleUIDs(String[] uids) {
    }

    public EntryType getEntryType() {
        throw new UnsupportedOperationException("This method is not supported for query packets.");
    }

    public TransportPacketType getPacketType() {
        throw new UnsupportedOperationException("This method is not supported for query packets.");
    }

    public Object getRoutingFieldValue() {
        return null;
    }

    public boolean supportsTypeDescChecksum() {
        return false;
    }

    public int getTypeDescChecksum() {
        return 0;
    }

    public long getTTL() {
        return 0;
    }

    public void setTTL(long ttl) {
    }

    public ITypeDesc getTypeDescriptor() {
        return null;
    }

    public void setTypeDesc(ITypeDesc typeDesc, boolean serializeTypeDesc) {
    }

    public boolean isSerializeTypeDesc() {
        return false;
    }

    public void setSerializeTypeDesc(boolean serializeTypeDesc) {
    }

    public String getUID() {
        return _uid;
    }

    public int getVersion() {
        return 0;
    }

    public boolean isFifo() {
        return false;
    }

    public boolean isNoWriteLease() {
        return false;
    }

    public boolean isReturnOnlyUids() {
        return false;
    }

    public boolean isTransient() {
        return false;
    }

    public void setFieldValue(int index, Object value) {
    }

    public void setFieldsValues(Object[] values) {
        _fieldValues = values;
    }

    public void setReturnOnlyUIDs(boolean returnOnlyUIDs) {
    }

    public void setUID(String uid) {
        _uid = uid;
    }

    public void setVersion(int version) {
    }

    public ICustomQuery getCustomQuery() {
        throw new RuntimeException("Not Implemented Yet - getCustomQuery.");
    }

    public void setCustomQuery(ICustomQuery customQuery) {
        throw new RuntimeException("Not Implemented Yet - setCustomQuery.");
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {

    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {

    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _fieldNames = (String[]) in.readObject();
        _fieldValues = (Object[]) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_fieldNames);
        out.writeObject(_fieldValues);
    }

    @Override
    public IEntryPacket clone() {
        try {
            return (IEntryPacket) super.clone();
        } catch (CloneNotSupportedException e) {
            // should not happen
            throw new InternalError();
        }
    }

    public OperationID getOperationID() {
        return null;
    }

    public void setOperationID(OperationID operationID) {
    }

    public Object toObject(QueryResultTypeInternal resultType) {
        throw new UnsupportedOperationException("This method is not supported for query packets.");
    }

    public Object toObject(EntryType entryType) {
        throw new UnsupportedOperationException("This method is not supported for query packets.");
    }

    @Override
    public Object toObject(QueryResultTypeInternal resultType, StorageTypeDeserialization storageTypeDeserialization) {
        throw new UnsupportedOperationException("This method is not supported for query packets.");
    }

    @Override
    public Object toObject(EntryType entryType, StorageTypeDeserialization storageTypeDeserialization) {
        throw new UnsupportedOperationException("This method is not supported for query packets.");
    }

    @Override
    public void setPreviousVersion(int version) {
    }

    @Override
    public int getPreviousVersion() {
        return 0;
    }

    @Override
    public boolean hasPreviousVersion() {
        return false;
    }

    /**
     * true if the entry packet has an array of fixed properties
     */
    @Override
    public boolean hasFixedPropertiesArray() {
        return false;
    }

    @Override
    public boolean isExternalizableEntryPacket() {
        return false;
    }
}
