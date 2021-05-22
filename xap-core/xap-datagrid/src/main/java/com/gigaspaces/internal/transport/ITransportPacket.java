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

package com.gigaspaces.internal.transport;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.StorageTypeDeserialization;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.OperationID;

import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 7.1
 */
public interface ITransportPacket extends SmartExternalizable, Cloneable {
    OperationID getOperationID();

    void setOperationID(OperationID operationID);

    EntryType getEntryType();

    TransportPacketType getPacketType();

    String getTypeName();

    String getCodebase();

    ITypeDesc getTypeDescriptor();

    void setTypeDesc(ITypeDesc typeDesc, boolean serializeTypeDesc);

    int getTypeDescChecksum();

    boolean supportsTypeDescChecksum();

    boolean isSerializeTypeDesc();

    void setSerializeTypeDesc(boolean serializeTypeDesc);

    @Deprecated
        // since 8.0
    boolean isFifo();

    int getVersion();

    void setVersion(int version);

    @Deprecated
        // since 8.0
    boolean isNoWriteLease();

    Object[] getFieldValues();

    void setFieldsValues(Object[] values);

    Object getFieldValue(int index);

    void setFieldValue(int index, Object value);

    Object getPropertyValue(String name);

    void setPropertyValue(String name, Object value);

    Map<String, Object> getDynamicProperties();

    void setDynamicProperties(Map<String, Object> properties);

    String[] getMultipleUIDs();

    void setMultipleUIDs(String[] uids);

    String getUID();

    Object toObject(QueryResultTypeInternal resultType);

    Object toObject(QueryResultTypeInternal resultType, StorageTypeDeserialization storageTypeDeserialization);

    Object toObject(EntryType entryType);

    Object toObject(EntryType entryType, StorageTypeDeserialization storageTypeDeserialization);

    String getExternalEntryImplClassName();

    boolean isTransient();

    ICustomQuery getCustomQuery();

    Object getRoutingFieldValue();
}
