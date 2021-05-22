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

package com.gigaspaces.internal.metadata;

import com.gigaspaces.internal.client.StorageTypeDeserialization;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.IGSEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * Common interface for all the introspectors implementations.
 *
 * @author Niv Ingberg
 * @since 7.0
 */
public interface ITypeIntrospector<T> extends SmartExternalizable {
    byte getExternalizableCode();

    void writeExternal(ObjectOutput out, PlatformLogicalVersion version) throws IOException;

    void readExternal(ObjectInput in, PlatformLogicalVersion version) throws IOException, ClassNotFoundException;

    void initialize(ITypeDesc typeDesc);

    ITypeDesc getTypeDesc();

    Class<T> getType();

    boolean hasUID(T target);

    String getUID(T target);

    String getUID(T target, boolean isTemplate, boolean ignoreAutoGenerateUid);

    boolean setUID(T target, String uid);

    Object getRouting(T target);

    boolean hasVersionProperty(T target);

    int getVersion(T target);

    boolean setVersion(T target, int version);

    boolean hasTimeToLiveProperty(T target);

    long getTimeToLive(T target);

    boolean setTimeToLive(T target, long ttl);

    boolean hasTransientProperty(T target);

    boolean isTransient(T target);

    boolean setTransient(T target, boolean isTransient);

    Object[] getValues(T target);

    void setValues(T target, Object[] values);

    Object[] getSerializedValues(T target);

    boolean hasDynamicProperties();

    Map<String, Object> getDynamicProperties(T target);

    void setDynamicProperties(T target, Map<String, Object> dynamicProperties);

    void setDynamicProperty(T target, String name, Object value);

    void unsetDynamicProperty(T target, String name);

    Object getValue(T target, int index);

    void setValue(T target, Object value, int index);

    Object getValue(T target, String name);

    void setValue(T target, String name, Object value);

    void setEntryInfo(T target, String uid, int version, long ttl);

    T toObject(IEntryPacket packet);

    T toObject(IEntryPacket packet, StorageTypeDeserialization storageTypeDeserialization);

    T toObject(IGSEntry entry, ITypeDesc typeDesc);

    boolean supportsNestedOperations();

    Class<?> getPathType(String path);

    Object getPathValue(IEntryPacket entryPacket, String path);

    boolean propertyHasNullValue(int position);

    boolean hasConstructorProperties();

}
