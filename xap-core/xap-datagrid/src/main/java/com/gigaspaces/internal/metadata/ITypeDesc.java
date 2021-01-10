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

import com.gigaspaces.client.storage_adapters.class_storage_adapters.ClassBinaryStorageAdapter;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ISwapExternalizable;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;

import java.io.Externalizable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 7.0
 */
public interface ITypeDesc extends SpaceTypeDescriptor, IDotnetTypeDescDetails, Cloneable, Externalizable, ISwapExternalizable {
    ITypeDesc clone();

    String getCodeBase();

    boolean isExternalizable();

    String[] getSuperClassesNames();

    String[] getRestrictSuperClassesNames();

    PropertyInfo[] getProperties();

    @Override
    int getNumOfFixedProperties();

    @Override
    PropertyInfo getFixedProperty(int propertyID);

    boolean isAllPropertiesObjectStorageType();

    SpaceIdType getSpaceIdType();

    int getIdentifierPropertyId();

    boolean isAutoGenerateId();

    boolean isAutoGenerateRouting();

    int getRoutingPropertyId();

    String getDefaultPropertyName();

    int getChecksum();

//	String[] getPropertiesNames();
//	String[] getPropertiesTypes();
//	boolean[] getPropertiesIndexTypes();

    int getNumOfIndexedProperties();

    int getIndexedPropertyID(int propertyID);

    boolean isSystemType();

    boolean isFifoSupported();

    boolean isFifoDefault();

    EntryType getObjectType();

    boolean supports(EntryType entryType);

    ITypeIntrospector getIntrospector(EntryType entryType);

    EntryTypeDesc getEntryTypeDesc(EntryType entryType);

    boolean isInactive();

    @Override
    Map<String, SpaceIndex> getIndexes();

    SpaceIndexType getIndexType(String indexName);

    List<SpaceIndex> getCompoundIndexes();

    boolean anyCompoundIndex();

    ClassBinaryStorageAdapter getClassBinaryStorageAdapter();

    Serializable getVersionedSerializable();

    String getPrimitivePropertiesWithoutNullValues();

    String getTypeUidPrefix();

    PropertyInfo[] getSerializedProperties();

    PropertyInfo[] getNonSerializedProperties();

    int[] getPositionsForScanning();

    int[] getPositionsForSplitting();
}
