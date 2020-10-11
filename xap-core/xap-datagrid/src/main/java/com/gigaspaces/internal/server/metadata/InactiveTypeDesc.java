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

package com.gigaspaces.internal.server.metadata;

import com.gigaspaces.annotation.pojo.FifoSupport;
import com.gigaspaces.client.storage_adapters.class_storage_adapters.ClassBinaryStorageAdapter;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.EntryTypeDesc;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.ITypeIntrospector;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.metadata.SpaceIdType;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.metadata.StorageType;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.query.extension.metadata.TypeQueryExtensions;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

@com.gigaspaces.api.InternalApi
public class InactiveTypeDesc implements ITypeDesc {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    private String _className;
    private String[] _superClassesNames;

    private transient String _description;

    public InactiveTypeDesc() {
    }

    public InactiveTypeDesc(String className, String[] superClassesNames) {
        this._className = className;
        this._superClassesNames = superClassesNames;
    }

    @Override
    public String toString() {
        if (_description == null)
            _description = generateDescription();
        return _description;
    }

    private String generateDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("typeName=").append(_className).append(", ");
        sb.append("superTypesNames=").append(Arrays.toString(_superClassesNames));
        sb.append("]");
        return sb.toString();
    }

    public String getTypeName() {
        return this._className;
    }

    @Override
    public String getTypeUidPrefix() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTypeSimpleName() {
        return StringUtils.getSuffix(_className, ".");
    }

    public String[] getSuperClassesNames() {
        return this._superClassesNames;
    }

    public boolean isInactive() {
        return true;
    }

    @Override
    public ITypeDesc clone() {
        try {
            return (ITypeDesc) super.clone();
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError();
        }
    }
    
    /* UNSUPPORTED */

    public Class<? extends Object> getObjectClass() {
        throw new UnsupportedOperationException();
    }

    public Class<? extends SpaceDocument> getDocumentWrapperClass() {
        throw new UnsupportedOperationException();
    }

    public boolean isExternalizable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supports(EntryType entryType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ITypeIntrospector getIntrospector(EntryType objectType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public EntryTypeDesc getEntryTypeDesc(EntryType entryType) {
        throw new UnsupportedOperationException();
    }

    public String getCodeBase() {
        throw new UnsupportedOperationException();
    }

    public String getIdPropertyName() {
        throw new UnsupportedOperationException();
    }

    public int getIdentifierPropertyId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SpaceIdType getSpaceIdType() {
        throw new UnsupportedOperationException();
    }

    public boolean isAutoGenerateId() {
        throw new UnsupportedOperationException();
    }

    public boolean isAutoGenerateRouting() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBlobstoreEnabled() {
        throw new UnsupportedOperationException();
    }


    public boolean supportsDynamicProperties() {
        throw new UnsupportedOperationException();
    }

    public boolean supportsOptimisticLocking() {
        throw new UnsupportedOperationException();
    }

    public PropertyInfo[] getProperties() {
        throw new UnsupportedOperationException();
    }

    public int getNumOfFixedProperties() {
        throw new UnsupportedOperationException();
    }

    public PropertyInfo getFixedProperty(int propertyID) {
        throw new UnsupportedOperationException();
    }

    public int getFixedPropertyPosition(String propertyName) {
        throw new UnsupportedOperationException();
    }

    public PropertyInfo getFixedProperty(String propertyName) {
        throw new UnsupportedOperationException();
    }

    public int getNumOfIndexedProperties() {
        throw new UnsupportedOperationException();
    }

    public int getIndexedPropertyID(int propertyID) {
        throw new UnsupportedOperationException();
    }

    public String[] getPropertiesNames() {
        throw new UnsupportedOperationException();
    }

    public String[] getPropertiesTypes() {
        throw new UnsupportedOperationException();
    }

    public boolean[] getPropertiesIndexTypes() {
        throw new UnsupportedOperationException();
    }

    public String getDefaultPropertyName() {
        throw new UnsupportedOperationException();
    }

    public boolean[] getIndexedFields() {
        throw new UnsupportedOperationException();
    }

    public EntryType getObjectType() {
        throw new UnsupportedOperationException();
    }

    public String[] getRestrictSuperClassesNames() {
        throw new UnsupportedOperationException();
    }

    public int getRoutingPropertyId() {
        throw new UnsupportedOperationException();
    }

    public String getRoutingPropertyName() {
        throw new UnsupportedOperationException();
    }

    public int getChecksum() {
        throw new UnsupportedOperationException();
    }

    public boolean hasInvalidFields() {
        throw new UnsupportedOperationException();
    }

    public boolean isFifoSupported() {
        throw new UnsupportedOperationException();
    }

    public boolean isFifoDefault() {
        throw new UnsupportedOperationException();
    }

    public FifoSupport getFifoSupport() {
        throw new UnsupportedOperationException();
    }

    public boolean isSystemType() {
        throw new UnsupportedOperationException();
    }

    public boolean isReplicable() {
        throw new UnsupportedOperationException();
    }

    public boolean isValidField(String fieldName) {
        throw new UnsupportedOperationException();
    }

    public void setIndexedFields(boolean[] indexedFields) {
        throw new UnsupportedOperationException();
    }

    public void setReplicable(boolean replicable) {
        throw new UnsupportedOperationException();
    }

    public Map<String, SpaceIndex> getIndexes() {
        throw new UnsupportedOperationException();
    }

    public SpaceIndexType getIndexType(String indexName) {
        throw new UnsupportedOperationException();
    }

    public byte getDotnetDynamicPropertiesStorageType() {
        throw new UnsupportedOperationException();
    }

    public String getDotnetDocumentWrapperTypeName() {
        throw new UnsupportedOperationException();
    }

    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        deserialize(in);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        deserialize(in);
    }

    private final void deserialize(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _className = IOUtils.readString(in);
        _superClassesNames = IOUtils.readStringArray(in);
    }

    public void writeExternal(ObjectOutput out)
            throws IOException {
        serialize(out);
    }

    private final void serialize(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, _className);
        IOUtils.writeStringArray(out, _superClassesNames);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        serialize(out);
    }

    @Override
    public boolean isAllPropertiesObjectStorageType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StorageType getStorageType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getFifoGroupingPropertyPath() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> getFifoGroupingIndexesPaths() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isConcreteType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSuperTypeName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TypeQueryExtensions getQueryExtensions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Serializable getVersionedSerializable() {
        throw new UnsupportedOperationException();
    }

    public List<SpaceIndex> getCompoundIndexes() {
        return null;
    }

    public boolean anyCompoundIndex() {
        return false;
    }

    @Override
    public ClassBinaryStorageAdapter getClassBinaryStorageAdapter() {
        return null;
    }

    @Override
    public String getPrimitivePropertiesWithoutNullValues() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasSequenceNumber() {
        return false;
    }

    ;

    @Override
    public int getSequenceNumberFixedPropertyID() {
        return -1;
    }

    ;

}
