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

import com.gigaspaces.internal.client.utils.SerializationUtil;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.ClassUtils;
import com.gigaspaces.internal.utils.ReflectionUtils;
import com.gigaspaces.metadata.SpaceDocumentSupport;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.gigaspaces.metadata.StorageType;
import com.j_spaces.kernel.ClassLoaderHelper;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Represents an entry property.
 *
 * @author Niv Ingberg
 * @since 7.0 NOTE: Starting 8.0 this class is not serialized - Externalizable code is maintained
 * for backwards compatibility only.
 */
@com.gigaspaces.api.InternalApi
public class PropertyInfo implements SpacePropertyDescriptor, Externalizable {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    private String _name;
    private String _typeName;
    private boolean _primitive;
    private boolean _spacePrimitive;
    private boolean _comparable;
    private Class<?> _type;
    private SpaceDocumentSupport _documentSupport;
    private StorageType _storageType;
    private byte _dotnetStorageType;

    /**
     * Default constructor for Externalizable.
     */
    public PropertyInfo() {
    }

    public PropertyInfo(String name, String typeName, SpaceDocumentSupport documentSupport, StorageType storageType) {
        this(name, typeName, null, documentSupport, storageType, DotNetStorageType.NULL);
    }

    public PropertyInfo(String name, Class<?> type, SpaceDocumentSupport documentSupport, StorageType storageType) {
        this(name, type.getName(), type, documentSupport, storageType, DotNetStorageType.NULL);
    }

    public PropertyInfo(String name, String typeName, Class<?> type, SpaceDocumentSupport documentSupport, StorageType storageType, byte dotNetStorageType) {
        this._name = name;
        this._typeName = typeName;
        this._type = (type == null) ? getTypeFromName(typeName) : type;
        this._primitive = ReflectionUtils.isPrimitive(typeName);
        this._spacePrimitive = ReflectionUtils.isSpacePrimitive(_typeName);
        this._documentSupport = documentSupport != SpaceDocumentSupport.DEFAULT
                ? documentSupport
                : SpaceDocumentSupportHelper.getDefaultDocumentSupport(_type);
        this._storageType = storageType;
        this._dotnetStorageType = dotNetStorageType;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public String getTypeName() {
        return _typeName;
    }

    @Override
    public String getTypeDisplayName() {
        return ClassUtils.getTypeDisplayName(_typeName);
    }

    @Override
    public Class<?> getType() {
        return _type;
    }

    @Override
    public SpaceDocumentSupport getDocumentSupport() {
        return _documentSupport;
    }

    @Override
    public StorageType getStorageType() {
        return _storageType;
    }

    public void setDefaultStorageType(StorageType defaultStorageType) {
        _storageType = _spacePrimitive ? StorageType.OBJECT : defaultStorageType;
    }

    public byte getDotnetStorageType() {
        return _dotnetStorageType;
    }

    public boolean isPrimitive() {
        return _primitive;
    }

    public boolean isSpacePrimitive() {
        return _spacePrimitive;
    }

    @Override
    public String toString() {
        return "Property[name=" + _name + ", type=" + _typeName + "]";
    }

    public Object beforeSerialize(Object value)
            throws IOException {
        if (_spacePrimitive)
            return value;
        return SerializationUtil.serializeFieldValue(value, _storageType);
    }

    public Object afterDeserialize(Object value)
            throws IOException, ClassNotFoundException {
        if (_spacePrimitive)
            return value;
        return SerializationUtil.deSerializeFieldValue(value, _storageType);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        _name = IOUtils.readString(in);
        _typeName = IOUtils.readString(in);
        _spacePrimitive = in.readBoolean();
        _comparable = in.readBoolean();
        _type = getTypeFromName(_typeName);
        _documentSupport = SpaceDocumentSupport.DEFAULT;
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        IOUtils.writeString(out, _name);
        IOUtils.writeString(out, _typeName);
        out.writeBoolean(_spacePrimitive);
        out.writeBoolean(_comparable);
    }

    private static Class<?> getTypeFromName(String typeName) {
        if (typeName == null || typeName.length() == 0)
            return Object.class;

        try {
            return ClassLoaderHelper.loadClass(typeName);
        } catch (ClassNotFoundException e) {
            return Object.class;
        }
    }

    public boolean isCommonJavaType() {
        return ReflectionUtils.isCommonJavaType(_typeName);
    }
}
