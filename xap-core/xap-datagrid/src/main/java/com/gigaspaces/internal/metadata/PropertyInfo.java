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
import com.gigaspaces.internal.version.PlatformLogicalVersion;
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

    private PropertyInfo(Builder builder) {
        this._name = builder.name;
        this._typeName = builder.typeName;
        this._type = (builder.type == null) ? getTypeFromName(_typeName) : builder.type;
        this._primitive = ReflectionUtils.isPrimitive(_typeName);
        this._spacePrimitive = ReflectionUtils.isSpacePrimitive(_typeName);
        this._documentSupport = builder.documentSupport != SpaceDocumentSupport.DEFAULT
                ? builder.documentSupport
                : SpaceDocumentSupportHelper.getDefaultDocumentSupport(_type);
        this._storageType = builder.storageType;
        this._dotnetStorageType = builder.dotnetStorageType;
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

    public static Builder builder(String name) {
        return new Builder(name);
    }

    void serialize(ObjectOutput out, PlatformLogicalVersion version) throws IOException {
        IOUtils.writeString(out, _name);
        IOUtils.writeString(out, _typeName);
        IOUtils.writeObject(out, _type);
        // Removed in 8.0.4: primitive is calculated from typename.
        //out.writeBoolean(property.isPrimitive());
        // New in 8.0.1: write SpaceDocumentSupport code.
        out.writeByte(SpaceDocumentSupportHelper.toCode(_documentSupport));
        // New in 9.0.0: write storage type as code.
        out.writeInt(_storageType.getCode());
        // Changed in 8.0.4: write dotnet storage type as code instead of object
        out.writeByte(_dotnetStorageType);
    }

    static PropertyInfo deserialize(ObjectInput in, PlatformLogicalVersion version) throws IOException, ClassNotFoundException {
        Builder builder = new Builder(IOUtils.readString(in));
        builder.typeName = IOUtils.readString(in);
        builder.type = IOUtils.readObject(in);
        // Removed in 8.0.4: primitive is calculated from typename.
        //boolean isPrimitive = in.readBoolean();
        // New in 8.0.1: read SpaceDocumentSupport code
        builder.documentSupport = SpaceDocumentSupportHelper.fromCode(in.readByte());
        // New in 9.0.0: read storage type code
        builder.storageType = StorageType.fromCode(in.readInt());
        // Changed in 8.0.4: read dotnet storage type as code instead of object.
        builder.dotnetStorageType = in.readByte();
        return builder.build();
    }

    public static class Builder {
        private final String name;
        private Class<?> type;
        private String typeName;
        private SpaceDocumentSupport documentSupport;
        private StorageType storageType;
        private byte dotnetStorageType = DotNetStorageType.NULL;

        public Builder(String name) {
            this.name = name;
        }

        public PropertyInfo build() {
            return new PropertyInfo(this);
        }

        public Builder type(Class<?> type) {
            this.type = type;
            this.typeName = type.getName();
            return this;
        }

        public Builder type(String typeName) {
            this.type = null;
            this.typeName = typeName;
            return this;
        }

        public Builder documentSupport(SpaceDocumentSupport documentSupport) {
            this.documentSupport = documentSupport;
            return this;
        }

        public Builder storageType(StorageType storageType) {
            this.storageType = storageType;
            return this;
        }

        public Builder dotNetStorageType(byte dotnetStorageType) {
            this.dotnetStorageType = dotnetStorageType;
            return this;
        }
    }
}
