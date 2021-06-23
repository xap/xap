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

import com.gigaspaces.document.DocumentProperties;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.entry.VirtualEntry;
import com.gigaspaces.internal.reflection.IConstructor;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.metadata.SpaceMetadataException;

import net.jini.core.lease.Lease;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Niv Ingberg
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class VirtualEntryIntrospector<T extends VirtualEntry> extends AbstractTypeIntrospector<T> {
    private static final long serialVersionUID = 1L;

    private static final IConstructor<VirtualEntry> DEFAULT_CTOR = new SpaceDocumentFactory();
    private final Class<T> _implClass;
    private final IConstructor<T> _constructor;

    /**
     * Required for Externalizable
     */
    public VirtualEntryIntrospector() {
        throw new IllegalStateException("This constructor is required for Externalizable and should not be called directly.");
    }

    public VirtualEntryIntrospector(ITypeDesc typeDesc, Class<T> documentWrapperClass) {
        super(typeDesc);
        this._implClass = documentWrapperClass;
        this._constructor = SpaceDocument.class.equals(_implClass) ? (IConstructor<T>) DEFAULT_CTOR : ReflectionUtil.createCtor(_implClass);
    }

    public Class<T> getType() {
        return _implClass;
    }

    public boolean hasUID(T target) {
        return getUID(target) != null;
    }

    public String getUID(T target, boolean isTemplate, boolean ignoreIdIfNotExists) {
        final String idPropertyName = _typeDesc.getIdPropertyName();

        // If no SpaceId property:
        if (idPropertyName == null || idPropertyName.length() == 0) {
            if (ignoreIdIfNotExists)
                return null;
            throw new SpaceMetadataException("Cannot get uid - SpaceId property is not defined.");
        }

        final Object id = target.getProperty(idPropertyName);

        // If SpaceId(autoGenerate=true):
        if (_typeDesc.isAutoGenerateId()) {
            if (id != null)
                return id.toString();
            if (ignoreIdIfNotExists)
                return null;

            throw new SpaceMetadataException("SpaceId(autogenerate=true) property value cannot be null.");
        }

        // If SpaceId(autoGenerate=false):
        // Do not generate uid for templates - required to support inheritance and SQLQuery.
        if (isTemplate)
            return null;

        // generate the uid from the id property and the type's name:
        if (id != null)
            return SpaceUidFactory.createUidFromTypeAndId(_typeDesc, id);

        throw new SpaceMetadataException("SpaceId(autogenerate=false) property value cannot be null.");
    }

    public boolean setUID(T target, String uid) {
        try {
            final String idPropertyName = _typeDesc.getIdPropertyName();

            // If no id property cannot set uid:
            if (idPropertyName == null || idPropertyName.length() == 0)
                return false;

            // If id property is autogenerate false cannot set uid:
            if (!_typeDesc.isAutoGenerateId())
                return false;

            // if id is already set do not override it:
            String fieldValue = target.getProperty(idPropertyName);
            if (fieldValue != null && fieldValue.length() != 0)
                return false;

            // Set uid:
            target.setProperty(idPropertyName, uid);
            return true;
        } catch (Exception e) {
            throw new SpaceMetadataException("Failed to set uid for type " + getType().getName(), e);
        }
    }

    public boolean hasVersionProperty(T target) {
        return true;
    }

    public int getVersion(T target) {
        return target.getVersion();
    }

    public boolean setVersion(T target, int version) {
        target.setVersion(version);
        return true;
    }

    public boolean hasTransientProperty(T target) {
        return true;
    }

    public boolean isTransient(T target) {
        if (target == null)
            return false;
        return target.isTransient();
    }

    public boolean setTransient(T target, boolean isTransient) {
        target.setTransient(isTransient);
        return true;
    }

    public boolean hasTimeToLiveProperty(T target) {
        // TODO: Implement once TTL/LeaseExpiration is well understood.
        return false;
    }

    public long getTimeToLive(T target) {
        // TODO: Implement once TTL/LeaseExpiration is well understood.
        return Lease.FOREVER;
    }

    public boolean setTimeToLive(T target, long ttl) {
        // TODO: Implement once TTL/LeaseExpiration is well understood.
        return false;
    }

    public void setEntryInfo(T target, String uid, int version, long timeToLive) {
        setUID(target, uid);
        setVersion(target, version);
        setTimeToLive(target, timeToLive);
    }

    @Override
    public T newInstance() {
        T instance = _constructor.newInstance();
        instance.setTypeName(_typeDesc.getTypeName());
        return instance;
    }

    @Override
    public T[] newArray(int length) {
        return _constructor.newArray(length);
    }

    @Override
    protected Object[] processDocumentObjectInterop(Object[] values, EntryType entryType, boolean cloneOnChange) {
        return entryType.isConcrete() ? toDocumentIfNeeded(values, cloneOnChange) : values;
    }

    public boolean hasDynamicProperties() {
        return true;
    }

    public Map<String, Object> getDynamicProperties(T target) {
        DocumentProperties dynamicProperties = new DocumentProperties(target.getProperties().size());
        for (Entry<String, Object> entry : target.getProperties().entrySet())
            if (_typeDesc.getFixedPropertyPosition(entry.getKey()) == -1)
                dynamicProperties.put(entry.getKey(), entry.getValue());
        return dynamicProperties.size() != 0 ? dynamicProperties : null;
    }

    public void setDynamicProperties(T target, Map<String, Object> dynamicProperties) {
        target.addProperties(dynamicProperties);
    }

    public Object getValue(T target, int index) {
        return target.getProperty(_typeDesc.getFixedProperty(index).getName());
    }

    public void setValue(T target, Object value, int index) {
        target.setProperty(_typeDesc.getFixedProperty(index).getName(), value);
    }

    @Override
    protected Object getDynamicProperty(T target, String name) {
        return target.getProperty(name);
    }

    @Override
    public void setDynamicProperty(T target, String name, Object value) {
        target.setProperty(name, value);
    }

    @Override
    public void unsetDynamicProperty(T target, String name) {
        target.removeProperty(name);
    }

    public Object[] getValues(T target) {
        Object[] result = new Object[_typeDesc.getNumOfFixedProperties()];
        for (int i = 0; i < result.length; i++) {
            if (_typeDesc.isAutoGenerateId() && i == _typeDesc.getIdentifierPropertyId())
                result[i] = null;
            else
                result[i] = getValue(target, i);
        }
        return result;
    }

    public void setValues(T target, Object[] values) {
        for (int i = 0; i < values.length; i++)
            setValue(target, values[i], i);
    }

    @Override
    public byte getExternalizableCode() {
        throw new IllegalStateException("This class does not support serialization.");
    }

    @Override
    public void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        throw new IOException("This class does not support serialization.");
    }

    @Override
    public void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        throw new IOException("This class does not support serialization.");
    }

    private static class SpaceDocumentFactory implements IConstructor<VirtualEntry> {
        static final long serialVersionUID = -8268338904975063950L;
        @Override
        public SpaceDocument newInstance() {
            return new SpaceDocument();
        }

        @Override
        public SpaceDocument[] newArray(int length) {
            return new SpaceDocument[length];
        }
    }
}
