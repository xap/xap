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

import com.gigaspaces.entry.VirtualEntry;
import com.gigaspaces.internal.client.StorageTypeDeserialization;
import com.gigaspaces.internal.document.DocumentObjectConverterInternal;
import com.gigaspaces.internal.metadata.converter.ConversionException;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.j_spaces.core.IGSEntry;

import net.jini.space.InternalSpaceException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Common base class for ITypeIntrospector implementations.
 *
 * @author Niv Ingberg
 * @since 7.0
 */
public abstract class AbstractTypeIntrospector<T> implements ITypeIntrospector<T> {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    protected static DocumentObjectConverterInternal _documentObjectConverter = DocumentObjectConverterInternal.instance();

    protected transient ITypeDesc _typeDesc;

    private final ConcurrentHashMap<String, Class<?>> _nestedPropertiesIndex = new ConcurrentHashMap<String, Class<?>>();

    /**
     * Default constructor for Externalizable.
     */
    protected AbstractTypeIntrospector() {
    }

    protected AbstractTypeIntrospector(ITypeDesc typeDesc) {
        _typeDesc = typeDesc;
    }

    public void initialize(ITypeDesc typeDesc) {
        this._typeDesc = typeDesc;
    }

    public ITypeDesc getTypeDesc() {
        return _typeDesc;
    }

    public String getUID(T target) {
        return getUID(target, false, true);
    }

    public Object getRouting(T target) {
        String routingPropertyName = _typeDesc.getRoutingPropertyName();
        if (routingPropertyName == null)
            return null;
        return getValue(target, routingPropertyName);
    }

    public T toObject(IEntryPacket packet) {
        return toObject(packet, StorageTypeDeserialization.EAGER);
    }

    public T toObject(IEntryPacket packet, StorageTypeDeserialization storageTypeDeserialization) {
        try {
            Object[] originalValues = packet.getFieldValues();
            Object[] values = storageTypeDeserialization == StorageTypeDeserialization.EAGER
                    ? deserializeValues(originalValues)
                    : originalValues;
            values = processDocumentObjectInterop(values, packet.getEntryType(), values == originalValues);
            return instantiateObject(values,
                    packet.getDynamicProperties(),
                    packet.getUID(),
                    packet.getVersion(),
                    packet.getTTL(),
                    packet.isTransient());

        } catch (IOException | ClassNotFoundException e) {
            throw new ConversionException(e);
        }
    }

    protected T instantiateObject(
            Object[] values,
            Map<String, Object> dynamicProperties,
            String uid,
            int version,
            long timeToLive,
            boolean isTransient) {
        final T res = newInstance();
        if (values != null)
            setValues(res, values);
        setDynamicProperties(res, dynamicProperties);
        setEntryInfo(res, uid, version, timeToLive);
        setTransient(res, isTransient);
        setTimeToLive(res, timeToLive);
        return res;
    }

    protected abstract Object[] processDocumentObjectInterop(Object[] values, EntryType entryType, boolean cloneOnChange);

    protected Object[] toDocumentIfNeeded(Object[] values, boolean cloneOnChange) {
        if (values == null || values.length == 0)
            return values;
        Object[] result = cloneOnChange ? new Object[values.length] : values;
        for (int i = 0; i < values.length; i++) {
            PropertyInfo property = _typeDesc.getFixedProperty(i);
            result[i] = _documentObjectConverter.toDocumentIfNeeded(values[i], property.getDocumentSupport());
        }
        return result;
    }

    protected Object[] fromDocumentIfNeeded(Object[] values, boolean cloneOnChange) {
        if (values == null || values.length == 0)
            return values;
        Object[] result = cloneOnChange ? new Object[values.length] : values;
        for (int i = 0; i < values.length; i++) {
            PropertyInfo property = _typeDesc.getFixedProperty(i);
            result[i] = _documentObjectConverter.fromDocumentIfNeeded(values[i], property.getDocumentSupport(), property.getType());
        }
        return result;
    }

    public T toObject(IGSEntry entry, ITypeDesc typeDesc) {
        T result = newInstance();
        setValues(result, entry.getFieldsValues());
        setEntryInfo(result, entry.getUID(), entry.getVersion(), entry.getTimeToLive());
        setTransient(result, entry.isTransient());
        return result;
    }

    public Object[] getSerializedValues(T target) {
        final Object[] values = getValues(target);
        serializeValues(values);
        return values;
    }

    private void serializeValues(Object[] values) {
        if (values == null)
            return;
        if (_typeDesc.isAllPropertiesObjectStorageType())
            return;

        try {
            for (int i = 0; i < values.length; i++)
                values[i] = _typeDesc.getFixedProperty(i).beforeSerialize(values[i]);
        } catch (IOException e) {
            throw new ConversionException(e);
        }
    }

    private Object[] deserializeValues(Object[] values) throws IOException, ClassNotFoundException {
        if (values == null)
            return null;
        if (_typeDesc.isAllPropertiesObjectStorageType())
            return values;

        final Object[] clonedValues = new Object[values.length];
        System.arraycopy(values, 0, clonedValues, 0, values.length);
        for (int i = 0; i < values.length; i++)
            clonedValues[i] = _typeDesc.getFixedProperty(i).afterDeserialize(values[i]);

        return clonedValues;
    }

    protected abstract T newInstance();

    public Object getValue(T target, String name) {
        final int pos = _typeDesc.getFixedPropertyPosition(name);

        if (pos != -1)
            return getValue(target, pos);

        if (!_typeDesc.supportsDynamicProperties())
            throw new IllegalArgumentException("Failed to get value of property '" + name + "' in type '" + _typeDesc.getTypeName() +
                    "' - it does not exist and the type does not support dynamic properties.");

        return getDynamicProperty(target, name);
    }

    public void setValue(T target, String name, Object value) {
        final int pos = _typeDesc.getFixedPropertyPosition(name);

        if (pos != -1)
            setValue(target, value, pos);
        else {
            if (!_typeDesc.supportsDynamicProperties())
                throw new IllegalArgumentException("Failed to set value of property '" + name + "' in type '" + _typeDesc.getTypeName() +
                        "' - it does not exist and the type does not support dynamic properties.");

            setDynamicProperty(target, name, value);
        }
    }

    protected abstract Object getDynamicProperty(T target, String name);

    public boolean supportsNestedOperations() {
        return true;
    }

    public Class<?> getPathType(String path) {
        Class<?> type = _nestedPropertiesIndex.get(path);

        if (type == null) {
            type = getNestedPathType(path);
            if (type != null)
                _nestedPropertiesIndex.putIfAbsent(path, type);
        }

        return type;
    }

    private Class<?> getNestedPathType(String path) {
        // Split path:
        final String[] tokens = path.split("\\.");
        if (tokens == null || tokens.length == 0)
            throw new InternalSpaceException("Error splitting property path [" + path + "].");

        int i = 0;
        Class<?> type = null;
        while (true) {
            // Get current token:
            final String token = tokens[i++];
            // Get property in current type by token (error if none):
            SpacePropertyDescriptor property = type == null
                    ? _typeDesc.getFixedProperty(token)
                    : SpaceTypeInfoRepository.getTypeInfo(type).getProperty(token);

            if (property == null) {
                if (_typeDesc.supportsDynamicProperties())
                    return void.class;
                String currentPath = StringUtils.join(tokens, ".", 0, i);
                throw new SpaceMetadataException("Unable to get nested property type for path [" + currentPath + "].");
            }
            if (ObjectUtils.equals(property.getType(), Object.class)) {
                if (property.getTypeName() != null)
                    return void.class;
                String ownerTypeName = type != null ? type.getName() : _typeDesc.getTypeName();
                throw new SpaceMetadataException("Cannot get type of property " + property.getName() + " in class " + ownerTypeName);
            }

            type = property.getType();
            // If this is the last token, return result:
            if (i == tokens.length)
                return type;

            // If this property is a map, stop processing:
            if (java.util.Map.class.isAssignableFrom(type) || VirtualEntry.class.isAssignableFrom(type))
                return void.class;
        }
    }


    public Object getPathValue(IEntryPacket entryPacket, String path) {
        // Split path to properties:
        final String[] tokens = path.split("\\.");
        if (tokens == null || tokens.length == 0)
            throw new InternalSpaceException("Error splitting property path [" + path + "].");

        Object object = entryPacket.getPropertyValue(tokens[0]);
        return getPathValue(object, tokens, path);
    }

    public static Object getPathValue(Object object, String[] tokens, String path) {
        for (int i = 1; i < tokens.length; i++) {
            // If null abort match (avoid NPE).
            if (object == null)
                return null;

            object = getNestedValue(object, i, tokens, null, path);
        }

        return object;
    }

    /**
     * Gets the next nested property value according to the provided next token index.
     */
    public static Object getNestedValue(Object value, int position, String[] tokens, SpacePropertyInfo[] propertyInfoCache, String originalPath) {
        if (position == tokens.length)
            throw new IllegalArgumentException("[*] can only be used on Collection properties.");

        if (value instanceof Map)
            return ((Map<?, ?>) value).get(tokens[position]);
        if (value instanceof VirtualEntry)
            return ((VirtualEntry) value).getProperty(tokens[position]);

        SpacePropertyInfo propertyInfo;
        if (propertyInfoCache != null && propertyInfoCache[position] != null)
            propertyInfo = propertyInfoCache[position];
        else {
            Class<? extends Object> type = value.getClass();
            SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);
            propertyInfo = typeInfo.getProperty(tokens[position]);
            if (propertyInfo == null)
                throw new IllegalArgumentException("Property '" + tokens[position] + "' is not a member of " + type.getName() + " in '" + originalPath + "'");

            if (propertyInfoCache != null)
                propertyInfoCache[position] = propertyInfo;
        }

        return propertyInfo.getValue(value);
    }

    @Override
    public boolean propertyHasNullValue(int position) {
        return false;
    }

    @Override
    public boolean hasConstructorProperties() {
        return false;
    }

    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        readExternal(in, LRMIInvocationContext.getEndpointLogicalVersion());
    }

    public void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
    }

    public void writeExternal(ObjectOutput out)
            throws IOException {
        writeExternal(out, LRMIInvocationContext.getEndpointLogicalVersion());
    }

    /**
     * NOTE: if you change this method, you need to make this class ISwapExternalizable
     */
    public void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
    }

}
