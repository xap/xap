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
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.converter.ConversionException;
import com.gigaspaces.internal.reflection.IConstructor;
import com.gigaspaces.internal.reflection.IParamsConstructor;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.metadata.SpaceMetadataValidationException;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.IGSEntry;

import net.jini.core.lease.Lease;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Pojo introspector for all the pojo implementations.
 *
 * @author Niv Ingberg
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class PojoIntrospector<T> extends AbstractTypeIntrospector<T> {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;
    public static final byte EXTERNALIZABLE_CODE = 1;

    private SpaceTypeInfo _typeInfo;
    private transient IConstructor<T> _constructor;
    private transient SpacePropertyInfo _idProperty;
    private transient SpacePropertyInfo _versionProperty;
    private transient SpacePropertyInfo _timeToLiveProperty;
    private transient SpacePropertyInfo _transientProperty;
    private transient SpacePropertyInfo _dynamicPropertiesProperty;

    private transient boolean _isAutoPkGen;
    private transient boolean _transient;
    private transient Class<T> _pojoClass;

    /**
     * Default constructor for Externalizable.
     */
    public PojoIntrospector() {
    }

    public PojoIntrospector(ITypeDesc typeDesc) {
        super(typeDesc);
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(typeDesc.getObjectClass());
        init(typeInfo);
    }

    public Class<T> getType() {
        return _pojoClass;
    }

    public T newInstance() {
        return _constructor.newInstance();
    }

    @Override
    public T toObject(IGSEntry entry, ITypeDesc typeDesc) {
        if (!_typeInfo.hasConstructorProperties())
            return super.toObject(entry, typeDesc);

        try {
            return instantiateObjectByConstructor(entry.getFieldsValues(),
                    null /* dynamicProperties */,
                    entry.getUID(),
                    entry.getVersion(),
                    entry.getTimeToLive(),
                    entry.isTransient());
        } catch (Exception e) {
            throw new ConversionException(e);
        }
    }

    @Override
    protected T instantiateObject(
            Object[] values,
            Map<String, Object> dynamicProperties,
            String uid,
            int version,
            long timeToLive,
            boolean isTransient) {
        if (!_typeInfo.hasConstructorProperties()) {
            return super.instantiateObject(values, dynamicProperties, uid, version, timeToLive, isTransient);
        }

        return instantiateObjectByConstructor(values, dynamicProperties, uid, version, timeToLive, isTransient);
    }

    @SuppressWarnings("unchecked")
    private T instantiateObjectByConstructor(
            Object[] values,
            Map<String, Object> dynamicProperties,
            String uid,
            int version,
            long timeToLive,
            boolean isTransient) {
        return (T) _typeInfo.createConstructorBasedInstance(values,
                dynamicProperties,
                uid,
                version,
                calculateLease(timeToLive),
                !isTransient);
    }

    @Override
    protected Object[] processDocumentObjectInterop(Object[] values, EntryType entryType, boolean cloneOnChange) {
        return entryType.isVirtual() ? fromDocumentIfNeeded(values, cloneOnChange) : values;
    }

    public int getVersion(T target) {
        if (_versionProperty == null)
            return 0;

        Integer version = (Integer) _versionProperty.getValue(target);
        return version == null ? 0 : version.intValue();
    }

    public boolean setVersion(T target, int version) {
        if (_versionProperty == null)
            return false;

        try {
            _versionProperty.setValue(target, version);
            return true;
        } catch (Throwable e) {
            throw new ConversionException("Failed to set pojo version", e);
        }
    }

    public long getTimeToLive(T target) {
        if (_timeToLiveProperty == null)
            return Lease.FOREVER;

        Long ttl = (Long) _timeToLiveProperty.getValue(target);
        long realTTL = (ttl == null ? 0 : ttl.longValue());
        return realTTL - SystemTime.timeMillis();
    }

    public boolean setTimeToLive(T target, long ttl) {
        if (_timeToLiveProperty == null)
            return false;

        try {
            _timeToLiveProperty.setValue(target, calculateLease(ttl));
            return true;
        } catch (Throwable e) {
            throw new ConversionException("Failed to set pojo lease.", e);
        }
    }

    private static long calculateLease(long ttl) {
        return ttl == Long.MAX_VALUE ? ttl : ttl + SystemTime.timeMillis();
    }

    public boolean isTransient(T target) {
        if (_transientProperty == null || target == null)
            return _transient;
        try {
            Boolean persistent = (Boolean) getValue(target, _transientProperty);
            return persistent == null ? true : !persistent;
        } catch (Exception e) {
            throw new ConversionException("Failed to get pojo transient", e);
        }
    }

    public boolean setTransient(T target, boolean isTransient) {
        if (_transientProperty == null)
            return false;

        try {
            _transientProperty.setValue(target, !isTransient);
            return true;
        } catch (Exception e) {
            throw new ConversionException("Failed to set pojo transient", e);
        }
    }

    public boolean hasDynamicProperties() {
        return _dynamicPropertiesProperty != null;
    }

    public Map<String, Object> getDynamicProperties(T target) {
        if (_dynamicPropertiesProperty == null)
            return null;
        Map<String, Object> properties = (Map<String, Object>) _dynamicPropertiesProperty.getValue(target);
        if (properties == null)
            return null;

        DocumentProperties dynamicProperties = new DocumentProperties(properties.size());
        for (Entry<String, Object> entry : properties.entrySet()) {
            if (_typeDesc.getFixedPropertyPosition(entry.getKey()) != -1)
                throw new SpaceMetadataException("Illegal dynamic property '" + entry.getKey() + "' in type '" + _typeDesc.getTypeName() + "' - this property is defined as a fixed property.");
            dynamicProperties.put(entry.getKey(), entry.getValue());
        }
        return dynamicProperties;
    }

    public void setDynamicProperties(T target, Map<String, Object> dynamicProperties) {
        if (_dynamicPropertiesProperty == null)
            return;

        if (dynamicProperties == null)
            _dynamicPropertiesProperty.setValue(target, null);
        else {
            Map<String, Object> properties = (Map<String, Object>) _dynamicPropertiesProperty.getValue(target);
            if (properties == null) {
                properties = new DocumentProperties();
                _dynamicPropertiesProperty.setValue(target, properties);
            }
            properties.putAll(dynamicProperties);
        }
    }

    public Object[] getValues(T target) {
        try {
            final Object[] values = _typeInfo.getSpacePropertiesValues(target, false);

            for (int i = 0; i < values.length; i++) {
                SpacePropertyInfo property = _typeInfo.getProperty(i);

                if (_isAutoPkGen && property == _idProperty)
                    values[i] = null;
                else
                    values[i] = property.convertToNullIfNeeded(values[i]);
            }

            return values;
        } catch (Exception e) {
            throw new ConversionException(e);
        }
    }

    public void setValues(T target, Object[] values) {
        try {
            _typeInfo.setSpacePropertiesValues(target, values);
        } catch (Exception e) {
            throw new ConversionException(e);
        }
    }

    public Object getValue(T target, int index) {
        try {
            return getValue(target, _typeInfo.getProperty(index));
        } catch (Exception e) {
            throw new ConversionException(e);
        }
    }

    private Object getValue(T target, SpacePropertyInfo property) {
        Object value = property.getValue(target);
        return property.convertToNullIfNeeded(value);
    }

    public void setValue(T target, Object value, int index) {
        try {
            setValue(target, _typeInfo.getProperty(index), value);
        } catch (Exception e) {
            throw new ConversionException(e);
        }
    }

    private void setValue(T target, SpacePropertyInfo property, Object value) {
        value = property.convertFromNullIfNeeded(value);
        property.setValue(target, value);
    }

    protected Object getDynamicProperty(T target, String name) {
        if (_dynamicPropertiesProperty == null)
            return null;

        Map<String, Object> properties = (Map<String, Object>) _dynamicPropertiesProperty.getValue(target);
        if (properties == null)
            return null;

        return properties.get(name);
    }

    public void setDynamicProperty(T target, String name, Object value) {
        if (_dynamicPropertiesProperty != null) {
            Map<String, Object> properties = (Map<String, Object>) _dynamicPropertiesProperty.getValue(target);
            if (properties == null) {
                properties = new DocumentProperties();
                _dynamicPropertiesProperty.setValue(target, properties);
            }

            properties.put(name, value);
        }
    }

    @Override
    public void unsetDynamicProperty(T target, String name) {
        if (_dynamicPropertiesProperty != null) {
            Map<String, Object> properties = (Map<String, Object>) _dynamicPropertiesProperty.getValue(target);
            if (properties != null)
                properties.remove(name);
        }
    }

    private void init(SpaceTypeInfo typeInfo) {
        this._typeInfo = typeInfo;
        this._pojoClass = (Class<T>) typeInfo.getType();

        if (typeInfo.hasConstructorProperties()) {
            // validation only, the instantiation logic exists in typeInfo
            IParamsConstructor<?> paramsConstructor = typeInfo.getParamsConstructor();
            if (paramsConstructor == null)
                throw new SpaceMetadataValidationException(_pojoClass, "Missing expected constructor");
        } else {
            this._constructor = (IConstructor<T>) typeInfo.getDefaultConstructor();
            if (_constructor == null)
                throw new SpaceMetadataValidationException(_pojoClass, "Must be a public static class and have a default constructor.");
        }

        _idProperty = typeInfo.getIdProperty();
        _versionProperty = typeInfo.getVersionProperty();
        _timeToLiveProperty = typeInfo.getLeaseExpirationProperty();
        _transientProperty = typeInfo.getPersistProperty();
        _dynamicPropertiesProperty = typeInfo.getDynamicPropertiesProperty();

        _isAutoPkGen = typeInfo.getIdAutoGenerate();
        _transient = !typeInfo.isPersist();
    }

    public String getUID(T target, boolean isTemplate, boolean ignoreIdIfNotExists) {
        // If no SpaceId property:
        if (_idProperty == null) {
            if (ignoreIdIfNotExists)
                return null;
            throw new SpaceMetadataException("Cannot get uid - SpaceId property is not defined.");
        }

        final Object id = getValue(target, _idProperty);

        // If SpaceId(autoGenerate=true):
        if (_isAutoPkGen) {
            if (id != null)
                return id.toString();
            if (ignoreIdIfNotExists)
                return null;

            throw new SpaceMetadataException("SpaceId(autoGenerate=true) property value cannot be null.");
        }

        // If SpaceId(autoGenerate=false):
        // Do not generate uid for templates - required to support inheritance and SQLQuery.
        if (isTemplate)
            return null;

        // generate the uid from the id property and the type's name:
        if (id != null)
            return SpaceUidFactory.createUidFromTypeAndId(getTypeDesc(), id);

        throw new SpaceMetadataException("SpaceId(autoGenerate=false) property value cannot be null.");
    }

    public boolean setUID(T target, String uid) {
        try {
            // If no id property cannot set uid:
            if (_idProperty == null)
                return false;

            // If id property is autoGenerate false cannot set uid:
            if (!_isAutoPkGen)
                return false;

            // if id is already set do not override it:
            String fieldValue = (String) getValue(target, _idProperty);
            if (fieldValue != null && fieldValue.length() != 0)
                return false;

            // Set uid:
            setValue(target, _idProperty, uid);
            return true;
        } catch (Exception e) {
            throw new SpaceMetadataException("Failed to set uid for type " + getType().getName(), e);
        }
    }

    public void setEntryInfo(T target, String uid, int version, long ttl) {
        setUID(target, uid);
        setVersion(target, version);
    }

    public boolean hasTimeToLiveProperty(T target) {
        return _timeToLiveProperty != null;
    }

    public boolean hasTransientProperty(T target) {
        return _transientProperty != null;
    }

    public boolean hasUID(T target) {
        return _idProperty != null && _isAutoPkGen && getValue(target, _idProperty) != null;
    }

    public boolean hasVersionProperty(T target) {
        return _versionProperty != null;
    }

    @Override
    public boolean propertyHasNullValue(int position) {
        return _typeInfo.getProperty(position).hasNullValue();
    }

    @Override
    public boolean hasConstructorProperties() {
        return _typeInfo.hasConstructorProperties();
    }

    @Override
    public byte getExternalizableCode() {
        return EXTERNALIZABLE_CODE;
    }

    @Override
    public void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        super.readExternal(in, version);
        if (version.greaterOrEquals(PlatformLogicalVersion.v10_1_0))
            readExternalV10_1(in, version);
        else
            readExternalV7_1(in);
    }

    private void readExternalV10_1(ObjectInput in, PlatformLogicalVersion version) throws IOException, ClassNotFoundException {
        SpaceTypeInfo typeInfo = new SpaceTypeInfo();
        typeInfo.readExternal(in, version);
        init(typeInfo);
    }

    private void readExternalV7_1(ObjectInput in) throws IOException, ClassNotFoundException {
        SpaceTypeInfo typeInfo = IOUtils.readObject(in);
        init(typeInfo);
    }

    /**
     * NOTE: if you change this method, you need to make this class ISwapExternalizable
     */
    @Override
    public void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        super.writeExternal(out, version);
        if (version.greaterOrEquals(PlatformLogicalVersion.v10_1_0))
            writeExternalV10_1(out, version);
        else
            writeExternalV7_1(out);
    }

    private void writeExternalV10_1(ObjectOutput out, PlatformLogicalVersion version) throws IOException {
        _typeInfo.writeExternal(out, version);
    }

    private void writeExternalV7_1(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _typeInfo);
    }
}
