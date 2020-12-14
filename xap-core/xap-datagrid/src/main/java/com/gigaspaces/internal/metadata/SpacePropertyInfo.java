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

import com.gigaspaces.client.storage_adapters.PropertyStorageAdapter;
import com.gigaspaces.client.storage_adapters.internal.PropertyStorageAdapterRegistry;
import com.gigaspaces.internal.metadata.converter.ConversionException;
import com.gigaspaces.internal.metadata.pojo.PojoPropertyInfo;
import com.gigaspaces.internal.reflection.IField;
import com.gigaspaces.internal.reflection.IGetterMethod;
import com.gigaspaces.internal.reflection.ISetterMethod;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.internal.reflection.standard.StandardReflectionFactory;
import com.gigaspaces.internal.utils.ClassUtils;
import com.gigaspaces.internal.utils.ReflectionUtils;
import com.gigaspaces.metadata.SpaceDocumentSupport;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.gigaspaces.metadata.StorageType;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

@com.gigaspaces.api.InternalApi
public class SpacePropertyInfo implements SpacePropertyDescriptor, Comparable<SpacePropertyInfo> {
    private final String _name;
    private final Class<?> _type;
    private final boolean _primitive;
    private final Method _getterMethod;
    private final Method _setterMethod;
    private IGetterMethod<Object> _getter;
    private ISetterMethod<Object> _setter;

    private Field _field;
    private IField<Object, Object> _fieldProperty;

    private int _level;
    private Primitive _nullValue;
    private StorageType _storageType = StorageType.DEFAULT;
    private Class<? extends PropertyStorageAdapter> _storageAdapterClass;
    private SpaceDocumentSupport _documentSupport;

    public SpacePropertyInfo(PojoPropertyInfo property) {
        this._name = property.getName();
        this._type = property.getType();
        this._primitive = ReflectionUtils.isPrimitive(_type.getName());
        this._getterMethod = property.getGetterMethod();
        this._setterMethod = property.getSetterMethod();
        this._level = -1;
    }

    @Override
    public String toString() {
        return "SpacePropertyInfo: [" + _name + "]";
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public Class<?> getType() {
        return _type;
    }

    public boolean isPrimitive() {
        return _primitive;
    }

    @Override
    public String getTypeName() {
        return _type.getName();
    }

    @Override
    public String getTypeDisplayName() {
        return ClassUtils.getTypeDisplayName(getTypeName());
    }

    public int getLevel() {
        return _level;
    }

    public void setLevel(int level) {
        this._level = level;
    }

    public Method getGetterMethod() {
        return _getterMethod;
    }

    public Method getSetterMethod() {
        return _setterMethod;
    }

    public boolean hasNullValue() {
        return _nullValue != null;
    }

    public Object getNullValue() {
        return _nullValue == null ? null : _nullValue.getValue();
    }

    public void setNullValue(String nullValue) {
        this._nullValue = (nullValue == null || nullValue.length() == 0
                ? null :
                new Primitive(_type, nullValue));
    }

    public void setField(Field field) {
        if (field != null)
            field.setAccessible(true);
        _field = field;
    }

    @Override
    public StorageType getStorageType() {
        return _storageType;
    }

    @Override
    public String getStorageAdapterName() {
        return _storageAdapterClass == null ? "" : PropertyStorageAdapterRegistry.getInstance().getOrCreate(_storageAdapterClass).getName();
    }

    public void setStorageType(StorageType storageType) {
        this._storageType = storageType;
    }

    public Class<? extends PropertyStorageAdapter> getStorageAdapterClass() {
        return _storageAdapterClass;
    }

    public void setStorageAdapterClass(Class<? extends PropertyStorageAdapter> storageAdapterClass) {
        this._storageAdapterClass = storageAdapterClass;
    }

    @Override
    public SpaceDocumentSupport getDocumentSupport() {
        return _documentSupport;
    }

    public void setDocumentSupport(SpaceDocumentSupport documentSupport) {
        this._documentSupport = documentSupport;
    }

    public Object getValue(Object target) {
        if (_getter == null) {
            synchronized (this) {
                if (_getter == null)
                    _getter = ReflectionUtil.createGetterMethod(_getterMethod);
            }
        }

        try {
            return _getter.get(target);
        } catch (IllegalArgumentException e) {
            // TODO: Log.
            throw new SpaceMetadataException("Failed to get property value.", e);
        } catch (IllegalAccessException e) {
            // TODO: Log.
            throw new SpaceMetadataException("Failed to get property value.", e);
        } catch (InvocationTargetException e) {
            // TODO: Log.
            throw new SpaceMetadataException("Failed to get property value.", e);
        }
    }

    public void setValue(Object target, Object value) {
        try {
            if (_setterMethod != null)
                setValueBySetter(target, value);
            else if (_field != null)
                setValueByField(target, value);
            else
                throw new SpaceMetadataException("Missing setter and field for property [" + getName() + "]");
        } catch (IllegalArgumentException e) {
            // TODO: Log.
            throw new SpaceMetadataException("Failed to set property value.", e);
        } catch (IllegalAccessException e) {
            // TODO: Log.
            throw new SpaceMetadataException("Failed to set property value.", e);
        } catch (InvocationTargetException e) {
            // TODO: Log.
            throw new SpaceMetadataException("Failed to set property value.", e);
        }
    }

    private void setValueByField(Object target, Object value)
            throws IllegalArgumentException, IllegalAccessException {
        if (_fieldProperty == null) {
            synchronized (this) {
                if (_fieldProperty == null) {
                    if (Modifier.isFinal(_field.getModifiers()))
                        _fieldProperty = new StandardReflectionFactory().getField(_field);
                    else
                        _fieldProperty = ReflectionUtil.createField(_field);
                }
            }
        }

        _fieldProperty.set(target, value);
    }

    private void setValueBySetter(Object target, Object value)
            throws IllegalArgumentException, IllegalAccessException,
            InvocationTargetException {
        if (_setter == null) {
            synchronized (this) {
                if (_setter == null)
                    _setter = ReflectionUtil.createSetterMethod(_setterMethod);
            }
        }

        _setter.set(target, value);
    }

    public Object convertFromNullIfNeeded(Object value) {
        return value == null && _nullValue != null
                ? _nullValue.getValue()
                : value;
    }

    public Object convertToNullIfNeeded(Object value) {
        return (value != null && _nullValue != null &&
                value.equals(_nullValue.getValue()))
                ? null : value;
    }

    @Override
    public int compareTo(SpacePropertyInfo other) {
        int levelCompare = other._level - this._level;
        if (levelCompare != 0)
            return levelCompare;

        return this._name.compareTo(other._name);
    }

    /**
     * This class store the primitive types : boolean, byte, short, char, int, long, float, double
     * in Primitive object
     *
     * @author Lior Ben Yizhak
     * @version 5.0
     */
    private static final class Primitive {
        /**
         * The primitive value stored in its java.lang wrapper class
         */
        private final Object _value;

        public Primitive(Class<?> type, String value) {
            if (value == null)
                throw new ConversionException("value can not be null.");

            if (type.equals(boolean.class))
                this._value = Boolean.parseBoolean(value);
            else if (type.equals(byte.class))
                this._value = Byte.parseByte(value);
            else if (type.equals(short.class))
                this._value = Short.parseShort(value);
            else if (type.equals(char.class)) {
                if (value.length() != 1)
                    throw new IllegalArgumentException(value + " contains more than 1 char.");
                this._value = value.charAt(0);
            } else if (type.equals(int.class))
                this._value = Integer.parseInt(value);
            else if (type.equals(long.class))
                this._value = Long.parseLong(value);
            else if (type.equals(float.class))
                this._value = Float.parseFloat(value);
            else if (type.equals(double.class))
                this._value = Double.parseDouble(value);
            else
                throw new ConversionException("The null value type class must be one of the following primitive types:" +
                        " boolean; byte; short; char; int; long; float; or double. Previously, it was : " + type);
        }

        public Object getValue() {
            return _value;
        }
    }
}
