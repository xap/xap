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

package com.gigaspaces.internal.reflection.standard;

import com.gigaspaces.internal.metadata.SpacePropertyInfo;
import com.gigaspaces.internal.reflection.IProperties;
import com.gigaspaces.internal.reflection.ReflectionUtil;

import java.lang.reflect.InvocationTargetException;

/**
 * Default implementation of IProperties based on Java Methods reflection.
 *
 * @author GuyK
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class StandardProperties<T> implements IProperties<T> {
    public static final String INTERNAL_NAME = ReflectionUtil.getInternalName(StandardProperties.class);
    public static final String CTOR_DESC = "([Lcom/gigaspaces/internal/metadata/SpacePropertyInfo;)V";

    private final SpacePropertyInfo[] _properties;

    public StandardProperties(SpacePropertyInfo[] properties) {
        _properties = properties;
        for (SpacePropertyInfo property : properties) {
            if (!property.getGetterMethod().isAccessible())
                property.getGetterMethod().setAccessible(true);
            if (property.getSetterMethod() != null && !property.getSetterMethod().isAccessible())
                property.getSetterMethod().setAccessible(true);
        }
    }

    public Object[] getValues(T obj) {
        Object[] results = new Object[_properties.length];
        for (int i = 0; i < results.length; ++i)
            results[i] = getValue(obj, i);
        return results;
    }

    public void setValues(T obj, Object[] values) {
        for (int i = 0; i < _properties.length; ++i)
            if (_properties[i].getSetterMethod() != null)
                setValue(obj, values[i], i);
    }

    public static final String GET_VALUE_NAME = "getValue";
    public static final String GET_VALUE_DESC = "(Ljava/lang/Object;I)Ljava/lang/Object;";

    // NOTE: this method is overridden in the byte-generated class.
    protected Object getValue(T obj, int i) {
        try {
            return _properties[i].getGetterMethod().invoke(obj);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Failed to get value of property " + _properties[i].getName() + " from " + obj, e);
        }
    }

    public static final String SET_VALUE_NAME = "setValue";
    public static final String SET_VALUE_DESC = "(Ljava/lang/Object;Ljava/lang/Object;I)V";

    // NOTE: this method is overridden in the byte-generated class.
    protected void setValue(T obj, Object value, int i) {
        try {
            if (!_properties[i].isPrimitive())
                _properties[i].getSetterMethod().invoke(obj, value);
            else if (_properties[i].hasNullValue())
                _properties[i].getSetterMethod().invoke(obj, convertFromNullIfNeeded(value, i));
            else if (value != null)
                _properties[i].getSetterMethod().invoke(obj, value);
            else {
                // property is primitive, has no null value and value is null:
                // setter cannot be invoked.
                // (The setter's underlying field will be initialized according to java's semantics).
            }
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Failed to set value of property " + _properties[i].getName() + " from " + obj, e);
        }
    }

    public static final String FROM_NULL_VALUE_NAME = "convertFromNullIfNeeded";
    public static final String FROM_NULL_VALUE_DESC = "(Ljava/lang/Object;I)Ljava/lang/Object;";

    // NOTE: this method is invoked from the byte-generated class as well.
    protected Object convertFromNullIfNeeded(Object value, int propertyIndex) {
        return _properties[propertyIndex].convertFromNullIfNeeded(value);
    }
}
