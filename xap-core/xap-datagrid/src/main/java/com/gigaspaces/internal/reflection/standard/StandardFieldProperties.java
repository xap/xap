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

import com.gigaspaces.internal.reflection.IProperties;

import java.lang.reflect.Field;

/**
 * Default implementation of IProperties based on Java Fields reflection.
 *
 * @author GuyK
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class StandardFieldProperties<T> implements IProperties<T> {
    private final Field[] _fields;

    public StandardFieldProperties(Field[] fields) {
        _fields = fields;
    }

    public Object[] getValues(T obj) {
        Object[] results = new Object[_fields.length];
        int i = 0;
        for (; i < _fields.length; ++i) {
            try {
                results[i] = _fields[i].get(obj);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Failed to get field " + _fields[i].getName() + " from " + obj, e);
            }
        }
        return results;
    }

    public void setValues(T obj, Object[] values) {
        int i = 0;
        try {
            for (; i < _fields.length; ++i)
                _fields[i].set(obj, values[i]);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Failed to set field " + _fields[i].getName() + " to " + values[i] + " on " + obj, e);
        }
    }
}
