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

package com.gigaspaces.internal.reflection;

import com.gigaspaces.internal.metadata.SpaceTypeInfo;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Interface for reflection enhancement factory.
 *
 * @author Guy
 * @since 7.0
 */
public interface IReflectionFactory {
    <T> IConstructor<T> getConstructor(Constructor<T> ctor);

    <T> IParamsConstructor<T> getParamsConstructor(Constructor<T> ctor);

    <T> String[] getConstructorParametersNames(Constructor<T> ctor);

    default <T> IMethod<T> getMethod(Method method) {
        return getMethod(null, method);
    }

    <T> IMethod<T> getMethod(ClassLoader classLoader, Method method);

    default <T> IMethod<T>[] getMethods(Method[] methods) {
        return getMethods(null, methods);
    }

    default <T> IMethod<T>[] getMethods(ClassLoader classLoader, Method[] methods) {
        final int length = methods.length;
        IMethod<T>[] result = new IMethod[length];

        for (int i = 0; i < length; ++i)
            result[i] = getMethod(classLoader, methods[i]);

        return result;
    }

    default <T> IGetterMethod<T> getGetterMethod(Method method) {
        return getGetterMethod(null, method);
    }

    <T> IGetterMethod<T> getGetterMethod(ClassLoader classLoader, Method method);

    default <T> ISetterMethod<T> getSetterMethod(Method method) {
        return getSetterMethod(null, method);
    }

    <T> ISetterMethod<T> getSetterMethod(ClassLoader classLoader, Method method);

    <T, F> IField<T, F> getField(Field field);

    default <T, F> IField<T, F>[] getFields(Class<T> clazz) {
        final Field[] fields = clazz.getFields();
        final int length = fields.length;
        final IField<T, F>[] result = new IField[length];

        for (int i = 0; i < length; ++i)
            result[i] = getField(fields[i]);

        return result;
    }

    <T> IProperties<T> getProperties(SpaceTypeInfo typeInfo);

    <T> IProperties<T> getFieldProperties(Class<T> declaringClass, Field[] fields);

    Object getProxy(ClassLoader loader, Class<?>[] interfaces, ProxyInvocationHandler handler, boolean allowCache);
}
