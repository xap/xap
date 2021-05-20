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

import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.reflection.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

@com.gigaspaces.api.InternalApi
public class StandardReflectionFactory implements IReflectionFactory {
    public <T> IConstructor<T> getConstructor(Constructor<T> ctor) {
        return new StandardConstructor<T>(ctor);
    }

    public <T> IParamsConstructor<T> getParamsConstructor(Constructor<T> ctor) {
        return new StandardParamsConstructor<T>(ctor);
    }

    @Override
    public <T> String[] getConstructorParametersNames(Constructor<T> ctor) {
        return GetParametersNameUtil.getParametersName(ctor);
    }

    public <T> IMethod<T> getMethod(ClassLoader classLoader, Method method) {
        return new StandardMethod<T>(method);
    }

    public <T> IGetterMethod<T> getGetterMethod(ClassLoader classLoader, Method method) {
        return new StandardGetterMethod<T>(method);
    }

    public <T> ISetterMethod<T> getSetterMethod(ClassLoader classLoader, Method method) {
        return new StandardSetterMethod<T>(method);
    }

    public <T, F> IField<T, F> getField(Field field) {
        return new StandardField<T, F>(field);
    }

    public <T> IProperties<T> getFieldProperties(Class<T> declaringClass, Field[] fields) {
        return new StandardFieldProperties<T>(fields);
    }

    public <T> IProperties<T> getProperties(SpaceTypeInfo typeInfo) {
        return new StandardProperties<T>(typeInfo.getSpaceProperties());
    }

    public Object getProxy(ClassLoader loader, Class<?>[] interfaces, ProxyInvocationHandler handler, boolean allowCache /*ignore in standard cache*/) {
        if (handler instanceof InvocationHandler)
            return Proxy.newProxyInstance(loader, interfaces, (InvocationHandler) handler);

        throw new IllegalArgumentException("Fail to create standard dynamic proxy, provided handler " + handler + " must implement " + InvocationHandler.class);
    }
}
