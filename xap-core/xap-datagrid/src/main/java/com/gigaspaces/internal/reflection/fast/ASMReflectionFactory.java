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

package com.gigaspaces.internal.reflection.fast;

import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.reflection.IConstructor;
import com.gigaspaces.internal.reflection.IField;
import com.gigaspaces.internal.reflection.IGetterMethod;
import com.gigaspaces.internal.reflection.IMethod;
import com.gigaspaces.internal.reflection.IParamsConstructor;
import com.gigaspaces.internal.reflection.IProperties;
import com.gigaspaces.internal.reflection.IReflectionFactory;
import com.gigaspaces.internal.reflection.ISetterMethod;
import com.gigaspaces.internal.reflection.ProxyInvocationHandler;
import com.gigaspaces.internal.reflection.fast.proxy.ProxyFactory;
import com.gigaspaces.internal.reflection.standard.StandardReflectionFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implement {@link IReflectionFactory} based on ASM.
 *
 * @author Guy
 * @since 7.0
 *
 * Notice!!!! this class is loaded by name in ReflectionUtil, it should be changed if the class is
 * renamed.
 */
@com.gigaspaces.api.InternalApi
public class ASMReflectionFactory implements IReflectionFactory {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_REFLECTION);
    private final IReflectionFactory _fallbackFactory;

    public ASMReflectionFactory() {
        this._fallbackFactory = new StandardReflectionFactory();
    }

    public <T> IConstructor<T> getConstructor(Constructor<T> ctor) {
        if (canGenerateAsm(ctor)) {
            try {
                return ASMConstructorFactory.getConstructor(ctor);
            } catch (Exception e) {
                if (_logger.isWarnEnabled())
                    _logger.warn("Failed to generate ASM for constructor, falling back to standard reflection. " +
                            "class: " + ctor.getDeclaringClass().getName(), e);
            }
        }

        return _fallbackFactory.getConstructor(ctor);
    }

    public <T> IParamsConstructor<T> getParamsConstructor(Constructor<T> ctor) {
        if (canGenerateAsm(ctor)) {
            try {
                return ASMConstructorFactory.getParamsConstructor(ctor);
            } catch (Exception e) {
                if (_logger.isWarnEnabled())
                    _logger.warn("Failed to generate ASM for constructor, falling back to standard reflection. " +
                            "class: " + ctor.getDeclaringClass().getName(), e);
            }
        }

        return _fallbackFactory.getParamsConstructor(ctor);
    }

    @Override
    public <T> String[] getConstructorParametersNames(Constructor<T> ctor) {
        try {
            return ConstructorPropertyNameExtractor.getParameterNames(ctor);
        } catch (IOException e) {
            if (_logger.isWarnEnabled())
                _logger.warn("Failed to get constructor parameters names using ASM, falling back to standard reflection. " +
                        "class: " + ctor.getDeclaringClass().getName(), e);
        }
        return _fallbackFactory.getConstructorParametersNames(ctor);
    }

    public <T> IMethod<T> getMethod(ClassLoader classLoader, Method method) {
        if (canGenerateAsm(method)) {
            try {
                return ASMMethodFactory.getMethod(classLoader, method);
            } catch (Exception e) {
                if (_logger.isWarnEnabled())
                    _logger.warn("Failed to generate ASM for method, falling back to standard reflection. " +
                            "class: [" + method.getDeclaringClass().getName() + "], " +
                            "method: [" + method.getName() + "].", e);
            }
        }

        return _fallbackFactory.getMethod(classLoader, method);
    }

    public <T> IGetterMethod<T> getGetterMethod(ClassLoader classLoader, Method method) {
        if (canGenerateAsm(method)) {
            try {
                return ASMMethodFactory.getGetterMethod(classLoader, method);
            } catch (Exception e) {
                if (_logger.isWarnEnabled())
                    _logger.warn("Failed to generate ASM for getter method, falling back to standard reflection. " +
                            "class: [" + method.getDeclaringClass().getName() + "], " +
                            "method: [" + method.getName() + "].", e);
            }
        }
        return _fallbackFactory.getGetterMethod(classLoader, method);
    }

    public <T> ISetterMethod<T> getSetterMethod(ClassLoader classLoader, Method method) {
        if (canGenerateAsm(method)) {
            try {
                return ASMMethodFactory.getSetterMethod(classLoader, method);
            } catch (Exception e) {
                if (_logger.isWarnEnabled())
                    _logger.warn("Failed to generate ASM for setter method, falling back to standard reflection. " +
                            "class: [" + method.getDeclaringClass().getName() + "], " +
                            "method: [" + method.getName() + "].", e);
            }
        }
        return _fallbackFactory.getSetterMethod(classLoader, method);
    }

    public <T, F> IField<T, F> getField(Field field) {
        if (canGenerateAsm(field)) {
            try {
                return ASMFieldFactory.getField(field);
            } catch (Exception e) {
                if (_logger.isWarnEnabled())
                    _logger.warn("Failed to generate ASM for field, falling back to standard reflection. " +
                            "class: [" + field.getDeclaringClass().getName() + "], " +
                            "method: [" + field.getName() + "].", e);
            }
        }

        return _fallbackFactory.getField(field);
    }

    public <T> IProperties<T> getFieldProperties(Class<T> declaringClass, Field[] fields) {
        try {
            return ASMFieldPropertiesFactory.getProperties(declaringClass, fields);
        } catch (Exception e) {
            if (_logger.isWarnEnabled())
                _logger.warn("Failed to generate ASM for properties, falling back to standard reflection. " +
                        "class: [" + declaringClass.getName() + "].", e);
        }

        return _fallbackFactory.getFieldProperties(declaringClass, fields);
    }

    public <T> IProperties<T> getProperties(SpaceTypeInfo typeInfo) {
        try {
            return ASMPropertiesFactory.getProperties(typeInfo);
        } catch (Exception e) {
            if (_logger.isWarnEnabled())
                _logger.warn("Failed to generate ASM for properties, falling back to standard reflection. " +
                        "class: [" + typeInfo.getName() + "].", e);
        }

        return _fallbackFactory.getProperties(typeInfo);
    }

    public Object getProxy(ClassLoader loader, Class<?>[] interfaces, ProxyInvocationHandler handler, boolean allowCache) {
        try {
            return ProxyFactory.newProxyInstance(loader, interfaces, handler, allowCache);
        } catch (Exception e) {
            if (_logger.isWarnEnabled())
                _logger.warn("Failed to generate ASM for dynamic proxy, falling back to standard reflection. ", e);
        }

        return _fallbackFactory.getProxy(loader, interfaces, handler, allowCache);
    }

    /**
     * We can only use ASM if allowed by a system property, the method is public, and the declaring
     * class is not an anonymous one.
     */
    private static boolean canGenerateAsm(Member member) {
        if (!Modifier.isPrivate(member.getModifiers()) && member.getDeclaringClass().getClassLoader() != null)
            return true;

        if (_logger.isDebugEnabled())
            _logger.debug("Cannot generate ASM for member " + member.getName() +
                    " on class " + member.getDeclaringClass().getName() +
                    " - using standard reflection instead.");

        return false;
    }
}
