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

package com.gigaspaces.internal.utils.parsers;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.SQLException;

/**
 * Created by ester on 03/02/2016.
 */
@com.gigaspaces.api.InternalApi
public class ConventionObjectParser {

    public static AbstractParser getConventionParserIfAvailable(Class<?> clazz) {
        Class<?>[] supportedArgTypes = new Class<?>[]{String.class, CharSequence.class, Object.class};
        for (Class<?> type : supportedArgTypes) {
            AbstractParser parser = createMethodParserIfExists(clazz, "parse", type);
            if (parser != null)
                return parser;
        }
        for (Class<?> type : supportedArgTypes) {
            AbstractParser parser = createMethodParserIfExists(clazz, "valueOf", type);
            if (parser != null)
                return parser;
        }
        for (Class<?> type : supportedArgTypes) {
            AbstractParser parser = createConstructorParserIfExists(clazz, type);
            if (parser != null)
                return parser;
        }

        AbstractParser parser = createMethodParserIfExists(clazz, "fromString", String.class);
        if (parser != null)
            return parser;

        return null;
    }

    private static AbstractParser createMethodParserIfExists(Class<?> clazz, String methodName, Class<?> paramType) {
        try {
            final Method method = clazz.getMethod(methodName, new Class<?>[]{paramType});
            if (!Modifier.isPublic(method.getModifiers()) || !Modifier.isStatic(method.getModifiers()))
                return null;
            if (!clazz.isAssignableFrom(method.getReturnType()))
                return null;
            return new MethodParser(method);

        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private static AbstractParser createConstructorParserIfExists(Class<?> clazz, Class<?> paramType) {
        try {
            final Constructor<?> constructor = clazz.getConstructor(paramType);
            if (!Modifier.isPublic(constructor.getModifiers()))
                return null;
            return new ConstructorParser(constructor);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private static class MethodParser extends AbstractParser {
        private final Method method;

        private MethodParser(Method method) {
            this.method = method;
        }

        @Override
        public Object parse(String s) throws SQLException {
            try {
                return method.invoke(null, s);
            } catch (Exception e) {
                throw new SQLException("Failed to parse " + s + " to " + method.getDeclaringClass().getName(), e);
            }
        }
    }

    private static class ConstructorParser extends AbstractParser {
        private final Constructor constructor;

        private ConstructorParser(Constructor constructor) {
            this.constructor = constructor;
        }

        @Override
        public Object parse(String s) throws SQLException {
            try {
                return constructor.newInstance(s);
            } catch (Exception e) {
                throw new SQLException("Failed to parse " + s + " to " + constructor.getDeclaringClass().getName(), e);
            }
        }
    }
}
