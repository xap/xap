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

import java.lang.reflect.InvocationTargetException;


/**
 * Represents a all properties accessor
 *
 * @author GuyK
 * @since 7.0
 */
public interface IProperties<T> {
    Object[] getValues(T obj);

    void setValues(T obj, Object[] values);

    class Helper {
        public static final String INTERNAL_NAME = ReflectionUtil.getInternalName(IProperties.class);

        private static final String[] EXCEPTIONS = new String[]{
                ReflectionUtil.getInternalName(IllegalAccessException.class),
                ReflectionUtil.getInternalName(InvocationTargetException.class)};

        public static String getterName() {
            return "getValues";
        }

        public static String getterDesc() {
            return "(Ljava/lang/Object;)[Ljava/lang/Object;";
        }

        public static String setterName() {
            return "setValues";
        }

        public static String setterDesc() {
            return "(Ljava/lang/Object;[Ljava/lang/Object;)V";
        }

        public static String[] exceptions() {
            return EXCEPTIONS;
        }
    }
}
