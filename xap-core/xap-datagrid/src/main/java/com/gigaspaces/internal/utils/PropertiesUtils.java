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

package com.gigaspaces.internal.utils;

import java.util.Map;
import java.util.Properties;

/**
 * @author Niv Ingberg
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class PropertiesUtils {
    protected PropertiesUtils() {
    }

    public static String get(Properties properties, String propertyName) {
        return properties == null ? null : properties.getProperty(propertyName);
    }

    public static boolean getBoolean(Properties properties, String propertyName, boolean defaultValue) {
        String value = get(properties, propertyName);
        return value == null || value.length() == 0 ? defaultValue : Boolean.parseBoolean(value);
    }

    public static short getShort(Properties properties, String propertyName, short defaultValue) {
        String value = get(properties, propertyName);
        return value == null || value.length() == 0 ? defaultValue : Short.parseShort(value);
    }

    public static int getInteger(Properties properties, String propertyName, int defaultValue) {
        String value = get(properties, propertyName);
        return value == null || value.length() == 0 ? defaultValue : Integer.parseInt(value);
    }

    public static long getLong(Properties properties, String propertyName, long defaultValue) {
        String value = get(properties, propertyName);
        return value == null || value.length() == 0 ? defaultValue : Long.parseLong(value);
    }

    public static float getFloat(Properties properties, String propertyName, float defaultValue) {
        String value = get(properties, propertyName);
        return value == null || value.length() == 0 ? defaultValue : Float.parseFloat(value);
    }

    public static double getDouble(Properties properties, String propertyName, double defaultValue) {
        String value = get(properties, propertyName);
        return value == null || value.length() == 0 ? defaultValue : Double.parseDouble(value);
    }

    public static <T extends Enum<T>> T getEnum(Properties properties, String propertyName, Class<T> enumType, T defaultValue) {
        String value = get(properties, propertyName);

        if (value == null || value.length() == 0)
            return defaultValue;

        return Enum.valueOf(enumType, value);
    }

    public static Properties fromMap(Map<String, String> map) {
        if (map == null)
            return null;
        Properties properties = new Properties();
        map.forEach((k, v) -> properties.setProperty(k, v));
        return properties;
    }
}
