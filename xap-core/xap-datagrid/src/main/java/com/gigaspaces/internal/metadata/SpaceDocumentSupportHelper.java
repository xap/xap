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

import com.gigaspaces.metadata.SpaceDocumentSupport;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class SpaceDocumentSupportHelper {
    private static final byte CODE_DEFAULT = 0;
    private static final byte CODE_CONVERT = 2;
    private static final byte CODE_COPY = 1;

    public static byte toCode(SpaceDocumentSupport documentSupport) {
        switch (documentSupport) {
            case DEFAULT:
                return CODE_DEFAULT;
            case CONVERT:
                return CODE_CONVERT;
            case COPY:
                return CODE_COPY;
            default:
                return CODE_DEFAULT;
        }
    }

    public static SpaceDocumentSupport fromCode(byte code) {
        switch (code) {
            case CODE_DEFAULT:
                return SpaceDocumentSupport.DEFAULT;
            case CODE_CONVERT:
                return SpaceDocumentSupport.CONVERT;
            case CODE_COPY:
                return SpaceDocumentSupport.COPY;
            default:
                return SpaceDocumentSupport.DEFAULT;
        }
    }

    private static final Map<Class<?>, SpaceDocumentSupport> _defaultTypesDocumentSupport = initDefaultTypesDocumentSupport();

    private static Map<Class<?>, SpaceDocumentSupport> initDefaultTypesDocumentSupport() {
        final Map<Class<?>, SpaceDocumentSupport> result = new HashMap<Class<?>, SpaceDocumentSupport>();

        // Primitive types and arrays:
        result.put(byte.class, SpaceDocumentSupport.COPY);
        result.put(byte[].class, SpaceDocumentSupport.COPY);
        result.put(short.class, SpaceDocumentSupport.COPY);
        result.put(short[].class, SpaceDocumentSupport.COPY);
        result.put(int.class, SpaceDocumentSupport.COPY);
        result.put(int[].class, SpaceDocumentSupport.COPY);
        result.put(long.class, SpaceDocumentSupport.COPY);
        result.put(long[].class, SpaceDocumentSupport.COPY);
        result.put(float.class, SpaceDocumentSupport.COPY);
        result.put(float[].class, SpaceDocumentSupport.COPY);
        result.put(double.class, SpaceDocumentSupport.COPY);
        result.put(double[].class, SpaceDocumentSupport.COPY);
        result.put(boolean.class, SpaceDocumentSupport.COPY);
        result.put(boolean[].class, SpaceDocumentSupport.COPY);
        result.put(char.class, SpaceDocumentSupport.COPY);
        result.put(char[].class, SpaceDocumentSupport.COPY);
        // Primitive type wrappers:
        result.put(java.lang.Byte.class, SpaceDocumentSupport.COPY);
        result.put(java.lang.Byte[].class, SpaceDocumentSupport.COPY);
        result.put(java.lang.Short.class, SpaceDocumentSupport.COPY);
        result.put(java.lang.Short[].class, SpaceDocumentSupport.COPY);
        result.put(java.lang.Integer.class, SpaceDocumentSupport.COPY);
        result.put(java.lang.Integer[].class, SpaceDocumentSupport.COPY);
        result.put(java.lang.Long.class, SpaceDocumentSupport.COPY);
        result.put(java.lang.Long[].class, SpaceDocumentSupport.COPY);
        result.put(java.lang.Float.class, SpaceDocumentSupport.COPY);
        result.put(java.lang.Float[].class, SpaceDocumentSupport.COPY);
        result.put(java.lang.Double.class, SpaceDocumentSupport.COPY);
        result.put(java.lang.Double[].class, SpaceDocumentSupport.COPY);
        result.put(java.lang.Boolean.class, SpaceDocumentSupport.COPY);
        result.put(java.lang.Boolean[].class, SpaceDocumentSupport.COPY);
        result.put(java.lang.Character.class, SpaceDocumentSupport.COPY);
        result.put(java.lang.Character[].class, SpaceDocumentSupport.COPY);
        // Special case - Object:
        result.put(java.lang.Object.class, SpaceDocumentSupport.DEFAULT);
        // Common system types:
        result.put(java.lang.String.class, SpaceDocumentSupport.COPY);
        result.put(java.util.Date.class, SpaceDocumentSupport.COPY);
        result.put(java.util.UUID.class, SpaceDocumentSupport.COPY);
        result.put(java.util.Calendar.class, SpaceDocumentSupport.COPY);
        result.put(java.sql.Date.class, SpaceDocumentSupport.COPY);
        result.put(java.sql.Time.class, SpaceDocumentSupport.COPY);
        result.put(java.sql.Timestamp.class, SpaceDocumentSupport.COPY);
        result.put(java.math.BigDecimal.class, SpaceDocumentSupport.COPY);
        result.put(java.math.BigInteger.class, SpaceDocumentSupport.COPY);
        // Common maps:
        result.put(java.util.HashMap.class, SpaceDocumentSupport.COPY);
        result.put(java.util.TreeMap.class, SpaceDocumentSupport.COPY);
        result.put(java.util.WeakHashMap.class, SpaceDocumentSupport.COPY);
        result.put(java.util.IdentityHashMap.class, SpaceDocumentSupport.COPY);
        result.put(java.util.concurrent.ConcurrentHashMap.class, SpaceDocumentSupport.COPY);
        // Common collections:
        result.put(java.util.ArrayList.class, SpaceDocumentSupport.DEFAULT);
        result.put(java.util.LinkedList.class, SpaceDocumentSupport.DEFAULT);
        result.put(java.util.HashSet.class, SpaceDocumentSupport.DEFAULT);
        result.put(java.util.TreeSet.class, SpaceDocumentSupport.DEFAULT);

        // Common java types
        result.put(Class.class, SpaceDocumentSupport.COPY);
        result.put(URI.class, SpaceDocumentSupport.COPY);
        result.put(Locale.class, SpaceDocumentSupport.COPY);

        // GigaSpaces types:
        result.put(com.gigaspaces.document.SpaceDocument.class, SpaceDocumentSupport.COPY);
        result.put(com.gigaspaces.document.DocumentProperties.class, SpaceDocumentSupport.COPY);

        //java 8 java.time classes
        result.put(java.time.LocalDateTime.class, SpaceDocumentSupport.COPY);
        result.put(java.time.LocalDate.class, SpaceDocumentSupport.COPY);
        result.put(java.time.LocalTime.class, SpaceDocumentSupport.COPY);
        result.put(java.time.Duration.class, SpaceDocumentSupport.COPY);
        result.put(java.time.DayOfWeek.class, SpaceDocumentSupport.COPY);
        result.put(java.time.MonthDay.class, SpaceDocumentSupport.COPY);
        result.put(java.time.Period.class, SpaceDocumentSupport.COPY);
        result.put(java.time.Year.class, SpaceDocumentSupport.COPY);
        result.put(java.time.YearMonth.class, SpaceDocumentSupport.COPY);
        result.put(java.time.ZonedDateTime.class, SpaceDocumentSupport.COPY);
        result.put(java.time.ZoneId.class, SpaceDocumentSupport.COPY);
        result.put(java.time.ZoneOffset.class, SpaceDocumentSupport.COPY);
        result.put(java.time.Instant.class, SpaceDocumentSupport.COPY);
        result.put(java.time.OffsetDateTime.class, SpaceDocumentSupport.COPY);
        result.put(java.time.OffsetTime.class, SpaceDocumentSupport.COPY);

        return result;
    }

    public static SpaceDocumentSupport getDefaultDocumentSupport(Class<?> type) {
        SpaceDocumentSupport result = _defaultTypesDocumentSupport.get(type);
        if (result != null)
            return result;
        if (Map.class.isAssignableFrom(type))
            return SpaceDocumentSupport.COPY;
        if (Collection.class.isAssignableFrom(type))
            return SpaceDocumentSupport.DEFAULT;
        if (type.isArray())
            return getDefaultDocumentSupport(type.getComponentType());
        if (type.isEnum())
            return SpaceDocumentSupport.COPY;
        return SpaceDocumentSupport.CONVERT;
    }
}
