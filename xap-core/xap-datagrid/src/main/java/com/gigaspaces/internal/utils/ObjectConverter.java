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

import com.gigaspaces.internal.utils.parsers.*;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 7.1.1
 */
public abstract class ObjectConverter {
    private static final Map<String, AbstractParser> _typeParserMap = createTypeConverterMap();
    private static final Map<String, AbstractParser> _runtimeGeneratedParserMap = new ConcurrentHashMap<String, AbstractParser>();

    public static Object convert(Object obj, Class<?> type)
            throws SQLException {
        if (obj == null)
            return null;

        if (type.isAssignableFrom(obj.getClass()))
            return obj;

        if (type.equals(Object.class))
            return obj;


        AbstractParser parser = getParserFromType(type);
        if (parser == null)
            throw new SQLException("Failed converting [" + obj + "] from '" + obj.getClass().getName() + "' to '" + type.getName() + "' - converter not found.");
        try {
            obj = parser.parse(obj.toString());
            return obj;
        } catch (RuntimeException e) {
            SQLException ex = new SQLException("Failed converting [" + obj + "] from '" + obj.getClass().getName() + "' to '" + type.getName() + "'.");
            ex.initCause(e);
            throw ex;
        }
    }

    private static AbstractParser getParserFromType(Class<?> type) {
        AbstractParser parser = _typeParserMap.get(type.getName());
        if(parser == null ){
            parser = _runtimeGeneratedParserMap.get(type.getName());
        }
        // If no parser found, check if type is an enum
        if (parser == null) {
            if (type.isEnum()) {
                parser = new EnumParser(type);
                _runtimeGeneratedParserMap.put(type.getName(), parser);
                return parser;
            }

            // Enum with abstract methods is an inner class of the original Enum
            // and its isEnum() method returns false. Therefore we check its enclosing class.
            if (type.getEnclosingClass() != null && type.getEnclosingClass().isEnum()) {
                parser = _runtimeGeneratedParserMap.get(type.getEnclosingClass().getName());
                if (parser == null) {
                    parser = new EnumParser(type.getEnclosingClass());
                    _runtimeGeneratedParserMap.put(type.getEnclosingClass().getName(), parser);
                }
                return parser;
            }

            //fix for GS-13625
            if( java.util.Date.class.isAssignableFrom( type ) ||
                    java.time.temporal.Temporal.class.isAssignableFrom( type ) ){
                initDateRelatedTypes();
                parser = _runtimeGeneratedParserMap.get(type.getName());
                if( parser != null ){
                    return parser;
                }
            }


            parser = ConventionObjectParser.getConventionParserIfAvailable(type);
            if (parser != null) {
                _runtimeGeneratedParserMap.put(type.getName(), parser);
                return parser;
            }
        }
        return parser;
    }

    private static Map<String, AbstractParser> createTypeConverterMap() {
        final Map<String, AbstractParser> map = new ConcurrentHashMap<String, AbstractParser>();

        // Primitive types:
        map.put(Byte.class.getName(), new ByteParser());
        map.put(Short.class.getName(), new ShortParser());
        map.put(Integer.class.getName(), new IntegerParser());
        map.put(Long.class.getName(), new LongParser());
        map.put(Float.class.getName(), new FloatParser());
        map.put(Double.class.getName(), new DoubleParser());
        map.put(Boolean.class.getName(), new BooleanParser());
        map.put(Character.class.getName(), new CharacterParser());

        map.put(byte.class.getName(), map.get(Byte.class.getName()));
        map.put(short.class.getName(), map.get(Short.class.getName()));
        map.put(int.class.getName(), map.get(Integer.class.getName()));
        map.put(long.class.getName(), map.get(Long.class.getName()));
        map.put(float.class.getName(), map.get(Float.class.getName()));
        map.put(double.class.getName(), map.get(Double.class.getName()));
        map.put(boolean.class.getName(), map.get(Boolean.class.getName()));
        map.put(char.class.getName(), map.get(Character.class.getName()));

        map.put(String.class.getName(), new StringParser());

        // Additional common types from JRE:
        map.put(java.math.BigDecimal.class.getName(), new BigDecimalParser());
        // JDBC types:
        map.put(com.j_spaces.jdbc.driver.Blob.class.getName(), new BlobParser());
        map.put(com.j_spaces.jdbc.driver.Clob.class.getName(), new ClobParser());

        return map;
    }

    private static void initDateRelatedTypes(){

        _runtimeGeneratedParserMap.put(java.time.LocalDate.class.getName(), new LocalDateParser());
        _runtimeGeneratedParserMap.put(java.time.LocalTime.class.getName(), new LocalTimeParser());
        _runtimeGeneratedParserMap.put(java.time.LocalDateTime.class.getName(), new LocalDateTimeParser());
        _runtimeGeneratedParserMap.put(java.util.Date.class.getName(), new DateParser());
        _runtimeGeneratedParserMap.put(java.sql.Date.class.getName(), new SqlDateParser());
        _runtimeGeneratedParserMap.put(java.sql.Time.class.getName(), new SqlTimeParser());
        _runtimeGeneratedParserMap.put(java.sql.Timestamp.class.getName(), new SqlTimestampParser());
        _runtimeGeneratedParserMap.put(java.time.Instant.class.getName(), new InstantParser());
    }


    public static void clearRuntimeGeneratedCache(){
        if(!_runtimeGeneratedParserMap.isEmpty()) {
            LoggerFactory.getLogger("ObjectConverter").info("clearing runtime generated parser cache");
            _runtimeGeneratedParserMap.clear();
        }
    }
}
