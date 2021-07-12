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
package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.entry.CompoundSpaceId;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;

public class GSTable extends AbstractTable {

    private final ITypeDesc typeDesc;
    private final String name;

    public GSTable(String name, ITypeDesc typeDesc) {
        this.name = name;
        this.typeDesc = typeDesc;
    }

    /**
     * java.sql.Date -> DATE
     * java.time.LocalDate -> DATE
     *
     * java.sql.Time -> TIME
     * java.time.LocalTime -> TIME
     *
     * java.sql.Timestamp -> TIMESTAMP
     * java.time.LocalDateTime -> TIMESTAMP
     *
     * java.util.Date -> TIMESTAMP WITH TIME ZONE
     * java.util.Calendar -> TIMESTAMP WITH TIME ZONE
     * java.time.OffsetDateTime -> TIMESTAMP WITH TIME ZONE
     * java.time.ZonedDateTime -> TIMESTAMP WITH TIME ZONE
     * java.time.Instant -> TIMESTAMP WITH TIME ZONE
     */
    public static SqlTypeName mapToSqlType(Class<?> clazz) {
        if (clazz == Short.class || clazz == short.class) {
            return SqlTypeName.SMALLINT;
        } else if (clazz == Character.class || clazz == char.class) {
            return SqlTypeName.CHAR;
        } else if (clazz == Integer.class || clazz == int.class) {
            return SqlTypeName.INTEGER;
        } else if (clazz == Long.class || clazz == long.class) {
            return SqlTypeName.BIGINT;
        } else if (clazz == Float.class || clazz == float.class) {
            return SqlTypeName.FLOAT;
        } else if (clazz == Double.class || clazz == double.class) {
            return SqlTypeName.DOUBLE;
        } else if (clazz == Boolean.class || clazz == boolean.class) {
            return SqlTypeName.BOOLEAN;
        } else if (clazz == BigDecimal.class) {
            return SqlTypeName.DECIMAL;
        } else if (clazz == String.class) {
            return SqlTypeName.VARCHAR;
        } else if (clazz == java.sql.Date.class
                || clazz == java.time.LocalDate.class) {
            return SqlTypeName.DATE;
        } else if (clazz == java.sql.Time.class
                || clazz == java.time.LocalTime.class) {
            return SqlTypeName.TIME;
        } else if (clazz == java.sql.Timestamp.class
                || clazz == java.time.LocalDateTime.class) {
            return SqlTypeName.TIMESTAMP;
        } else if (clazz == java.util.Date.class
                || clazz == java.util.Calendar.class
                || clazz == java.time.OffsetDateTime.class
                || clazz == java.time.ZonedDateTime.class
                || clazz == java.time.Instant.class) {
            return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
        } else if (clazz.isArray() ){
            return SqlTypeName.ARRAY;
        } else if (CompoundSpaceId.class.isAssignableFrom(clazz)){
            return SqlTypeName.ANY;
        }


        throw new UnsupportedOperationException("Unsupported type: " + clazz);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

        for (PropertyInfo property : typeDesc.getProperties()) {
            builder.add(
                    property.getName(),
                    mapToSqlType(property.getType())
            ).nullable(!property.isPrimitive());
        }
        return builder.build();
    }

    public String getName() {
        return name;
    }

    public String getShortName() {
        return this.typeDesc.getTypeSimpleName();
    }
}
