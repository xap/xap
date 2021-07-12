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
package com.gigaspaces.jdbc.calcite.pg;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class PgTypeUtils {

    private static final HashMap<Integer, PgTypeDescriptor> TYPE_ID_TO_TYPE;
    private static final HashMap<String, PgTypeDescriptor> TYPE_NAME_TO_TYPE;

    static {
        TYPE_ID_TO_TYPE = new HashMap<>();
        TYPE_NAME_TO_TYPE = new HashMap<>();
        Set<PgTypeDescriptor> typeSet = new HashSet<>();
        for (PgTypeDescriptor type : PgTypeDescriptor.ALL_DESCRIPTORS) {
            if (typeSet.add(type)) {
                TYPE_ID_TO_TYPE.put(type.id, type);
                TYPE_NAME_TO_TYPE.put(type.name, type);

                if (type.arrayType != 0) {
                    PgTypeDescriptor arrayType = type.asArray();
                    if (typeSet.add(arrayType)) {
                        TYPE_ID_TO_TYPE.put(arrayType.id, arrayType);
                        TYPE_NAME_TO_TYPE.put(arrayType.name, arrayType);
                    }
                }
            }
        }
    }

    public static Set<String> getTypeNames() {
        return TYPE_NAME_TO_TYPE.keySet();
    }

    public static PgTypeDescriptor getTypeByName(String typeName) {
        return TYPE_NAME_TO_TYPE.getOrDefault(typeName, PgTypeDescriptor.UNKNOWN);
    }

    public static PgTypeDescriptor getTypeById(int id) {
        return TYPE_ID_TO_TYPE.getOrDefault(id, PgTypeDescriptor.UNKNOWN);
    }

    public static Collection<PgTypeDescriptor> getTypes() {
        return TYPE_ID_TO_TYPE.values();
    }

    public static PgTypeDescriptor fromSqlTypeName(SqlTypeName typeName) {
        switch (typeName) {
            case BOOLEAN:
                return PgTypeDescriptor.BOOL;
            case TINYINT:
            case SMALLINT:
                return PgTypeDescriptor.INT2;
            case INTEGER:
                return PgTypeDescriptor.INT4;
            case BIGINT:
                return PgTypeDescriptor.INT8;
            case DECIMAL:
                return PgTypeDescriptor.NUMERIC;
            case FLOAT:
            case REAL:
                return PgTypeDescriptor.FLOAT4;
            case DOUBLE:
                return PgTypeDescriptor.FLOAT8;
            case CHAR:
                return PgTypeDescriptor.CHAR;
            case VARCHAR:
                return PgTypeDescriptor.VARCHAR;
            case BINARY:
            case VARBINARY:
                return PgTypeDescriptor.BYTEA;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return PgTypeDescriptor.INTERVAL;
            case DATE:
                return PgTypeDescriptor.DATE;
            case TIME:
                return PgTypeDescriptor.TIME;
            case TIME_WITH_LOCAL_TIME_ZONE:
                return PgTypeDescriptor.TIME_WITH_TIME_ZONE;
            case TIMESTAMP:
                return PgTypeDescriptor.TIMESTAMP;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return PgTypeDescriptor.TIMESTAMP_WITH_TIME_ZONE;
            default:
                return PgTypeDescriptor.UNKNOWN;
        }
    }

    public static SqlTypeName toSqlTypeName(PgTypeDescriptor type) {
        if (PgTypeDescriptor.BOOL.equals(type)) {
            return SqlTypeName.BOOLEAN;
        } else if (PgTypeDescriptor.REGPROC.equals(type)) {
            return SqlTypeName.INTEGER;
        } else if (PgTypeDescriptor.OID.equals(type)) {
            return SqlTypeName.INTEGER;
        } else if (PgTypeDescriptor.TEXT.equals(type)) {
            return SqlTypeName.VARCHAR;
        } else if (PgTypeDescriptor.NAME.equals(type)) {
            return SqlTypeName.VARCHAR;
        } else if (PgTypeDescriptor.INT2.equals(type)) {
            return SqlTypeName.SMALLINT;
        } else if (PgTypeDescriptor.INT4.equals(type)) {
            return SqlTypeName.INTEGER;
        } else if (PgTypeDescriptor.INT8.equals(type)) {
            return SqlTypeName.BIGINT;
        } else if (PgTypeDescriptor.NUMERIC.equals(type)) {
            return SqlTypeName.DECIMAL;
        } else if (PgTypeDescriptor.FLOAT4.equals(type)) {
            return SqlTypeName.FLOAT;
        } else if (PgTypeDescriptor.FLOAT8.equals(type)) {
            return SqlTypeName.DOUBLE;
        } else if (PgTypeDescriptor.CHAR.equals(type)) {
            return SqlTypeName.CHAR;
        } else if (PgTypeDescriptor.VARCHAR.equals(type)) {
            return SqlTypeName.VARCHAR;
        } else if (PgTypeDescriptor.BYTEA.equals(type)) {
            return SqlTypeName.BINARY;
        } else if (PgTypeDescriptor.DATE.equals(type)) {
            return SqlTypeName.DATE;
        } else if (PgTypeDescriptor.TIME.equals(type)) {
            return SqlTypeName.TIME;
        } else if (PgTypeDescriptor.TIMESTAMP.equals(type)) {
            return SqlTypeName.TIMESTAMP;
        } else if (PgTypeDescriptor.TIME_WITH_TIME_ZONE.equals(type)) {
            return SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE;
        } else if (PgTypeDescriptor.TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
        } else {
            return SqlTypeName.NULL;
        }
    }

    public static RelProtoDataType toRelProtoDataType(PgTypeDescriptor type) {
        return ((factory) -> toRelDataType(type, factory));
    }

    public static RelDataType toRelDataType(PgTypeDescriptor type, RelDataTypeFactory factory) {
        if (type.elementType != 0) {
            return factory.createArrayType(toRelDataType(getTypeById(type.elementType), factory), -1);
        }

        SqlTypeName typeName = PgTypeUtils.toSqlTypeName(type);
        if (typeName == SqlTypeName.OTHER) {
            return factory.createUnknownType();
        }

        return factory.createSqlType(typeName);
    }
}
