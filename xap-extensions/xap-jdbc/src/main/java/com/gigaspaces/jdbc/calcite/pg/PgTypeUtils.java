package com.gigaspaces.jdbc.calcite.pg;

import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class PgTypeUtils {

    private static final HashMap<Integer, PgTypeDescriptor> typeIdToType;

    static {
        Field[] fields = PgTypeUtils.class.getDeclaredFields();
        typeIdToType = new HashMap<>(fields.length * 2);
        Set<PgTypeDescriptor> typeSet = new HashSet<>();
        try {
            for (Field field : fields) {
                if (PgTypeDescriptor.class.isAssignableFrom(field.getType()) && Modifier.isStatic(field.getModifiers())) {
                    field.setAccessible(true);
                    PgTypeDescriptor type = (PgTypeDescriptor) field.get(null);
                    if (typeSet.add(type)) {
                        typeIdToType.put(type.id, type);

                        if (type.arrayType != 0) {
                            PgTypeDescriptor arrayType = type.asArray();
                            if (typeSet.add(arrayType)) {
                                typeIdToType.put(arrayType.id, arrayType);
                            }
                        }
                    }
                }
            }
        } catch (Throwable e) {
            throw new AssertionError(e);
        }
    }

    public static PgTypeDescriptor getTypeById(int id) {
        return typeIdToType.getOrDefault(id, PgTypeDescriptor.UNKNOWN);
    }

    public static Collection<PgTypeDescriptor> getTypes() {
        return typeIdToType.values();
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
}
