package com.gigaspaces.jdbc.calcite.pg;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class PgTypeUtils {

    private static final HashMap<Integer, PgType> elementToArray;
    private static final HashMap<Integer, PgType> typeIdToType;
    private static final HashMap<String, PgType> typeNameToType;

    static {
        Field[] fields = PgTypeUtils.class.getDeclaredFields();
        elementToArray = new HashMap<>(fields.length * 2);
        typeIdToType = new HashMap<>(fields.length * 2);
        typeNameToType = new HashMap<>();
        Set<PgType> typeSet = new HashSet<>();
        try {
            for (Field field : fields) {
                if (PgType.class.isAssignableFrom(field.getType()) && Modifier.isStatic(field.getModifiers())) {
                    field.setAccessible(true);
                    PgType type = (PgType) field.get(null);
                    if (typeSet.add(type)) {
                        typeIdToType.put(type.id, type);
                        typeNameToType.put(type.name, type);

                        if (type.arrayType != 0) {
                            PgType arrayType = type.asArray();
                            if (typeSet.add(arrayType)) {
                                typeIdToType.put(arrayType.id, arrayType);
                                typeNameToType.put(arrayType.name, arrayType);

                                elementToArray.put(type.id, arrayType);
                            }
                        }
                    }
                }
            }
        } catch (Throwable e) {
            throw new AssertionError(e);
        }
    }

    public static PgType getType(int id) {
        return typeIdToType.getOrDefault(id, PgType.UNKNOWN);
    }

    public static PgType getArrayType(int elementTypeId) {
        return elementToArray.getOrDefault(elementTypeId, PgType.UNKNOWN);
    }
    
    public static PgType fromInternal(SqlTypeName typeName) {
        switch (typeName) {
            case BOOLEAN:
                return PgType.BOOL;
            case TINYINT:
            case SMALLINT:
                return PgType.INT2;
            case INTEGER:
                return PgType.INT4;
            case BIGINT:
                return PgType.INT8;
            case DECIMAL:
                return PgType.NUMERIC;
            case FLOAT:
            case REAL:
                return PgType.FLOAT4;
            case DOUBLE:
                return PgType.FLOAT8;
            case CHAR:
                return PgType.CHAR;
            case VARCHAR:
                return PgType.VARCHAR;
            case BINARY:
            case VARBINARY:
                return PgType.BYTEA;
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
                return PgType.INTERVAL;
            case DATE:
                return PgType.DATE;
            case TIME:
                return PgType.TIME;
            case TIME_WITH_LOCAL_TIME_ZONE:
                return PgType.TIME_WITH_TIME_ZONE;
            case TIMESTAMP:
                return PgType.TIMESTAMP;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return PgType.TIMESTAMP_WITH_TIME_ZONE;
            default:
                return PgType.UNKNOWN;
        }
    }

    public static PgType typeByName(String typeName) {
        return typeNameToType.getOrDefault(typeName, PgType.UNKNOWN);
    }

    public static RelProtoDataType resolveType(String typeName) {
        PgType pgType = typeByName(typeName);
        return pgType == PgType.UNKNOWN ? null : protoType(pgType);
    }

    public static RelProtoDataType protoType(PgType pgType) {
        return ((tf) -> toInternal(pgType, tf));
    }

    public static Set<String> typeNames() {
        return typeNameToType.keySet();
    }

    public static Collection<PgType> types() {
        return typeIdToType.values();
    }

    public static RelDataType toInternal(int type, RelDataTypeFactory factory) {
        return toInternal(getType(type), factory);
    }

    public static SqlTypeName sqlTypeName(PgType type) {
        if (PgType.BOOL.equals(type)) {
            return SqlTypeName.BOOLEAN;
        } else if (PgType.REGPROC.equals(type)) {
            return SqlTypeName.INTEGER;
        } else if (PgType.OID.equals(type)) {
            return SqlTypeName.INTEGER;
        } else if (PgType.TEXT.equals(type)) {
            return SqlTypeName.VARCHAR;
        } else if (PgType.NAME.equals(type)) {
            return SqlTypeName.VARCHAR;
        } else if (PgType.INT2.equals(type)) {
            return SqlTypeName.SMALLINT;
        } else if (PgType.INT4.equals(type)) {
            return SqlTypeName.INTEGER;
        } else if (PgType.INT8.equals(type)) {
            return SqlTypeName.BIGINT;
        } else if (PgType.NUMERIC.equals(type)) {
            return SqlTypeName.DECIMAL;
        } else if (PgType.FLOAT4.equals(type)) {
            return SqlTypeName.FLOAT;
        } else if (PgType.FLOAT8.equals(type)) {
            return SqlTypeName.DOUBLE;
        } else if (PgType.CHAR.equals(type)) {
            return SqlTypeName.CHAR;
        } else if (PgType.VARCHAR.equals(type)) {
            return SqlTypeName.VARCHAR;
        } else if (PgType.BYTEA.equals(type)) {
            return SqlTypeName.BINARY;
        } else if (PgType.DATE.equals(type)) {
            return SqlTypeName.DATE;
        } else if (PgType.TIME.equals(type)) {
            return SqlTypeName.TIME;
        } else if (PgType.TIMESTAMP.equals(type)) {
            return SqlTypeName.TIMESTAMP;
        } else if (PgType.TIME_WITH_TIME_ZONE.equals(type)) {
            return SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE;
        } else if (PgType.TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
        } else {
            return SqlTypeName.NULL;
        }
    }

    private static RelDataType toInternal(PgType type, RelDataTypeFactory factory) {
        if (type.elementType != 0) {
            return factory.createArrayType(toInternal(type.elementType, factory), -1);
        }

        SqlTypeName typeName = sqlTypeName(type);
        if (typeName == SqlTypeName.OTHER) {
            return factory.createUnknownType();
        }

        return factory.createSqlType(typeName);
    }
}
