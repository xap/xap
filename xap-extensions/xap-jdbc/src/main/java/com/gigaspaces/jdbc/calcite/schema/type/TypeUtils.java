package com.gigaspaces.jdbc.calcite.schema.type;

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

public class TypeUtils {
    public static final PgType PG_TYPE_UNKNOWN = TypeUnknown.INSTANCE;
    public static final PgType PG_TYPE_BOOL = TypeBool.INSTANCE;
    public static final PgType PG_TYPE_BYTEA = TypeBytea.INSTANCE;
    public static final PgType PG_TYPE_CHAR = TypeChar.INSTANCE;
    public static final PgType PG_TYPE_NAME = TypeName.INSTANCE;
    public static final PgType PG_TYPE_INT8 = TypeInt8.INSTANCE;
    public static final PgType PG_TYPE_INT2 = TypeInt2.INSTANCE;
    public static final PgType PG_TYPE_INT4 = TypeInt4.INSTANCE;
    public static final PgType PG_TYPE_REGPROC = TypeRegproc.INSTANCE;
    public static final PgType PG_TYPE_TEXT = TypeText.INSTANCE;
    public static final PgType PG_TYPE_OID = TypeOid.INSTANCE;
    public static final PgType PG_TYPE_FLOAT4 = TypeFloat4.INSTANCE;
    public static final PgType PG_TYPE_FLOAT8 = TypeFloat8.INSTANCE;
    public static final PgType PG_TYPE_VARCHAR = TypeVarchar.INSTANCE;
    public static final PgType PG_TYPE_DATE = TypeDate.INSTANCE;
    public static final PgType PG_TYPE_TIME = TypeTime.INSTANCE;
    public static final PgType PG_TYPE_TIMESTAMP = TypeTimestamp.INSTANCE;
    public static final PgType PG_TYPE_TIMESTAMPTZ = TypeTamestampTZ.INSTANCE;
    public static final PgType PG_TYPE_INTERVAL = TypeInterval.INSTANCE;
    public static final PgType PG_TYPE_TIMETZ = TypeTimeTZ.INSTANCE;
    public static final PgType PG_TYPE_NUMERIC = TypeNumeric.INSTANCE;

    private static final HashMap<Integer, PgType> elementToArray;
    private static final HashMap<Integer, PgType> typeIdToType;
    private static final HashMap<String, PgType> typeNameToType;

    static {
        Field[] fields = TypeUtils.class.getDeclaredFields();
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
        return typeIdToType.getOrDefault(id, PG_TYPE_UNKNOWN);
    }

    public static PgType getArrayType(int elementTypeId) {
        return elementToArray.getOrDefault(elementTypeId, PG_TYPE_UNKNOWN);
    }

    public static PgType fromInternal(RelDataType internalType) {
        SqlTypeName typeName = internalType.getSqlTypeName();
        if (typeName == SqlTypeName.ARRAY)
            return getArrayType(fromInternal(internalType.getComponentType()).id);

        return fromInternal(typeName);
    }

    public static PgType fromInternal(SqlTypeName typeName) {
        switch (typeName) {
            case BOOLEAN:
                return PG_TYPE_BOOL;
            case TINYINT:
            case SMALLINT:
                return PG_TYPE_INT2;
            case INTEGER:
                return PG_TYPE_INT4;
            case BIGINT:
                return PG_TYPE_INT8;
            case DECIMAL:
                return PG_TYPE_NUMERIC;
            case FLOAT:
            case REAL:
                return PG_TYPE_FLOAT4;
            case DOUBLE:
                return PG_TYPE_FLOAT8;
            case CHAR:
                return PG_TYPE_CHAR;
            case VARCHAR:
                return PG_TYPE_VARCHAR;
            case BINARY:
            case VARBINARY:
                return PG_TYPE_BYTEA;
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
                return PG_TYPE_INTERVAL;
            case DATE:
                return PG_TYPE_DATE;
            case TIME:
                return PG_TYPE_TIME;
            case TIME_WITH_LOCAL_TIME_ZONE:
                return PG_TYPE_TIMETZ;
            case TIMESTAMP:
                return PG_TYPE_TIMESTAMP;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return PG_TYPE_TIMESTAMPTZ;
            default:
                return PG_TYPE_UNKNOWN;
        }
    }

    public static PgType typeByName(String typeName) {
        return typeNameToType.getOrDefault(typeName, PG_TYPE_UNKNOWN);
    }

    public static RelProtoDataType resolveType(String typeName) {
        PgType pgType = typeByName(typeName);
        return pgType == PG_TYPE_UNKNOWN ? null : protoType(pgType);
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
        if (PG_TYPE_BOOL.equals(type)) {
            return SqlTypeName.BOOLEAN;
        } else if (PG_TYPE_REGPROC.equals(type)) {
            return SqlTypeName.INTEGER;
        } else if (PG_TYPE_OID.equals(type)) {
            return SqlTypeName.INTEGER;
        } else if (PG_TYPE_TEXT.equals(type)) {
            return SqlTypeName.VARCHAR;
        } else if (PG_TYPE_NAME.equals(type)) {
            return SqlTypeName.VARCHAR;
        } else if (PG_TYPE_INT2.equals(type)) {
            return SqlTypeName.SMALLINT;
        } else if (PG_TYPE_INT4.equals(type)) {
            return SqlTypeName.INTEGER;
        } else if (PG_TYPE_INT8.equals(type)) {
            return SqlTypeName.BIGINT;
        } else if (PG_TYPE_NUMERIC.equals(type)) {
            return SqlTypeName.DECIMAL;
        } else if (PG_TYPE_FLOAT4.equals(type)) {
            return SqlTypeName.FLOAT;
        } else if (PG_TYPE_FLOAT8.equals(type)) {
            return SqlTypeName.DOUBLE;
        } else if (PG_TYPE_CHAR.equals(type)) {
            return SqlTypeName.CHAR;
        } else if (PG_TYPE_VARCHAR.equals(type)) {
            return SqlTypeName.VARCHAR;
        } else if (PG_TYPE_BYTEA.equals(type)) {
            return SqlTypeName.BINARY;
        } else if (PG_TYPE_DATE.equals(type)) {
            return SqlTypeName.DATE;
        } else if (PG_TYPE_TIME.equals(type)) {
            return SqlTypeName.TIME;
        } else if (PG_TYPE_TIMESTAMP.equals(type)) {
            return SqlTypeName.TIMESTAMP;
        } else if (PG_TYPE_TIMETZ.equals(type)) {
            return SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE;
        } else if (PG_TYPE_TIMESTAMPTZ.equals(type)) {
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
