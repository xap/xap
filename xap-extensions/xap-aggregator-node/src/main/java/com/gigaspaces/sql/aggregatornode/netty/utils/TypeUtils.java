package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.sql.aggregatornode.netty.exception.BreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.query.ColumnDescription;
import com.gigaspaces.sql.aggregatornode.netty.query.ParameterDescription;
import com.gigaspaces.sql.aggregatornode.netty.query.Session;
import io.netty.buffer.ByteBuf;
import io.netty.util.collection.IntObjectHashMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

public class TypeUtils {
    public static final PgType PG_TYPE_UNKNOWN = TypeUnknown.INSTANCE;
    public static final PgType PG_TYPE_ANY = TypeAny.INSTANCE;
    public static final PgType PG_TYPE_BOOL = TypeBool.INSTANCE;
    public static final PgType PG_TYPE_BYTEA = TypeBytea.INSTANCE;
    public static final PgType PG_TYPE_CHAR = TypeChar.INSTANCE;
    public static final PgType PG_TYPE_NAME = TypeName.INSTANCE;
    public static final PgType PG_TYPE_INT8 = TypeInt8.INSTANCE;
    public static final PgType PG_TYPE_INT2 = TypeInt2.INSTANCE;
    public static final PgType PG_TYPE_INT2VECTOR = TypeInt2vector.INSTANCE;
    public static final PgType PG_TYPE_INT4 = TypeInt4.INSTANCE;
    public static final PgType PG_TYPE_REGPROC = TypeRegproc.INSTANCE;
    public static final PgType PG_TYPE_TEXT = TypeText.INSTANCE;
    public static final PgType PG_TYPE_OID = TypeOid.INSTANCE;
    public static final PgType PG_TYPE_OIDVECTOR = TypeOidVector.INSTANCE;
    public static final PgType PG_TYPE_FLOAT4 = TypeFloat4.INSTANCE;
    public static final PgType PG_TYPE_FLOAT8 = TypeFloat8.INSTANCE;
    public static final PgType PG_TYPE_BPCHAR = TypeBpchar.INSTANCE;
    public static final PgType PG_TYPE_VARCHAR = TypeVarchar.INSTANCE;
    public static final PgType PG_TYPE_DATE = TypeDate.INSTANCE;
    public static final PgType PG_TYPE_TIME = TypeTime.INSTANCE;
    public static final PgType PG_TYPE_TIMESTAMP = TypeTimestamp.INSTANCE;
    public static final PgType PG_TYPE_TIMESTAMPTZ = TypeTamestampTZ.INSTANCE;
    public static final PgType PG_TYPE_INTERVAL = TypeInterval.INSTANCE;
    public static final PgType PG_TYPE_TIMETZ = TypeTimeTZ.INSTANCE;
    public static final PgType PG_TYPE_NUMERIC = TypeNumeric.INSTANCE;
    public static final PgType PG_TYPE_CURSOR = TypeCursor.INSTANCE;

    private static final IntObjectHashMap<PgType> elementToArray;
    private static final IntObjectHashMap<PgType> typeIdToType;

    static {
        Field[] fields = TypeUtils.class.getDeclaredFields();
        elementToArray = new IntObjectHashMap<>(fields.length * 2);
        typeIdToType = new IntObjectHashMap<>(fields.length * 2);
        Set<PgType> typeSet = new HashSet<>();
        try {
            for (Field field : fields) {
                if (PgType.class.isAssignableFrom(field.getType()) && Modifier.isStatic(field.getModifiers())) {
                    field.setAccessible(true);
                    PgType type = (PgType) field.get(null);
                    if (typeSet.add(type)) {
                        TypeUtils.typeIdToType.put(type.id, type);
                        if (type.arrayType != 0) {
                            PgType arrayType = TypeUtils.arrayType(type);
                            if (typeSet.add(arrayType))
                                TypeUtils.elementToArray.put(type.id, arrayType);
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
            case ARRAY:
                return getArrayType(fromInternal(internalType.getComponentType()).arrayType);
            case CURSOR:
                return PG_TYPE_CURSOR;
            case ANY:
                return PG_TYPE_ANY;

            case NULL:
            case SYMBOL:
            case MULTISET:
            case MAP:
            case DISTINCT:
            case STRUCTURED:
            case ROW:
            case OTHER:
            case COLUMN_LIST:
            case DYNAMIC_STAR:
            case GEOMETRY:

            default:
                return PG_TYPE_UNKNOWN;
        }
    }

    public static RelDataType toInternal(int type, RelDataTypeFactory factory) {
        PgType type1 = getType(type);
        if (type1.elementType != 0) {
            return factory.createArrayType(toInternal(type1.elementType, factory), -1);
        } else if (PG_TYPE_BOOL.equals(type1)) {
            return factory.createSqlType(SqlTypeName.BOOLEAN);
        } else if (PG_TYPE_INT2.equals(type1)) {
            return factory.createSqlType(SqlTypeName.SMALLINT);
        } else if (PG_TYPE_INT4.equals(type1)) {
            return factory.createSqlType(SqlTypeName.INTEGER);
        } else if (PG_TYPE_INT8.equals(type1)) {
            return factory.createSqlType(SqlTypeName.BIGINT);
        } else if (PG_TYPE_NUMERIC.equals(type1)) {
            return factory.createSqlType(SqlTypeName.DECIMAL);
        } else if (PG_TYPE_FLOAT4.equals(type1)) {
            return factory.createSqlType(SqlTypeName.FLOAT);
        } else if (PG_TYPE_FLOAT8.equals(type1)) {
            return factory.createSqlType(SqlTypeName.DOUBLE);
        } else if (PG_TYPE_CHAR.equals(type1)) {
            return factory.createSqlType(SqlTypeName.CHAR);
        } else if (PG_TYPE_VARCHAR.equals(type1)) {
            return factory.createSqlType(SqlTypeName.VARCHAR);
        } else if (PG_TYPE_BYTEA.equals(type1)) {
            return factory.createSqlType(SqlTypeName.BINARY);
        } else if (PG_TYPE_DATE.equals(type1)) {
            return factory.createSqlType(SqlTypeName.DATE);
        } else if (PG_TYPE_TIME.equals(type1)) {
            return factory.createSqlType(SqlTypeName.TIME);
        } else if (PG_TYPE_TIMESTAMP.equals(type1)) {
            return factory.createSqlType(SqlTypeName.TIMESTAMP);
        } else if (PG_TYPE_TIMETZ.equals(type1)) {
            return factory.createSqlType(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE);
        } else if (PG_TYPE_TIMESTAMPTZ.equals(type1)) {
            return factory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        } else if (PG_TYPE_ANY.equals(type1)) {
            return factory.createSqlType(SqlTypeName.ANY);
        } else if (PG_TYPE_CURSOR.equals(type1)) {
            return factory.createSqlType(SqlTypeName.CURSOR);
        } else {
            return factory.createUnknownType();
        }
    }

    public static <T> T readParameter(Session session, ByteBuf dst, ParameterDescription desc, int format) throws ProtocolException {
        if (format == Constants.TEXT)
            return desc.getType().fromText(session, dst);
        else if (format == Constants.BINARY)
            return desc.getType().fromBinary(session, dst);
        else
            throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Unexpected format code: " + format);
    }

    public static void writeColumn(Session session, ByteBuf dst, Object value, ColumnDescription desc) throws ProtocolException {
        int format = desc.getFormat();
        if (format == Constants.TEXT)
            desc.getType().asText(session, dst, value);
        else if (format == Constants.BINARY)
            desc.getType().asBinary(session, dst, value);
        else
            throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Unexpected format code: " + format);
    }

    protected static PgType arrayType(PgType type) {
        if (type == PG_TYPE_INT2)
            return new PgTypeInt2Array();
        if (type == PG_TYPE_INT4)
            return new PgTypeInt4Array();

        // TODO implement array type encoder/decoder
        return new PgType(type.arrayType, type.name + "_array", -1, 0, type.id){};
    }

    protected static void checkType(Object value, Class<?> type) throws ProtocolException {
        if (type.isInstance(value))
            return;
        throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value type: " + value.getClass());
    }

    protected static void checkLen(ByteBuf src, int expected) throws ProtocolException {
        int actual = src.readInt();
        if (actual != expected)
            throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value length, actual: " + actual + "; expected: " + expected);
    }

    protected static boolean readNull(ByteBuf src) {
        if (src.getInt(src.readerIndex()) == -1) {
            src.skipBytes(4);
            return true;
        }
        return false;
    }

    protected static boolean writeNull(ByteBuf dst, Object value) {
        if (value == null) {
            dst.writeInt(-1);
            return true;
        }
        return false;
    }

    protected static String readText(Session session, ByteBuf src) {
        return src.readCharSequence(src.readInt(), session.getCharset()).toString();
    }

    protected static void writeText(Session session, ByteBuf dst, String text) {
        byte[] bytes = text.getBytes(session.getCharset());
        dst.writeInt(bytes.length).writeBytes(bytes);
    }

    protected static int countNulls(Object[] values) {
        int res = 0;
        for (Object value : values) {
            if (value == null)
                res++;
        }
        return res;
    }
}
