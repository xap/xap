package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.sql.aggregatornode.netty.exception.BreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.NonBreakingException;
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
import java.nio.charset.StandardCharsets;
import java.util.*;

@SuppressWarnings("unchecked")
public abstract class PgType {
    private static final String DELIMITER = ",";

    public static final PgType PG_TYPE_UNKNOWN = new PgType(705, "unknown", -2, 0, 0) {};
    public static final PgType PG_TYPE_ANY = new PgType(2276, "any", 4, 0, 0) {};

    public static final PgType PG_TYPE_BOOL = new TypeBool();

    public static final PgType PG_TYPE_BYTEA = new TypeBytea();

    public static final PgType PG_TYPE_CHAR = new TypeChar();

    public static final PgType PG_TYPE_NAME = new TypeName();

    public static final PgType PG_TYPE_INT8 = new TypeInt8();

    public static final PgType PG_TYPE_INT2 = new TypeInt2();

    public static final PgType PG_TYPE_INT2VECTOR = new TypeInt2vector();

    public static final PgType PG_TYPE_INT4 = new TypeInt4();

    public static final PgType PG_TYPE_REGPROC = new TypeRegproc();

    public static final PgType PG_TYPE_TEXT = new TypeText();

    public static final PgType PG_TYPE_OID = new TypeOid();

    public static final PgType PG_TYPE_OIDVECTOR = new TypeOidVector();

    public static final PgType PG_TYPE_FLOAT4 = new TypeFloat4();

    public static final PgType PG_TYPE_FLOAT8 = new TypeFloat8();

    public static final PgType PG_TYPE_BPCHAR = new TypeBpchar();

    public static final PgType PG_TYPE_VARCHAR = new TypeVarchar();

    public static final PgType PG_TYPE_DATE = new TypeDate();

    public static final PgType PG_TYPE_TIME = new TypeTime();

    public static final PgType PG_TYPE_TIMESTAMP = new TypeTimestamp();

    public static final PgType PG_TYPE_TIMESTAMPTZ = new TypeTamestampTZ();

    public static final PgType PG_TYPE_INTERVAL = new TypeInterval();

    public static final PgType PG_TYPE_TIMETZ = new TypeTimeTZ();

    public static final PgType PG_TYPE_NUMERIC = new TypeNumeric();

    public static final PgType PG_TYPE_CURSOR = new TypeCursor();

    protected final int id;
    protected final String name;
    protected final int length;
    protected final int arrayType;
    protected final int elementType;

    private static final IntObjectHashMap<PgType> elementToArray;
    private static final IntObjectHashMap<PgType> typeIdToType;
    
    static {
        Field[] fields = PgType.class.getDeclaredFields();
        elementToArray = new IntObjectHashMap<>(fields.length * 2);
        typeIdToType = new IntObjectHashMap<>(fields.length * 2);
        Set<PgType> typeSet = new HashSet<>();
        try {
            for (Field field : fields) {
                if (PgType.class.isAssignableFrom(field.getType()) && Modifier.isStatic(field.getModifiers())) {
                    field.setAccessible(true);
                    PgType type = (PgType) field.get(null);
                    if (typeSet.add(type)) {
                        typeIdToType.put(type.id, type);
                        if (type.arrayType != 0) {
                            PgType arrayType = arrayType(type);
                            if (typeSet.add(arrayType))
                                elementToArray.put(type.id, arrayType);
                        } 
                    }
                }
            }
        } catch (Throwable e) {
            // failed to initialize types
        }
    }

    private PgType(int id, String name, int length, int arrayType, int elementType) {
        this.id = id;
        this.name = name;
        this.length = length;
        this.arrayType = arrayType;
        this.elementType = elementType;
    }

    public final int getId() {
        return id;
    }

    public final String getName() {
        return name;
    }

    public final int getLength() {
        return length;
    }

    protected final void asText(Session session, ByteBuf dst, Object value) throws ProtocolException {
        if (writeNull(dst, value))
            return;

        asTextInternal(session, dst, value);
    }

    protected final <T> T fromText(Session session, ByteBuf src) throws ProtocolException {
        if (readNull(src))
            return null;
        return fromTextInternal(session, src);
    }

    protected final void asBinary(Session session, ByteBuf dst, Object value) throws ProtocolException {
        if (writeNull(dst, value))
            return;
        asBinaryInternal(session, dst, value);
    }

    protected final <T> T fromBinary(Session session, ByteBuf src) throws ProtocolException {
        if (readNull(src))
            return null;
        return fromBinaryInternal(session, src);
    }

    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Unsupported data type: " + id);
    }

    protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
        throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Unsupported data type: " + id);
    }

    protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Unsupported data type: " + id);
    }

    protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
        throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Unsupported data type: " + id);
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PgType pgType = (PgType) o;

        return id == pgType.id;
    }

    @Override
    public final int hashCode() {
        return id;
    }

    public static PgType getType(int id) {
        return typeIdToType.getOrDefault(id, PG_TYPE_UNKNOWN);
    }

    public static PgType getArrayType(int elementTypeId) {
        return elementToArray.getOrDefault(elementTypeId, PG_TYPE_UNKNOWN);
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
        return toInternal(getType(type), factory);
    }

    public static RelDataType toInternal(PgType type, RelDataTypeFactory factory) {
        if (type.elementType != 0) {
            return factory.createArrayType(toInternal(type.elementType, factory), -1);
        } else if (PG_TYPE_BOOL.equals(type)) {
            return factory.createSqlType(SqlTypeName.BOOLEAN);
        } else if (PG_TYPE_INT2.equals(type)) {
            return factory.createSqlType(SqlTypeName.SMALLINT);
        } else if (PG_TYPE_INT4.equals(type)) {
            return factory.createSqlType(SqlTypeName.INTEGER);
        } else if (PG_TYPE_INT8.equals(type)) {
            return factory.createSqlType(SqlTypeName.BIGINT);
        } else if (PG_TYPE_NUMERIC.equals(type)) {
            return factory.createSqlType(SqlTypeName.DECIMAL);
        } else if (PG_TYPE_FLOAT4.equals(type)) {
            return factory.createSqlType(SqlTypeName.FLOAT);
        } else if (PG_TYPE_FLOAT8.equals(type)) {
            return factory.createSqlType(SqlTypeName.DOUBLE);
        } else if (PG_TYPE_CHAR.equals(type)) {
            return factory.createSqlType(SqlTypeName.CHAR);
        } else if (PG_TYPE_VARCHAR.equals(type)) {
            return factory.createSqlType(SqlTypeName.VARCHAR);
        } else if (PG_TYPE_BYTEA.equals(type)) {
            return factory.createSqlType(SqlTypeName.BINARY);
        } else if (PG_TYPE_DATE.equals(type)) {
            return factory.createSqlType(SqlTypeName.DATE);
        } else if (PG_TYPE_TIME.equals(type)) {
            return factory.createSqlType(SqlTypeName.TIME);
        } else if (PG_TYPE_TIMESTAMP.equals(type)) {
            return factory.createSqlType(SqlTypeName.TIMESTAMP);
        } else if (PG_TYPE_TIMETZ.equals(type)) {
            return factory.createSqlType(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE);
        } else if (PG_TYPE_TIMESTAMPTZ.equals(type)) {
            return factory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        } else if (PG_TYPE_ANY.equals(type)) {
            return factory.createSqlType(SqlTypeName.ANY);
        } else if (PG_TYPE_CURSOR.equals(type)) {
            return factory.createSqlType(SqlTypeName.CURSOR);
        } else {
            return factory.createUnknownType();
        }
    }

    private static PgType arrayType(PgType type) {
        if (type == PG_TYPE_INT2)
            return new PgTypeInt2Array();
        if (type == PG_TYPE_INT4)
            return new PgTypeInt4Array();

        // TODO implement array type encoder/decoder
        return new PgType(type.arrayType, type.name + "_array", -1, 0, type.id){};
    }

    private static void checkType(Object value, Class<?> type) throws ProtocolException {
        if (type.isInstance(value))
            return;
        throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value type: " + value.getClass());
    }

    private static void checkLen(ByteBuf src, int expected) throws ProtocolException {
        int actual = src.readInt();
        if (actual != expected)
            throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value length, actual: " + actual + "; expected: " + expected);
    }

    private static boolean readNull(ByteBuf src) {
        if (src.getInt(src.readerIndex()) == -1) {
            src.skipBytes(4);
            return true;
        }
        return false;
    }

    private static boolean writeNull(ByteBuf dst, Object value) {
        if (value == null) {
            dst.writeInt(-1);
            return true;
        }
        return false;
    }

    private static String readText(Session session, ByteBuf src) {
        return src.readCharSequence(src.readInt(), session.getCharset()).toString();
    }

    private static void writeText(Session session, ByteBuf dst, String text) {
        byte[] bytes = text.getBytes(session.getCharset());
        dst.writeInt(bytes.length).writeBytes(bytes);
    }

    private static int countNulls(Object[] values) {
        int res = 0;
        for (Object value : values) {
            if (value == null)
                res++;
        }
        return res;
    }

    // --------------------------------------------------------------

    private static class TypeBool extends PgType {
        public TypeBool() {
            super(16, "bool", 1, 1000, 0);
        }

        @Override
        protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            checkType(value, Boolean.class);
            writeText(session, dst, ((boolean) value) ? "t" : "f");
        }

        @Override
        protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
            String text = readText(session, src);
            switch (text.toLowerCase()) {
                case "t":
                    return (T) Boolean.TRUE;
                case "f":
                    return (T) Boolean.FALSE;
                default:
                    throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Cannot read value: " + text);
            }
        }

        @Override
        protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            checkType(value, Boolean.class);
            dst.writeInt(1).writeBoolean((boolean) value);
        }

        @Override
        protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
            checkLen(src, 1);
            return (T) Boolean.valueOf(src.readBoolean());
        }
    }

    private static class TypeChar extends PgType {
        public TypeChar() {
            super(18, "char", 1, 1002, 0);
        }

        @Override
        protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            checkType(value, Character.class);
            PgType.writeText(session, dst, value.toString());
        }

        @Override
        protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
            String text = PgType.readText(session, src);
            if (text.length() != 1)
                throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Cannot read value: " + text);
            return (T) Character.valueOf(text.charAt(0));
        }

        @Override
        protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            checkType(value, Character.class);
            dst.writeInt(1).writeByte((char) value);
        }

        @Override
        protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
            checkLen(src, 1);
            return (T) Character.valueOf((char) src.readByte());
        }
    }

    private static class TypeInt8 extends PgType {
        public TypeInt8() {
            super(20, "int8", 8, 1016, 0);
        }

        @Override
        protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            checkType(value, Long.class);
            PgType.writeText(session, dst, value.toString());
        }

        @Override
        protected <T> T fromTextInternal(Session session, ByteBuf src) {
            return (T) Long.valueOf(PgType.readText(session, src));
        }

        @Override
        protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            checkType(value, Long.class);
            dst.writeInt(8).writeLong((Long) value);
        }

        @Override
        protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
            checkLen(src, 8);
            return (T) Long.valueOf(src.readLong());
        }
    }

    private static class TypeInt2 extends PgType {
        public TypeInt2() {
            super(21, "int2", 2, 1005, 0);
        }

        @Override
        protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            checkType(value, Short.class);
            PgType.writeText(session, dst, value.toString());
        }

        @Override
        protected <T> T fromTextInternal(Session session, ByteBuf src) {
            return (T) Short.valueOf(PgType.readText(session, src));
        }

        @Override
        protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            checkType(value, Short.class);
            dst.writeInt(2).writeShort((Short) value);
        }

        @Override
        protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
            checkLen(src, 2);
            return (T) Short.valueOf(src.readShort());
        }
    }

    private static class TypeInt4 extends PgType {
        public TypeInt4() {
            super(23, "int4", 4, 1007, 0);
        }

        @Override
        protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            PgType.checkType(value, Integer.class);
            PgType.writeText(session, dst, value.toString());
        }

        @Override
        protected <T> T fromTextInternal(Session session, ByteBuf src) {
            return (T) Integer.valueOf(PgType.readText(session, src));
        }

        @Override
        protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            PgType.checkType(value, Integer.class);
            dst.writeInt(4).writeInt((Integer) value);
        }

        @Override
        protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
            PgType.checkLen(src, 4);
            return (T) Integer.valueOf(src.readInt());
        }
    }

    private static class TypeOid extends PgType {
        public TypeOid() {
            super(26, "oid", 4, 1028, 0);
        }

        @Override
        protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            PgType.checkType(value, Integer.class);
            PgType.writeText(session, dst, value.toString());
        }

        @Override
        protected <T> T fromTextInternal(Session session, ByteBuf src) {
            return (T) Integer.valueOf(PgType.readText(session, src));
        }

        @Override
        protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            PgType.checkType(value, Integer.class);
            dst.writeInt(4).writeInt((Integer) value);
        }

        @Override
        protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
            PgType.checkLen(src, 4);
            return (T) Integer.valueOf(src.readInt());
        }
    }

    private static class TypeFloat4 extends PgType {
        public TypeFloat4() {
            super(700, "float8", 4, 1021, 0);
        }

        @Override
        protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            PgType.checkType(value, Float.class);
            PgType.writeText(session, dst, value.toString());
        }

        @Override
        protected <T> T fromTextInternal(Session session, ByteBuf src) {
            return (T) Float.valueOf(PgType.readText(session, src));
        }

        @Override
        protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            PgType.checkType(value, Float.class);
            dst.writeInt(4).writeFloat((Float) value);
        }

        @Override
        protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
            PgType.checkLen(src, 4);
            return (T) Float.valueOf(src.readFloat());
        }
    }

    private static class TypeFloat8 extends PgType {
        public TypeFloat8() {
            super(701, "float8", 8, 1022, 0);
        }

        @Override
        protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            PgType.checkType(value, Double.class);
            PgType.writeText(session, dst, value.toString());
        }

        @Override
        protected <T> T fromTextInternal(Session session, ByteBuf src) {
            return (T) Double.valueOf(PgType.readText(session, src));
        }

        @Override
        protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            PgType.checkType(value, Double.class);
            dst.writeInt(8).writeDouble((Double) value);
        }

        @Override
        protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
            PgType.checkLen(src, 8);
            return (T) Double.valueOf(src.readDouble());
        }
    }

    private static class TypeName extends PgType {
        public TypeName() {
            super(19, "name", 63, 1003, 0);
        }

        @Override
        protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            PgType.checkType(value, String.class);
            PgType.writeText(session, dst, value.toString());
        }

        @Override
        protected <T> T fromTextInternal(Session session, ByteBuf src) {
            return (T) PgType.readText(session, src);
        }

        @Override
        protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            PgType.checkType(value, String.class);
            PgType.writeText(session, dst, value.toString());
        }

        @Override
        protected <T> T fromBinaryInternal(Session session, ByteBuf src) {
            return (T) PgType.readText(session, src);
        }
    }

    private static class TypeText extends PgType {
        public TypeText() {
            super(25, "text", -1, 1009, 0);
        }

        @Override
        protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            PgType.checkType(value, String.class);
            PgType.writeText(session, dst, value.toString());
        }

        @Override
        protected <T> T fromTextInternal(Session session, ByteBuf src) {
            return (T) PgType.readText(session, src);
        }

        @Override
        protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            PgType.checkType(value, String.class);
            PgType.writeText(session, dst, value.toString());
        }

        @Override
        protected <T> T fromBinaryInternal(Session session, ByteBuf src) {
            return (T) PgType.readText(session, src);
        }
    }

    private static class TypeVarchar extends PgType {
        public TypeVarchar() {
            super(1043, "varchar", -1, 1015, 0);
        }

        @Override
        protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            PgType.checkType(value, String.class);
            PgType.writeText(session, dst, value.toString());
        }

        @Override
        protected <T> T fromTextInternal(Session session, ByteBuf src) {
            return (T) PgType.readText(session, src);
        }

        @Override
        protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            PgType.checkType(value, String.class);
            PgType.writeText(session, dst, value.toString());
        }

        @Override
        protected <T> T fromBinaryInternal(Session session, ByteBuf src) {
            return (T) PgType.readText(session, src);
        }
    }

    private static class TypeBytea extends PgType {
        private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

        private static byte getHex(byte b) {
            // 0-9 == 48-57
            if (b <= 57) {
                return (byte) (b - 48);
            }

            // a-f == 97-102
            if (b >= 97) {
                return (byte) (b - 97 + 10);
            }

            // A-F == 65-70
            return (byte) (b - 65 + 10);
        }

        public TypeBytea() {
            super(17, "bytea", -1, 1001, 0);
        }

        @Override
        protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            PgType.checkType(value, byte[].class);
            byte[] bytes = (byte[]) value;
            char[] hexChars = new char[bytes.length * 2 + 2];
            hexChars[0] = '\\';
            hexChars[1] = 'x';
            for (int j = 0; j < bytes.length; j++) {
                int v = bytes[j] & 0xFF;
                hexChars[2 + j * 2] = HEX_ARRAY[v >>> 4];
                hexChars[2 + j * 2 + 1] = HEX_ARRAY[v & 0x0F];
            }
            PgType.writeText(session, dst, new String(hexChars));
        }

        @Override
        protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
            byte[] s = PgType.readText(session, src).getBytes(StandardCharsets.US_ASCII);
            if (s.length < 2 || s[0] != '\\' || s[1] != 'x')
                throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Unsupported text format");

            byte[] output = new byte[(s.length - 2) / 2];
            for (int i = 0; i < output.length; i++) {
                byte b1 = getHex(s[2 + i * 2]);
                byte b2 = getHex(s[2 + i * 2 + 1]);
                 output[i] = (byte) ((b1 << 4) | (b2 & 0xff));
            }
            return (T) output;
        }

        @Override
        protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            PgType.checkType(value, byte[].class);
            byte[] bytes = (byte[]) value;
            dst.writeInt(bytes.length).writeBytes(bytes);
        }

        @Override
        protected <T> T fromBinaryInternal(Session session, ByteBuf src) {
            int len = src.readInt();
            byte[] bytes = new byte[len];
            src.readBytes(bytes);
            return (T) bytes;
        }
    }

    private static class PgTypeInt2Array extends PgType {
        public PgTypeInt2Array() {
            super(PG_TYPE_INT2.arrayType, PG_TYPE_INT2.name + "_array", -1, 0, PG_TYPE_INT2.id);
        }

        @Override
        protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            Object[] values;
            try {
                values = (Object[]) value;
            } catch (Exception e) {
                throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value type: " + value.getClass());
            }

            StringBuilder sb = new StringBuilder();

            sb.append('{');
            for (int i = 0; i < values.length; i++) {
                if (i > 0)
                    sb.append(DELIMITER);

                Short val;
                try {
                    val = (Short) values[i];
                } catch (Exception e) {
                    throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value type: " + value.getClass());
                }

                if (val == null)
                    sb.append("NULL");
                else {
                    sb.append(val);
                }
            }
            sb.append('}');

            PgType.writeText(session, dst, sb.toString());
        }

        @Override
        protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
            ArrayList<Short> values = new ArrayList<>();
            char[] chars = PgType.readText(session, src).toCharArray();
            StringBuilder sb = new StringBuilder();
            boolean arrayOpened = false;
            boolean arrayClosed = false;
            int startOffset = 0;
            if (chars[0] == '[') {
                while (chars[startOffset] != '=') {
                    startOffset++;
                }
                startOffset++; // skip =
            }
            for (int i = startOffset; i < chars.length; i++) {
                char ch = chars[i];
                if (Character.isWhitespace(ch))
                    continue;

                switch (ch) {
                    case '{': {
                        if (arrayClosed)
                            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected array start");

                        if (arrayOpened)
                            throw new NonBreakingException(ErrorCodes.UNSUPPORTED_FEATURE, "Multidimensional arrays are unsupported");

                        arrayOpened = true;

                        break;
                    }

                    case '}': {
                        if (!arrayOpened || arrayClosed)
                            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected array end");

                        arrayClosed = true;
                        if (sb.length() > 0) {
                            try {
                                String val = sb.toString();
                                sb.setLength(0);

                                if ("NULL".equals(val.toUpperCase(Locale.ROOT)))
                                    values.add(null);
                                else
                                    values.add(Short.parseShort(val));
                            } catch (NumberFormatException e) {
                                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");
                            }
                        }

                        break;
                    }

                    case ',': {
                        if (!arrayOpened || arrayClosed || sb.length() == 0)
                            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected array delimiter");

                        try {
                            String val = sb.toString();
                            sb.setLength(0);

                            if ("NULL".equals(val.toUpperCase(Locale.ROOT)))
                                values.add(null);
                            else
                                values.add(Short.parseShort(val));
                        } catch (NumberFormatException e) {
                            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");
                        }

                        break;
                    }

                    default:
                        sb.append(ch);
                        break;
                }
            }

            if (!arrayOpened || !arrayClosed)
                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");

            return (T) values.toArray();
        }

        @Override
        protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            Object[] values;
            try {
                values = (Object[]) value;
            } catch (Exception e) {
                throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value type: " + value.getClass());
            }

            int idx = dst.writerIndex();
            dst.writeInt(0);

            dst.writeInt(1); // dimensions
            dst.writeInt(countNulls(values)); // nulls
            dst.writeInt(elementType); // element type
            dst.writeInt(values.length); // length
            dst.writeInt(1); // base
            for (Object value0 : values) {
                PG_TYPE_INT2.asBinary(session, dst, value0);
            }

            dst.setInt(idx, dst.writerIndex() - idx - 4);
        }

        @Override
        protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
            int end = src.readInt() + src.writerIndex();
            if (src.readInt() != 1)
                throw new NonBreakingException(ErrorCodes.UNSUPPORTED_FEATURE, "Multidimensional arrays are unsupported");
            src.skipBytes(4); // null element count
            if (src.readInt() != elementType)
                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "unexpected element type");
            int length = src.readInt();
            ArrayList<Short> values = new ArrayList<>(length);
            src.skipBytes(4); // base
            for (int i = 0; i < length; i++) {
                values.add(PG_TYPE_INT2.fromBinary(session, src));
            }
            if (src.writerIndex() != end)
                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");

            return (T) values.toArray();
        }
    }

    private static class PgTypeInt4Array extends PgType {
        public PgTypeInt4Array() {
            super(PG_TYPE_INT4.arrayType, PG_TYPE_INT4.name + "_array", -1, 0, PG_TYPE_INT4.id);
        }

        @Override
        protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            Object[] values;
            try {
                values = (Object[]) value;
            } catch (Exception e) {
                throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value type: " + value.getClass());
            }

            StringBuilder sb = new StringBuilder();

            sb.append('{');
            for (int i = 0; i < values.length; i++) {
                if (i > 0)
                    sb.append(DELIMITER);

                Integer val;
                try {
                    val = (Integer) values[i];
                } catch (Exception e) {
                    throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value type: " + value.getClass());
                }

                if (val == null)
                    sb.append("NULL");
                else {
                    sb.append(val);
                }
            }
            sb.append('}');

            PgType.writeText(session, dst, sb.toString());
        }

        @Override
        protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
            ArrayList<Integer> values = new ArrayList<>();
            char[] chars = PgType.readText(session, src).toCharArray();
            StringBuilder sb = new StringBuilder();
            boolean arrayOpened = false;
            boolean arrayClosed = false;
            int startOffset = 0;
            if (chars[0] == '[') {
                while (chars[startOffset] != '=') {
                    startOffset++;
                }
                startOffset++; // skip =
            }
            for (int i = startOffset; i < chars.length; i++) {
                char ch = chars[i];
                if (Character.isWhitespace(ch))
                    continue;

                switch (ch) {
                    case '{': {
                        if (arrayClosed)
                            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected array start");

                        if (arrayOpened)
                            throw new NonBreakingException(ErrorCodes.UNSUPPORTED_FEATURE, "Multidimensional arrays are unsupported");

                        arrayOpened = true;

                        break;
                    }

                    case '}': {
                        if (!arrayOpened || arrayClosed)
                            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected array end");

                        arrayClosed = true;
                        if (sb.length() > 0) {
                            try {
                                String val = sb.toString();
                                sb.setLength(0);

                                if ("NULL".equals(val.toUpperCase(Locale.ROOT)))
                                    values.add(null);
                                else
                                    values.add(Integer.parseInt(val));
                            } catch (NumberFormatException e) {
                                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");
                            }
                        }

                        break;
                    }

                    case ',': {
                        if (!arrayOpened || arrayClosed || sb.length() == 0)
                            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected array delimiter");

                        try {
                            String val = sb.toString();
                            sb.setLength(0);

                            if ("NULL".equals(val.toUpperCase(Locale.ROOT)))
                                values.add(null);
                            else
                                values.add(Integer.parseInt(val));
                        } catch (NumberFormatException e) {
                            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");
                        }

                        break;
                    }

                    default:
                        sb.append(ch);
                        break;
                }
            }

            if (!arrayOpened || !arrayClosed)
                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");

            return (T) values.toArray();
        }

        @Override
        protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            Object[] values;
            try {
                values = (Object[]) value;
            } catch (Exception e) {
                throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value type: " + value.getClass());
            }

            int idx = dst.writerIndex();
            dst.writeInt(0);

            dst.writeInt(1); // dimensions
            dst.writeInt(countNulls(values)); // nulls
            dst.writeInt(elementType); // element type
            dst.writeInt(values.length); // length
            dst.writeInt(1); // base
            for (Object value0 : values) {
                PG_TYPE_INT4.asBinary(session, dst, value0);
            }

            dst.setInt(idx, dst.writerIndex() - idx - 4);
        }

        @Override
        protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
            int end = src.readInt() + src.writerIndex();
            if (src.readInt() != 1)
                throw new NonBreakingException(ErrorCodes.UNSUPPORTED_FEATURE, "Multidimensional arrays are unsupported");
            src.skipBytes(4); // null element count
            if (src.readInt() != elementType)
                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "unexpected element type");
            int length = src.readInt();
            ArrayList<Short> values = new ArrayList<>(length);
            src.skipBytes(4); // base
            for (int i = 0; i < length; i++) {
                values.add(PG_TYPE_INT4.fromBinary(session, src));
            }
            if (src.writerIndex() != end)
                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");

            return (T) values.toArray();
        }
    }

    private static class TypeOidVector extends PgType {
        public TypeOidVector() {
            super(30, "oidvector", -1, 0, PG_TYPE_OID.id);
        }

        @Override
        protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            Object[] values;
            try {
                values = (Object[]) value;
            } catch (Exception e) {
                throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value type: " + value.getClass());
            }

            StringBuilder sb = new StringBuilder();

            sb.append('{');
            for (int i = 0; i < values.length; i++) {
                if (i > 0)
                    sb.append(DELIMITER);

                Integer val;
                try {
                    val = (Integer) values[i];
                } catch (Exception e) {
                    throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value type: " + value.getClass());
                }

                if (val == null)
                    sb.append("NULL");
                else {
                    sb.append(val);
                }
            }
            sb.append('}');

            PgType.writeText(session, dst, sb.toString());
        }

        @Override
        protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
            ArrayList<Integer> values = new ArrayList<>();
            char[] chars = PgType.readText(session, src).toCharArray();
            StringBuilder sb = new StringBuilder();
            boolean arrayOpened = false;
            boolean arrayClosed = false;
            int startOffset = 0;
            if (chars[0] == '[') {
                while (chars[startOffset] != '=') {
                    startOffset++;
                }
                startOffset++; // skip =
            }
            for (int i = startOffset; i < chars.length; i++) {
                char ch = chars[i];
                if (Character.isWhitespace(ch))
                    continue;

                switch (ch) {
                    case '{': {
                        if (arrayClosed)
                            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected array start");

                        if (arrayOpened)
                            throw new NonBreakingException(ErrorCodes.UNSUPPORTED_FEATURE, "Multidimensional arrays are unsupported");

                        arrayOpened = true;

                        break;
                    }

                    case '}': {
                        if (!arrayOpened || arrayClosed)
                            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected array end");

                        arrayClosed = true;
                        if (sb.length() > 0) {
                            try {
                                String val = sb.toString();
                                sb.setLength(0);

                                if ("NULL".equals(val.toUpperCase(Locale.ROOT)))
                                    values.add(null);
                                else
                                    values.add(Integer.parseInt(val));
                            } catch (NumberFormatException e) {
                                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");
                            }
                        }

                        break;
                    }

                    case ',': {
                        if (!arrayOpened || arrayClosed || sb.length() == 0)
                            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected array delimiter");

                        try {
                            String val = sb.toString();
                            sb.setLength(0);

                            if ("NULL".equals(val.toUpperCase(Locale.ROOT)))
                                values.add(null);
                            else
                                values.add(Integer.parseInt(val));
                        } catch (NumberFormatException e) {
                            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");
                        }

                        break;
                    }

                    default:
                        sb.append(ch);
                        break;
                }
            }

            if (!arrayOpened || !arrayClosed)
                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");

            return (T) values.toArray();
        }

        @Override
        protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            Object[] values;
            try {
                values = (Object[]) value;
            } catch (Exception e) {
                throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value type: " + value.getClass());
            }

            int idx = dst.writerIndex();
            dst.writeInt(0);

            dst.writeInt(1); // dimensions
            dst.writeInt(countNulls(values)); // nulls
            dst.writeInt(elementType); // element type
            dst.writeInt(values.length); // length
            dst.writeInt(1); // base
            for (Object value0 : values) {
                PG_TYPE_OID.asBinary(session, dst, value0);
            }

            dst.setInt(idx, dst.writerIndex() - idx - 4);
        }

        @Override
        protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
            int end = src.readInt() + src.writerIndex();
            if (src.readInt() != 1)
                throw new NonBreakingException(ErrorCodes.UNSUPPORTED_FEATURE, "Multidimensional arrays are unsupported");
            src.skipBytes(4); // null element count
            if (src.readInt() != elementType)
                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "unexpected element type");
            int length = src.readInt();
            ArrayList<Short> values = new ArrayList<>(length);
            src.skipBytes(4); // base
            for (int i = 0; i < length; i++) {
                values.add(PG_TYPE_OID.fromBinary(session, src));
            }
            if (src.writerIndex() != end)
                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");

            return (T) values.toArray();
        }
    }

    private static class TypeInt2vector extends PgType {
        public TypeInt2vector() {
            super(22, "int2vector", -1, 1006, PG_TYPE_INT2.id);
        }

        @Override
        protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            Object[] values;
            try {
                values = (Object[]) value;
            } catch (Exception e) {
                throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value type: " + value.getClass());
            }

            StringBuilder sb = new StringBuilder();

            sb.append('{');
            for (int i = 0; i < values.length; i++) {
                if (i > 0)
                    sb.append(DELIMITER);

                Short val;
                try {
                    val = (Short) values[i];
                } catch (Exception e) {
                    throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value type: " + value.getClass());
                }

                if (val == null)
                    sb.append("NULL");
                else {
                    sb.append(val);
                }
            }
            sb.append('}');

            PgType.writeText(session, dst, sb.toString());
        }

        @Override
        protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
            ArrayList<Short> values = new ArrayList<>();
            char[] chars = PgType.readText(session, src).toCharArray();
            StringBuilder sb = new StringBuilder();
            boolean arrayOpened = false;
            boolean arrayClosed = false;
            int startOffset = 0;
            if (chars[0] == '[') {
                while (chars[startOffset] != '=') {
                    startOffset++;
                }
                startOffset++; // skip =
            }
            for (int i = startOffset; i < chars.length; i++) {
                char ch = chars[i];
                if (Character.isWhitespace(ch))
                    continue;

                switch (ch) {
                    case '{': {
                        if (arrayClosed)
                            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected array start");

                        if (arrayOpened)
                            throw new NonBreakingException(ErrorCodes.UNSUPPORTED_FEATURE, "Multidimensional arrays are unsupported");

                        arrayOpened = true;

                        break;
                    }

                    case '}': {
                        if (!arrayOpened || arrayClosed)
                            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected array end");

                        arrayClosed = true;
                        if (sb.length() > 0) {
                            try {
                                String val = sb.toString();
                                sb.setLength(0);

                                if ("NULL".equals(val.toUpperCase(Locale.ROOT)))
                                    values.add(null);
                                else
                                    values.add(Short.parseShort(val));
                            } catch (NumberFormatException e) {
                                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");
                            }
                        }

                        break;
                    }

                    case ',': {
                        if (!arrayOpened || arrayClosed || sb.length() == 0)
                            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected array delimiter");

                        try {
                            String val = sb.toString();
                            sb.setLength(0);

                            if ("NULL".equals(val.toUpperCase(Locale.ROOT)))
                                values.add(null);
                            else
                                values.add(Short.parseShort(val));
                        } catch (NumberFormatException e) {
                            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");
                        }

                        break;
                    }

                    default:
                        sb.append(ch);
                        break;
                }
            }

            if (!arrayOpened || !arrayClosed)
                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");

            return (T) values.toArray();
        }

        @Override
        protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
            Object[] values;
            try {
                values = (Object[]) value;
            } catch (Exception e) {
                throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value type: " + value.getClass());
            }

            int idx = dst.writerIndex();
            dst.writeInt(0);

            dst.writeInt(1); // dimensions
            dst.writeInt(countNulls(values)); // nulls
            dst.writeInt(elementType); // element type
            dst.writeInt(values.length); // length
            dst.writeInt(1); // base
            for (Object value0 : values) {
                PG_TYPE_INT2.asBinary(session, dst, value0);
            }

            dst.setInt(idx, dst.writerIndex() - idx - 4);
        }

        @Override
        protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
            int end = src.readInt() + src.writerIndex();
            if (src.readInt() != 1)
                throw new NonBreakingException(ErrorCodes.UNSUPPORTED_FEATURE, "Multidimensional arrays are unsupported");
            src.skipBytes(4); // null element count
            if (src.readInt() != elementType)
                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "unexpected element type");
            int length = src.readInt();
            ArrayList<Short> values = new ArrayList<>(length);
            src.skipBytes(4); // base
            for (int i = 0; i < length; i++) {
                values.add(PG_TYPE_INT2.fromBinary(session, src));
            }
            if (src.writerIndex() != end)
                throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");

            return (T) values.toArray();
        }
    }

    // TODO implement type encoder/decoder
    private static class TypeDate extends PgType {
        public TypeDate() {
            super(1082, "date", 4, 1182, 0);
        }
    }

    // TODO implement type encoder/decoder
    private static class TypeTime extends PgType {
        public TypeTime() {
            super(1083, "time", 8, 1183, 0);
        }
    }

    // TODO implement type encoder/decoder
    private static class TypeTimestamp extends PgType {
        public TypeTimestamp() {
            super(1114, "timestamp", 8, 1115, 0);
        }
    }

    // TODO implement type encoder/decoder
    private static class TypeTamestampTZ extends PgType {
        public TypeTamestampTZ() {
            super(1184, "timestamptz", 8, 1185, 0);
        }
    }

    // TODO implement type encoder/decoder
    private static class TypeTimeTZ extends PgType {
        public TypeTimeTZ() {
            super(1266, "timetz", 12, 1270, 0);
        }
    }

    // TODO implement type encoder/decoder
    private static class TypeNumeric extends PgType {
        public TypeNumeric() {
            super(1700, "numeric", -1, 1231, 0);
        }
    }

    // TODO implement type encoder/decoder
    private static class TypeCursor extends PgType {
        public TypeCursor() {
            super(1790, "refcursor", -1, 2201, 0);
        }
    }

    // TODO implement type encoder/decoder
    private static class TypeInterval extends PgType {
        public TypeInterval() {
            super(1186, "interval", 16, 1187, 0);
        }
    }

    // TODO implement type encoder/decoder
    private static class TypeBpchar extends PgType {
        public TypeBpchar() {
            super(1042, "bpchar", -1, 1014, 0);
        }
    }

    // TODO implement type encoder/decoder
    private static class TypeRegproc extends PgType {
        public TypeRegproc() {
            super(24, "regproc", 4, 1008, 0);
        }
    }
}
