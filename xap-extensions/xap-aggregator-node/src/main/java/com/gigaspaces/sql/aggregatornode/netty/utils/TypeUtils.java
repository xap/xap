package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.sql.aggregatornode.netty.exception.BreakingException;
import io.netty.buffer.ByteBuf;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.Charset;

public class TypeUtils {
    public static int pgType(RelDataType sqlType) {
        SqlTypeName typeName = sqlType.getSqlTypeName();

        switch (typeName) {
            case BOOLEAN:
                return Constants.PG_TYPE_BOOL;
            case TINYINT:
            case SMALLINT:
                return Constants.PG_TYPE_INT2;
            case INTEGER:
                return Constants.PG_TYPE_INT4;
            case BIGINT:
                return Constants.PG_TYPE_INT8;
            case DECIMAL:
                return Constants.PG_TYPE_NUMERIC;
            case FLOAT:
            case REAL:
                return Constants.PG_TYPE_FLOAT4;
            case DOUBLE:
                return Constants.PG_TYPE_FLOAT8;
            case DATE:
                return Constants.PG_TYPE_DATE;
            case TIME:
                return Constants.PG_TYPE_TIME;
            case TIME_WITH_LOCAL_TIME_ZONE:
                return Constants.PG_TYPE_TIMETZ;
            case TIMESTAMP:
                return Constants.PG_TYPE_TIMESTAMP;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Constants.PG_TYPE_TIMESTAMPTZ;
            case VARCHAR:
                return Constants.PG_TYPE_VARCHAR;
            default:
                return Constants.PG_TYPE_UNKNOWN;
        }
    }

    public static RelDataType sqlType(int pgType) {
        return null;
    }

    private TypeUtils() {}

    public static void writeB(ByteBuf buf, Object value, int oid) throws BreakingException {
        if (value == null)
            buf.writeInt(-1);
        else
            throw new BreakingException("unsupported data type: " + oid);
    }

    public static void writeB(ByteBuf buf, Long value, int oid) throws BreakingException {
        if (value == null)
            buf.writeInt(-1);
        else {
            switch (oid) {
                case Constants.PG_TYPE_INT8:
                    buf.writeInt(8);
                    buf.writeLong(value);

                    break;
                default:
                    throw new BreakingException("unsupported data type: " + oid);
            }
        }
    }

    public static void writeB(ByteBuf buf, Integer value, int oid) throws BreakingException {
        if (value == null)
            buf.writeInt(-1);
        else {
            switch (oid) {
                case Constants.PG_TYPE_INT4:
                    buf.writeInt(4);
                    buf.writeInt(value);

                    break;
                case Constants.PG_TYPE_INT2:
                    buf.writeInt(2);
                    buf.writeShort(value);

                    break;
                default:
                    throw new BreakingException("unsupported data type: " + oid);
            }
        }
    }

    public static void writeB(ByteBuf buf, Boolean value, int oid) throws BreakingException {
        if (value == null)
            buf.writeInt(-1);
        else {
            switch (oid) {
                case Constants.PG_TYPE_BOOL:
                    buf.writeInt(1);
                    buf.writeBoolean(value);

                    break;
                default:
                    throw new BreakingException("unsupported data type: " + oid);
            }
        }
    }

    public static void writeS(ByteBuf buf, Object value, int oid, Charset charset) throws BreakingException {
        if (value == null)
            buf.writeInt(-1);
        else
            throw new BreakingException("unsupported data type: " + oid);
    }

    public static void writeS(ByteBuf buf, Long value, int oid, Charset charset) throws BreakingException {
        if (value == null)
            buf.writeInt(-1);
        else {
            switch (oid) {
                case Constants.PG_TYPE_INT8:
                    byte[] val = Long.toString(value, 10).getBytes(charset);
                    buf.writeInt(val.length).writeBytes(val);

                    break;
                default:
                    throw new BreakingException("unsupported data type: " + oid);
            }
        }
    }

    public static void writeS(ByteBuf buf, Integer value, int oid, Charset charset) throws BreakingException {
        if (value == null)
            buf.writeInt(-1);
        else {
            switch (oid) {
                case Constants.PG_TYPE_INT4:
                case Constants.PG_TYPE_INT2:
                    byte[] val = Integer.toString(value, 10).getBytes(charset);
                    buf.writeInt(val.length).writeBytes(val);

                    break;
                default:
                    throw new BreakingException("unsupported data type: " + oid);
            }
        }
    }

    public static void writeS(ByteBuf buf, Boolean value, int oid, Charset charset) throws BreakingException {
        if (value == null)
            buf.writeInt(-1);
        else {
            switch (oid) {
                case Constants.PG_TYPE_BOOL:
                    byte[] val = Boolean.toString(value).getBytes(charset);
                    buf.writeInt(val.length).writeBytes(val);

                    break;
                default:
                    throw new BreakingException("unsupported data type: " + oid);
            }
        }
    }

    public static void writeS(ByteBuf buf, String value, int oid, Charset charset) throws BreakingException {
        if (value == null)
            buf.writeInt(-1);
        else {
            switch (oid) {
                case Constants.PG_TYPE_VARCHAR:
                case Constants.PG_TYPE_BPCHAR:
                case Constants.PG_TYPE_TEXT:
                    byte[] val = value.getBytes(charset);
                    buf.writeInt(val.length).writeBytes(val);

                    break;
                default:
                    throw new BreakingException("unsupported data type: " + oid);
            }
        }
    }

    public static Integer readIntB(ByteBuf buf, int oid) throws BreakingException {
        int len = buf.readInt();
        if (len == -1)
            return null;

        switch (oid) {
            case Constants.PG_TYPE_INT4:
                assert len == 4;
                return buf.readInt();
            case Constants.PG_TYPE_INT2:
                assert len == 2;
                return (int) buf.readShort();
            default:
                throw new BreakingException("unsupported data type: " + oid);
        }
    }

    public static Integer readIntS(ByteBuf buf, int oid, Charset charset) throws BreakingException {
        int len = buf.readInt();
        if (len == -1)
            return null;

        byte[] dst = new byte[len];
        switch (oid) {
            case Constants.PG_TYPE_INT4:
            case Constants.PG_TYPE_INT2:
                buf.readBytes(dst);
                return Integer.parseInt(new String(dst, charset), 10);
            default:
                throw new BreakingException("unsupported data type: " + oid);
        }
    }

    public static Long readLongB(ByteBuf buf, int oid) throws BreakingException {
        int len = buf.readInt();
        if (len == -1)
            return null;

        switch (oid) {
            case Constants.PG_TYPE_INT8:
                assert len == 8;
                return buf.readLong();
            default:
                throw new BreakingException("unsupported data type: " + oid);
        }
    }

    public static Long readLongS(ByteBuf buf, int oid, Charset charset) throws BreakingException {
        int len = buf.readInt();
        if (len == -1)
            return null;

        byte[] dst = new byte[len];
        switch (oid) {
            case Constants.PG_TYPE_INT8:
                buf.readBytes(dst);
                return Long.parseLong(new String(dst, charset), 10);
            default:
                throw new BreakingException("unsupported data type: " + oid);
        }
    }

    public static Boolean readBoolS(ByteBuf buf, int oid, Charset charset) throws BreakingException {
        int len = buf.readInt();
        if (len == -1)
            return null;

        byte[] dst = new byte[len];
        switch (oid) {
            case Constants.PG_TYPE_BOOL:
                buf.readBytes(dst);
                switch (new String(dst, charset).toLowerCase()) {
                    case "t":
                    case "true":
                        return true;
                    case "f":
                    case "false":
                        return false;
                    default:
                        throw new BreakingException("unsupported data type: " + oid);
                }
            default:
                throw new BreakingException("unsupported data type: " + oid);
        }
    }

    public static Boolean readBoolB(ByteBuf buf, int oid) throws BreakingException {
        int len = buf.readInt();
        if (len == -1)
            return null;

        switch (oid) {
            case Constants.PG_TYPE_BOOL:
                assert len == 1;
                return buf.readBoolean();
            default:
                throw new BreakingException("unsupported data type: " + oid);
        }
    }

    public static String readString(ByteBuf buf, int oid, Charset charset) throws BreakingException {
        int len = buf.readInt();
        switch (oid) {
            case Constants.PG_TYPE_VARCHAR:
            case Constants.PG_TYPE_BPCHAR:
            case Constants.PG_TYPE_TEXT:
                return len == -1 ? null : buf.readCharSequence(len, charset).toString();
            default:
                throw new BreakingException("unsupported data type: " + oid);
        }
    }


//    BOOLEAN;   16
//    TINYINT;   ?? 31 - 1023
//    SMALLINT;  21
//    INTEGER;   23
//    BIGINT;    20
//    DECIMAL;   1700
//    FLOAT;     700
//    REAL;      700
//    DOUBLE;    701
//    DATE;      1082
//    TIME;      1083
//    TIME_WITH_LOCAL_TIME_ZONE; 1266
//    TIMESTAMP; 1114
//    TIMESTAMP_WITH_LOCAL_TIME_ZONE; 1184
//    INTERVAL_YEAR;   1186
//    INTERVAL_YEAR_MONTH;  1186
//    INTERVAL_MONTH;       1186
//    INTERVAL_DAY;         1186
//    INTERVAL_DAY_HOUR;    1186
//    INTERVAL_DAY_MINUTE;  1186
//    INTERVAL_DAY_SECOND;  1186
//    INTERVAL_HOUR;        1186
//    INTERVAL_HOUR_MINUTE; 1186
//    INTERVAL_HOUR_SECOND; 1186
//    INTERVAL_MINUTE;      1186
//    INTERVAL_MINUTE_SECOND;  1186
//    INTERVAL_SECOND;      1186
//    CHAR;  18
//    VARCHAR;    1043
//    BINARY;     17
//    VARBINARY;  17
//    NULL;            --
//    ANY;             2276
//    SYMBOL;          --
//    MULTISET;        --
//    ARRAY;           -- depends on array items type
//    MAP;             --
//    DISTINCT;        --
//    STRUCTURED;      --
//    ROW;             --
//    OTHER;           705
//    CURSOR;          1790
//    COLUMN_LIST;     --
//    DYNAMIC_STAR;    --
//    GEOMETRY   600 - 699
}
