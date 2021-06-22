package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.sql.aggregatornode.netty.exception.BreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.NonBreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.query.Session;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Locale;

import static com.gigaspaces.sql.aggregatornode.netty.utils.Constants.DELIMITER;

public class PgTypeInt4Array extends PgType {
    public static final PgType INSTANCE = new PgTypeInt4Array();

    public PgTypeInt4Array() {
        super(TypeUtils.PG_TYPE_INT4.arrayType, TypeUtils.PG_TYPE_INT4.name + "_array", -1, 0, TypeUtils.PG_TYPE_INT4.id);
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

        TypeUtils.writeText(session, dst, sb.toString());
    }

    @Override
    protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
        ArrayList<Integer> values = new ArrayList<>();
        char[] chars = TypeUtils.readText(session, src).toCharArray();
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
        dst.writeInt(TypeUtils.countNulls(values)); // nulls
        dst.writeInt(elementType); // element type
        dst.writeInt(values.length); // length
        dst.writeInt(1); // base
        for (Object value0 : values) {
            TypeUtils.PG_TYPE_INT4.asBinary(session, dst, value0);
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
            values.add(TypeUtils.PG_TYPE_INT4.fromBinary(session, src));
        }
        if (src.writerIndex() != end)
            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");

        return (T) values.toArray();
    }
}
