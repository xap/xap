package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.sql.aggregatornode.netty.exception.BreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.NonBreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.query.Session;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

import static com.gigaspaces.sql.aggregatornode.netty.utils.Constants.DELIMITER;

public class PgTypeArray<E> extends PgType {
    protected PgTypeArray(int id, String name, int length, int arrayType, int elementType) {
        super(id, name, length, arrayType, elementType);
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
            sb.append(asText(session, cast(values[i])));
        }
        sb.append('}');

        TypeUtils.writeText(session, dst, sb.toString());
    }

    @Override
    protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
        ArrayList<E> values = new ArrayList<>();
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
                    if (sb.length() > 0)
                        values.add(parseText(session, sb.toString()));

                    sb.setLength(0);

                    break;
                }

                case DELIMITER: {
                    if (!arrayOpened || arrayClosed || sb.length() == 0)
                        throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected array delimiter");

                    values.add(parseText(session, sb.toString()));
                    sb.setLength(0);

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
        dst.writeInt(0); // column len

        dst.writeInt(1); // dimensions
        dst.writeInt(countNulls(values)); // nulls
        dst.writeInt(elementType); // element type
        dst.writeInt(values.length); // length
        dst.writeInt(1); // base
        for (Object value0 : values) {
            writeBinary(session, cast(value0), dst);
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
        ArrayList<E> values = new ArrayList<>(length);
        src.skipBytes(4); // base
        for (int i = 0; i < length; i++) {
            values.add(readBinary(session, src));
        }
        if (src.writerIndex() != end)
            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");

        return (T) values.toArray();
    }

    protected String asText(Session session, E value) throws ProtocolException {
        throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Unsupported data type: " + elementType);
    }

    protected E parseText(Session session, String src) throws ProtocolException {
        throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Unsupported data type: " + elementType);
    }

    protected void writeBinary(Session session, E value, ByteBuf dst) throws ProtocolException {
        throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Unsupported data type: " + elementType);
    }

    protected E readBinary(Session session, ByteBuf src) throws ProtocolException {
        throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Unsupported data type: " + elementType);
    }

    @SuppressWarnings("unchecked")
    private E cast(Object value0) throws BreakingException {
        try {
            return (E) value0;
        } catch (Exception e) {
            throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Unexpected value type: " + value0.getClass());
        }
    }

    private int countNulls(Object[] values) {
        int res = 0;
        for (Object value : values) {
            if (value == null)
                res++;
        }
        return res;
    }
}
