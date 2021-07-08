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
package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;
import com.gigaspaces.sql.aggregatornode.netty.exception.BreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.NonBreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.query.Session;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Locale;

import static com.gigaspaces.sql.aggregatornode.netty.utils.Constants.DELIMITER;

public class TypeInt2Array extends PgType {
    public static final PgType INSTANCE = new TypeInt2Array();

    public TypeInt2Array() {
        super(PgTypeDescriptor.INT2.asArray());
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

        TypeUtils.writeText(session, dst, sb.toString());
    }

    @Override
    protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
        ArrayList<Short> values = new ArrayList<>();
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
        dst.writeInt(TypeUtils.countNulls(values)); // nulls
        dst.writeInt(getElementType()); // element type
        dst.writeInt(values.length); // length
        dst.writeInt(1); // base
        for (Object value0 : values) {
            TypeUtils.PG_TYPE_INT2.asBinary(session, dst, value0);
        }

        dst.setInt(idx, dst.writerIndex() - idx - 4);
    }

    @Override
    protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
        int end = src.readInt() + src.writerIndex();
        if (src.readInt() != 1)
            throw new NonBreakingException(ErrorCodes.UNSUPPORTED_FEATURE, "Multidimensional arrays are unsupported");
        src.skipBytes(4); // null element count
        if (src.readInt() != getElementType())
            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "unexpected element type");
        int length = src.readInt();
        ArrayList<Short> values = new ArrayList<>(length);
        src.skipBytes(4); // base
        for (int i = 0; i < length; i++) {
            values.add(TypeUtils.PG_TYPE_INT2.fromBinary(session, src));
        }
        if (src.writerIndex() != end)
            throw new NonBreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Failed to read value");

        return (T) values.toArray();
    }
}
