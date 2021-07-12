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
import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.query.Session;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public class TypeBytea extends PgType {
    public static final PgType INSTANCE = new TypeBytea();

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
        super(PgTypeDescriptor.BYTEA);
    }

    @Override
    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, byte[].class);
        byte[] bytes = (byte[]) value;
        char[] hexChars = new char[bytes.length * 2 + 2];
        hexChars[0] = '\\';
        hexChars[1] = 'x';
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[2 + j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[2 + j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        TypeUtils.writeText(session, dst, new String(hexChars));
    }

    @Override
    protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
        byte[] s = TypeUtils.readText(session, src).getBytes(StandardCharsets.US_ASCII);
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
        TypeUtils.checkType(value, byte[].class);
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
