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

public class TypeBool extends PgType {
    public static final PgType INSTANCE = new TypeBool();

    public TypeBool() {
        super(PgTypeDescriptor.BOOL);
    }

    @Override
    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, Boolean.class);
        TypeUtils.writeText(session, dst, ((boolean) value) ? "t" : "f");
    }

    @Override
    protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
        String text = TypeUtils.readText(session, src);
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
        TypeUtils.checkType(value, Boolean.class);
        dst.writeInt(1).writeBoolean((boolean) value);
    }

    @Override
    protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
        TypeUtils.checkLen(src, 1);
        return (T) Boolean.valueOf(src.readBoolean());
    }
}
