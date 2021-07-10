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

public class PgType {

    private final PgTypeDescriptor descriptor;

    protected PgType(PgTypeDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    public PgTypeDescriptor getDescriptor() {
        return descriptor;
    }

    public final int getId() {
        return descriptor.getId();
    }

    public final String getName() {
        return descriptor.getName();
    }

    public final int getLength() {
        return descriptor.getLength();
    }

    public final int getArrayType() {
        return descriptor.getArrayType();
    }

    public int getElementType() {
        return descriptor.getElementType();
    }

    protected final void asText(Session session, ByteBuf dst, Object value) throws ProtocolException {
        if (TypeUtils.writeNull(dst, value))
            return;

        asTextInternal(session, dst, value);
    }

    protected final <T> T fromText(Session session, ByteBuf src) throws ProtocolException {
        if (TypeUtils.readNull(src))
            return null;
        return fromTextInternal(session, src);
    }

    protected final void asBinary(Session session, ByteBuf dst, Object value) throws ProtocolException {
        if (TypeUtils.writeNull(dst, value))
            return;
        asBinaryInternal(session, dst, value);
    }

    protected final <T> T fromBinary(Session session, ByteBuf src) throws ProtocolException {
        if (TypeUtils.readNull(src))
            return null;
        return fromBinaryInternal(session, src);
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PgType pgType = (PgType) o;

        return descriptor.equals(pgType.descriptor);
    }

    @Override
    public final int hashCode() {
        return descriptor.hashCode();
    }

    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Unsupported data type: " + descriptor.getId());
    }

    protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
        throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Unsupported data type: " + descriptor.getId());
    }

    protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Unsupported data type: " + descriptor.getId());
    }

    protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
        throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Unsupported data type: " + descriptor.getId());
    }
}
