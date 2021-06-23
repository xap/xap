package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.sql.aggregatornode.netty.exception.BreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.query.Session;
import io.netty.buffer.ByteBuf;

public abstract class PgType {
    protected final int id;
    protected final String name;
    protected final int length;
    protected final int arrayType;
    protected final int elementType;

    protected PgType(int id, String name, int length, int arrayType, int elementType) {
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

        return id == pgType.id;
    }

    @Override
    public final int hashCode() {
        return id;
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
}
