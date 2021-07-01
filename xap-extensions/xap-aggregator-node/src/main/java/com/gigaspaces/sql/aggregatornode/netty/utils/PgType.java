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
