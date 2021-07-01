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
