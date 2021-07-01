package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;
import com.gigaspaces.sql.aggregatornode.netty.exception.BreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.query.Session;
import io.netty.buffer.ByteBuf;

public class TypeChar extends PgType {
    public static final PgType INSTANCE = new TypeChar();

    public TypeChar() {
        super(PgTypeDescriptor.CHAR);
    }

    @Override
    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, Character.class);
        TypeUtils.writeText(session, dst, value.toString());
    }

    @Override
    protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
        String text = TypeUtils.readText(session, src);
        if (text.length() != 1)
            throw new BreakingException(ErrorCodes.INTERNAL_ERROR, "Cannot read value: " + text);
        return (T) Character.valueOf(text.charAt(0));
    }

    @Override
    protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, Character.class);
        dst.writeInt(1).writeByte((char) value);
    }

    @Override
    protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
        TypeUtils.checkLen(src, 1);
        return (T) Character.valueOf((char) src.readByte());
    }
}
