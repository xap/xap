package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;
import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.query.Session;
import io.netty.buffer.ByteBuf;

// TODO implement type encoder/decoder
public class TypeTimeTZ extends PgType {
    public static final PgType INSTANCE = new TypeTimeTZ();

    public TypeTimeTZ() {
        super(PgTypeDescriptor.TIME_WITH_TIME_ZONE);
    }

    @Override
    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.writeText(session, dst, session.getDateTimeUtils().toString(value, true));
    }
}
