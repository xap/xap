package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.query.Session;
import io.netty.buffer.ByteBuf;

import java.text.SimpleDateFormat;
import java.util.Date;

// TODO implement type encoder/decoder
public class TypeDate extends PgType {
    public static final PgType INSTANCE = new TypeDate();

    public TypeDate() {
        super(1082, "date", 4, 1182, 0);
    }

    @Override
    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        //parseBackendTimestamp("2001-11-12 10:30:30.2")
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        TypeUtils.checkType(value, Date.class);
        TypeUtils.writeText(session, dst, simpleDateFormat.format(((Date) value)));
    }


}
