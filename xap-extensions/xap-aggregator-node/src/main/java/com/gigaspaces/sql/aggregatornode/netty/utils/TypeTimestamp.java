package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.query.Session;
import io.netty.buffer.ByteBuf;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

// TODO implement type encoder/decoder
public class TypeTimestamp extends PgType {
    public static final PgType INSTANCE = new TypeTimestamp();

    public TypeTimestamp() {
        super(1114, "timestamp", 8, 1115, 0);
    }

    @Override
    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, Timestamp.class);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        TypeUtils.writeText(session, dst, simpleDateFormat.format(((Timestamp) value)));    }
}
