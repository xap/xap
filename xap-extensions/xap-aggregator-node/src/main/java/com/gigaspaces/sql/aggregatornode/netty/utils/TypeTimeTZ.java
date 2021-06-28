package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.query.Session;
import io.netty.buffer.ByteBuf;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

// TODO implement type encoder/decoder
public class TypeTimeTZ extends PgType {
    public static final PgType INSTANCE = new TypeTimeTZ();

    public TypeTimeTZ() {
        super(1266, "timetz", 12, 1270, 0);
    }

    @Override
    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, Time.class);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss.S");
        TypeUtils.writeText(session, dst, simpleDateFormat.format(((Time) value)));    }
}
