package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.utils.PgType;
import io.netty.buffer.ByteBuf;

public class ParameterDescription extends TypeAware {
    public ParameterDescription(PgType type) {
        super(type);
    }

    public <T> T read(Session session, ByteBuf buf, int format) throws ProtocolException {
        return PgType.readParameter(session, buf, this, format);
    }
}
