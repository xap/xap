package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;

// TODO implement type encoder/decoder
public class TypeInterval extends PgType {
    public static final PgType INSTANCE = new TypeInterval();

    public TypeInterval() {
        super(PgTypeDescriptor.INTERVAL);
    }
}
