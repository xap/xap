package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;

// TODO implement type encoder/decoder
public class TypeTimestamp extends PgType {
    public static final PgType INSTANCE = new TypeTimestamp();

    public TypeTimestamp() {
        super(PgTypeDescriptor.TIMESTAMP);
    }
}
