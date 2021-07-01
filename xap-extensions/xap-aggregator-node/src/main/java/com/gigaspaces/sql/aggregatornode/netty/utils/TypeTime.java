package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;

// TODO implement type encoder/decoder
public class TypeTime extends PgType {
    public static final PgType INSTANCE = new TypeTime();

    public TypeTime() {
        super(PgTypeDescriptor.TIME);
    }
}
