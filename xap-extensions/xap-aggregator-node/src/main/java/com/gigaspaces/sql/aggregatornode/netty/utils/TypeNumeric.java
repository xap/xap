package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;

// TODO implement type encoder/decoder
public class TypeNumeric extends PgType {
    public static final PgType INSTANCE = new TypeNumeric();

    public TypeNumeric() {
        super(PgTypeDescriptor.NUMERIC);
    }
}
