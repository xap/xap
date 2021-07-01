package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;

// TODO implement type encoder/decoder
public class TypeDate extends PgType {
    public static final PgType INSTANCE = new TypeDate();

    public TypeDate() {
        super(PgTypeDescriptor.DATE);
    }
}
