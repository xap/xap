package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;

// TODO implement type encoder/decoder
public class TypeCursor extends PgType {
    public static final PgType INSTANCE = new TypeCursor();

    public TypeCursor() {
        super(PgTypeDescriptor.CURSOR);
    }
}
