package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;

// TODO implement type encoder/decoder
public class TypeTimeTZ extends PgType {
    public static final PgType INSTANCE = new TypeTimeTZ();

    public TypeTimeTZ() {
        super(PgTypeDescriptor.TIME_WITH_TIME_ZONE);
    }
}
