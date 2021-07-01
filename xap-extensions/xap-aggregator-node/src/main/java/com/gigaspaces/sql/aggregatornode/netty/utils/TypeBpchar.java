package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;

// TODO implement type encoder/decoder
public class TypeBpchar extends PgType {
    public static final PgType INSTANCE = new TypeBpchar();

    public TypeBpchar() {
        super(PgTypeDescriptor.BPCHAR);
    }
}
