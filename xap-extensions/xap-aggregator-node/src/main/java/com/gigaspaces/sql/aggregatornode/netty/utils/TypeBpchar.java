package com.gigaspaces.sql.aggregatornode.netty.utils;

// TODO implement type encoder/decoder
public class TypeBpchar extends PgType {
    public static final PgType INSTANCE = new TypeBpchar();

    public TypeBpchar() {
        super(1042, "bpchar", -1, 1014, 0);
    }
}
