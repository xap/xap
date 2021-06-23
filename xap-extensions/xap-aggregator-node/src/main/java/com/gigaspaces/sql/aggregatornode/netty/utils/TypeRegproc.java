package com.gigaspaces.sql.aggregatornode.netty.utils;

// TODO implement type encoder/decoder
public class TypeRegproc extends PgType {
    public static final PgType INSTANCE = new TypeRegproc();

    public TypeRegproc() {
        super(24, "regproc", 4, 1008, 0);
    }
}
