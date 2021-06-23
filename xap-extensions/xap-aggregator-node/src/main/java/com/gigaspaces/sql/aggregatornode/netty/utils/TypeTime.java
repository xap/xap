package com.gigaspaces.sql.aggregatornode.netty.utils;

// TODO implement type encoder/decoder
public class TypeTime extends PgType {
    public static final PgType INSTANCE = new TypeTime();

    public TypeTime() {
        super(1083, "time", 8, 1183, 0);
    }
}
