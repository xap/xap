package com.gigaspaces.sql.aggregatornode.netty.utils;

// TODO implement type encoder/decoder
public class TypeTimestamp extends PgType {
    public static final PgType INSTANCE = new TypeTimestamp();

    public TypeTimestamp() {
        super(1114, "timestamp", 8, 1115, 0);
    }
}
