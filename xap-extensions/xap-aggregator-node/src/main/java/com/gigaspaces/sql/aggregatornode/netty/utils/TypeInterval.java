package com.gigaspaces.sql.aggregatornode.netty.utils;

// TODO implement type encoder/decoder
public class TypeInterval extends PgType {
    public static final PgType INSTANCE = new TypeInterval();

    public TypeInterval() {
        super(1186, "interval", 16, 1187, 0);
    }
}
