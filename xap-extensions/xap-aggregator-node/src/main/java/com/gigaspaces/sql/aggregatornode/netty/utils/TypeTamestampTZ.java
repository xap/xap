package com.gigaspaces.sql.aggregatornode.netty.utils;

// TODO implement type encoder/decoder
public class TypeTamestampTZ extends PgType {
    public static final PgType INSTANCE = new TypeTamestampTZ();

    public TypeTamestampTZ() {
        super(1184, "timestamptz", 8, 1185, 0);
    }
}
