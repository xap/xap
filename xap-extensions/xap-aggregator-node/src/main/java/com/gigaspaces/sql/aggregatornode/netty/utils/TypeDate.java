package com.gigaspaces.sql.aggregatornode.netty.utils;

// TODO implement type encoder/decoder
public class TypeDate extends PgType {
    public static final PgType INSTANCE = new TypeDate();

    public TypeDate() {
        super(1082, "date", 4, 1182, 0);
    }
}
