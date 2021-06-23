package com.gigaspaces.sql.aggregatornode.netty.utils;

// TODO implement type encoder/decoder
public class TypeCursor extends PgType {
    public static final PgType INSTANCE = new TypeCursor();

    public TypeCursor() {
        super(1790, "refcursor", -1, 2201, 0);
    }
}
