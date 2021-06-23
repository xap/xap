package com.gigaspaces.sql.aggregatornode.netty.utils;

public class TypeAny extends PgType {
    public static final PgType INSTANCE = new TypeAny();

    public TypeAny() {
        super(2276, "any", 4, 0, 0);
    }
}
