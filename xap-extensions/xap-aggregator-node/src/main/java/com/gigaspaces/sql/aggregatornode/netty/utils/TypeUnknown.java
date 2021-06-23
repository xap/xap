package com.gigaspaces.sql.aggregatornode.netty.utils;

public class TypeUnknown extends PgType {
    public static final PgType INSTANCE = new TypeUnknown();

    public TypeUnknown() {
        super(705, "unknown", -2, 0, 0);
    }
}
