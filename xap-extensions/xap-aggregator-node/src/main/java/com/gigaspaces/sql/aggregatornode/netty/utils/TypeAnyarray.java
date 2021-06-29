package com.gigaspaces.sql.aggregatornode.netty.utils;

public class TypeAnyarray extends PgTypeArray<Object> {
    public static final PgType INSTANCE = new TypeAnyarray();

    public TypeAnyarray() {
        super(2277, "anyarray", -1, 0, TypeUtils.PG_TYPE_ANY.id);
    }
}
