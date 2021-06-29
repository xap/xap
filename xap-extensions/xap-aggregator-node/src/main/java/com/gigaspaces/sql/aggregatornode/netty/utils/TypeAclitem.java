package com.gigaspaces.sql.aggregatornode.netty.utils;

public class TypeAclitem extends PgType {
    public static final PgType INSTANCE = new TypeAclitem();

    public TypeAclitem() {
        super(1033, "aclitem", 12, 1034, 0);
    }
}
