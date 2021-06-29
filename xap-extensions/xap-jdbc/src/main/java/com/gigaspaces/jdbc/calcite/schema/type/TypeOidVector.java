package com.gigaspaces.jdbc.calcite.schema.type;

public class TypeOidVector extends PgType {
    public static final PgType INSTANCE = new TypeOidVector();

    public TypeOidVector() {
        super(30, "oidvector", -1, 0, TypeUtils.PG_TYPE_OID.id);
    }

}
