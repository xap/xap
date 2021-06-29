package com.gigaspaces.jdbc.calcite.schema.type;

public class TypeOid extends PgType {
    public static final PgType INSTANCE = new TypeOid();

    public TypeOid() {
        super(26, "oid", 4, 1028, 0);
    }

}
