package com.gigaspaces.jdbc.calcite.schema.type;

public class TypeFloat8 extends PgType {
    public static final PgType INSTANCE = new TypeFloat8();

    public TypeFloat8() {
        super(701, "float8", 8, 1022, 0);
    }

}
