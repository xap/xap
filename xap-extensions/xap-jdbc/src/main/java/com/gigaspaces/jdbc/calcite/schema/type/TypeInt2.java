package com.gigaspaces.jdbc.calcite.schema.type;

public class TypeInt2 extends PgType {
    public static final PgType INSTANCE = new TypeInt2();

    public TypeInt2() {
        super(21, "int2", 2, 1005, 0);
    }
}
