package com.gigaspaces.jdbc.calcite.schema.type;

public class TypeInt8 extends PgType {
    public static final PgType INSTANCE = new TypeInt8();

    public TypeInt8() {
        super(20, "int8", 8, 1016, 0);
    }

}
