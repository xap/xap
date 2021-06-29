package com.gigaspaces.jdbc.calcite.schema.type;

public class TypeInt4 extends PgType {
    public static final PgType INSTANCE = new TypeInt4();

    public TypeInt4() {
        super(23, "int4", 4, 1007, 0);
    }

}
