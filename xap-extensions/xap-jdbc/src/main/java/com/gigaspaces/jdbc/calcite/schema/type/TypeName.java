package com.gigaspaces.jdbc.calcite.schema.type;

public class TypeName extends PgType {
    public static final PgType INSTANCE = new TypeName();

    public TypeName() {
        super(19, "name", 63, 1003, 0);
    }

}
