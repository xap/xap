package com.gigaspaces.jdbc.calcite.schema.type;

public class TypeBool extends PgType {
    public static final PgType INSTANCE = new TypeBool();

    public TypeBool() {
        super(16, "bool", 1, 1000, 0);
    }

}
