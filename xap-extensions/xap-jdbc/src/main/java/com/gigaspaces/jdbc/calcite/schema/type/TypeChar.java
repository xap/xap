package com.gigaspaces.jdbc.calcite.schema.type;

public class TypeChar extends PgType {
    public static final PgType INSTANCE = new TypeChar();

    public TypeChar() {
        super(18, "char", 1, 1002, 0);
    }

}
