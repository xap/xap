package com.gigaspaces.jdbc.calcite.schema.type;

public class TypeText extends PgType {
    public static final PgType INSTANCE = new TypeText();

    public TypeText() {
        super(25, "text", -1, 1009, 0);
    }

}
