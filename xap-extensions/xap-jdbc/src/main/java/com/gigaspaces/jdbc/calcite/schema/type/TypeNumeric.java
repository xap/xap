package com.gigaspaces.jdbc.calcite.schema.type;

// TODO implement type encoder/decoder
public class TypeNumeric extends PgType {
    public static final PgType INSTANCE = new TypeNumeric();

    public TypeNumeric() {
        super(1700, "numeric", -1, 1231, 0);
    }
}
