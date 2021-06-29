package com.gigaspaces.jdbc.calcite.schema.type;

// TODO implement type encoder/decoder
public class TypeTimeTZ extends PgType {
    public static final PgType INSTANCE = new TypeTimeTZ();

    public TypeTimeTZ() {
        super(1266, "timetz", 12, 1270, 0);
    }
}
