package com.gigaspaces.jdbc.calcite.schema.type;

// TODO implement type encoder/decoder
public class TypeRegproc extends PgType {
    public static final PgType INSTANCE = new TypeRegproc();

    public TypeRegproc() {
        super(24, "regproc", 4, 1008, 0);
    }

}
