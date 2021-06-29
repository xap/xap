package com.gigaspaces.jdbc.calcite.schema.type;

public class TypeFloat4 extends PgType {
    public static final PgType INSTANCE = new TypeFloat4();

    public TypeFloat4() {
        super(700, "float4", 4, 1021, 0);
    }

}
