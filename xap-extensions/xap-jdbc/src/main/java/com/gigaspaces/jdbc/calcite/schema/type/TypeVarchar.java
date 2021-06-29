package com.gigaspaces.jdbc.calcite.schema.type;

public class TypeVarchar extends PgType {
    public static final PgType INSTANCE = new TypeVarchar();

    public TypeVarchar() {
        super(1043, "varchar", -1, 1015, 0);
    }

}
