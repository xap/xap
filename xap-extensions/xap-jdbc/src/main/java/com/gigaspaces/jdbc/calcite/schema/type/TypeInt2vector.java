package com.gigaspaces.jdbc.calcite.schema.type;

public class TypeInt2vector extends PgType {
    public static final PgType INSTANCE = new TypeInt2vector();

    public TypeInt2vector() {
        super(22, "int2vector", -1, 1006, TypeUtils.PG_TYPE_INT2.id);
    }

}
