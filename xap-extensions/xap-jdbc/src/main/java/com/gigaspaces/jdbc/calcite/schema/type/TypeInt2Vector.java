package com.gigaspaces.jdbc.calcite.schema.type;

public class TypeInt2Vector extends PgType {
    public static final PgType INSTANCE = new TypeInt2Vector();

    public TypeInt2Vector() {
        super(22, "int2vector", -1, 1006, TypeUtils.PG_TYPE_INT2.id);
    }

}
