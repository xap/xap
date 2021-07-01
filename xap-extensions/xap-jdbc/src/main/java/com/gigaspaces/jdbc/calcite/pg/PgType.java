package com.gigaspaces.jdbc.calcite.pg;

public class PgType {
    public static final PgType BOOL = new PgType(16, "bool", 1, 1000, 0);
    public static final PgType BPCHAR = new PgType(1042, "bpchar", -1, 1014, 0);
    public static final PgType BYTEA = new PgType(17, "bytea", -1, 1001, 0);
    public static final PgType CHAR = new PgType(18, "char", 1, 1002, 0);
    public static final PgType CURSOR = new PgType(1790, "refcursor", -1, 2201, 0);
    public static final PgType DATE = new PgType(1082, "date", 4, 1182, 0);
    public static final PgType FLOAT4 = new PgType(700, "float4", 4, 1021, 0);
    public static final PgType FLOAT8 = new PgType(701, "float8", 8, 1022, 0);
    public static final PgType INT2 = new PgType(21, "int2", 2, 1005, 0);
    public static final PgType INT2VECTOR = new PgType(22, "int2vector", -1, 1006, INT2.id);
    public static final PgType INT4 = new PgType(23, "int4", 4, 1007, 0);
    public static final PgType INT8 = new PgType(20, "int8", 8, 1016, 0);
    public static final PgType INTERVAL = new PgType(1186, "interval", 16, 1187, 0);
    public static final PgType NAME = new PgType(19, "name", 63, 1003, 0);
    public static final PgType NODE_TREE = new PgType(194, "pg_node_tree", -1, 0, 0);
    public static final PgType NUMERIC = new PgType(1700, "numeric", -1, 1231, 0);
    public static final PgType TEXT = new PgType(25, "text", -1, 1009, 0);
    public static final PgType OID = new PgType(26, "oid", 4, 1028, 0);
    public static final PgType OID_VECTOR = new PgType(30, "oidvector", -1, 0, OID.id);
    public static final PgType REGPROC = new PgType(24, "regproc", 4, 1008, 0);
    public static final PgType TIME = new PgType(1083, "time", 8, 1183, 0);
    public static final PgType TIME_WITH_TIME_ZONE = new PgType(1266, "timetz", 12, 1270, 0);
    public static final PgType TIMESTAMP = new PgType(1114, "timestamp", 8, 1115, 0);
    public static final PgType TIMESTAMP_WITH_TIME_ZONE = new PgType(1184, "timestamptz", 8, 1185, 0);
    public static final PgType VARCHAR = new PgType(1043, "varchar", -1, 1015, 0);
    public static final PgType UNKNOWN = new PgType(705, "unknown", -2, 0, 0);

    protected final int id;
    protected final String name;
    protected final int length;
    protected final int arrayType;
    protected final int elementType;

    public PgType(int id, String name, int length, int arrayType, int elementType) {
        this.id = id;
        this.name = name;
        this.length = length;
        this.arrayType = arrayType;
        this.elementType = elementType;
    }

    public final int getId() {
        return id;
    }

    public final String getName() {
        return name;
    }

    public final int getLength() {
        return length;
    }

    public int getElementType() {
        return elementType;
    }

    public PgType asArray() {
        return new PgType(arrayType, name + "_array", -1, 0, id);
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PgType pgType = (PgType) o;

        return id == pgType.id;
    }

    @Override
    public final int hashCode() {
        return id;
    }

}
