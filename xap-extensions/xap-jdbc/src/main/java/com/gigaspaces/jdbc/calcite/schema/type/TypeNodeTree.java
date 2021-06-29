package com.gigaspaces.jdbc.calcite.schema.type;

public class TypeNodeTree extends PgType {
    public static final PgType INSTANCE = new TypeNodeTree();

    public TypeNodeTree() {
        super(194, "pg_node_tree", -1, 0, 0);
    }

}
