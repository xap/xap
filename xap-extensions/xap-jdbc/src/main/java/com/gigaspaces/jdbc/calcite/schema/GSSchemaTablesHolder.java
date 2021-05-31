package com.gigaspaces.jdbc.calcite.schema;

import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.Map;

import static com.gigaspaces.jdbc.calcite.schema.GSSchemaTable.SchemaProperty;

public class GSSchemaTablesHolder {
    private static final Map<String, GSSchemaTable> schemaTables = new HashMap<>();

    static {
        for (PGSystemTable pgSystemTable : PGSystemTable.values()) {
            add(new GSSchemaTable(pgSystemTable));
        }
    }

    private static void add(GSSchemaTable gsSchemaTable) {
        schemaTables.put(gsSchemaTable.getName(), gsSchemaTable);
    }

    public static GSSchemaTable getTable(String name) {
        if (name.startsWith("pg_catalog."))
            name = name.substring(("pg_catalog.").length());
        return schemaTables.get(name);
    }
}
