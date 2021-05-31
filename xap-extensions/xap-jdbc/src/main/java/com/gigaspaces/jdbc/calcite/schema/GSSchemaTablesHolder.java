package com.gigaspaces.jdbc.calcite.schema;

import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.Map;

import static com.gigaspaces.jdbc.calcite.schema.GSSchemaTable.SchemaProperty;

public class GSSchemaTablesHolder {
    private static final Map<String, GSSchemaTable> schemaTables = new HashMap<>();

    static {
        add(new GSSchemaTable("pg_tables",
                SchemaProperty.of("schemaname", SqlTypeName.VARCHAR),
                SchemaProperty.of("tablename", SqlTypeName.VARCHAR),
                SchemaProperty.of("tableowner", SqlTypeName.VARCHAR)));
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
