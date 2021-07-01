package com.gigaspaces.jdbc.calcite.pg;

import com.gigaspaces.jdbc.calcite.GSAbstractSchema;
import org.apache.calcite.schema.Table;

import java.util.Set;

public class PgCalciteSchema extends GSAbstractSchema {

    public static final String NAME = "pg_catalog";
    public static final PgCalciteSchema INSTANCE = new PgCalciteSchema();

    private PgCalciteSchema() {
        // No-op.
    }

    @Override
    public Table getTable(String name) {
        return PgCalciteTable.getTable(name);
    }

    @Override
    public Set<String> getTableNames() {
        return PgCalciteTable.getTableNames();
    }
}
