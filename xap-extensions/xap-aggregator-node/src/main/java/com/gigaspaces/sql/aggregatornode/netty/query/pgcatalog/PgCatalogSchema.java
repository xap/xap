package com.gigaspaces.sql.aggregatornode.netty.query.pgcatalog;

import com.gigaspaces.jdbc.calcite.GSNamedSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PgCatalogSchema extends AbstractSchema implements GSNamedSchema {
    private final PgCatalogTable[] tables;

    public PgCatalogSchema(PgCatalogTable... tables) {
        this.tables = tables;

        for (PgCatalogTable table : tables) {
            table.init(this);
        }
    }

    public PgCatalogTable[] getTables() {
        return tables;
    }

    @Override
    public String getName() {
        return "pg_catalog";
    }

    @Override
    public boolean isDefault() {
        return false;
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return Arrays.stream(tables)
                .collect(Collectors.toMap(
                        PgCatalogTable::getName,
                        Function.identity(),
                        PgCatalogSchema::firstNotNull));
    }

    @SafeVarargs
    private static <T> T firstNotNull(T... values) {
        for (T value : values) {
            if (value != null)
                return value;
        }
        return null;
    }
}
