package com.gigaspaces.sql.aggregatornode.netty.query.pgcatalog;

import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;
import com.j_spaces.core.IJSpace;

import java.sql.SQLException;

public class PgNamespaceTable extends PgCatalogTable {
    public static final int NS_PUBLIC_OID = 0;
    public static final int NS_PG_CATALOG_OID = -1000;

    public PgNamespaceTable(IJSpace space) {
        super(space, "pg_namespace");
    }

    @Override
    protected void init(PgCatalogSchema parent) {
        columns.add(new PgTableColumn("oid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("nspname", TypeUtils.resolveType("name"), this));
        columns.add(new PgTableColumn("nspowner", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("nspacl", TypeUtils.resolveType("aclitem_array"), this));

        super.init(parent);
    }

    @Override
    protected QueryResult executeReadInternal(QueryExecutionConfig config) throws SQLException {
        QueryResult result = super.executeReadInternal(config);
        QueryColumn[] queryColumns = result.getQueryColumns().toArray(new QueryColumn[0]);
        result.add(new TableRow(queryColumns, NS_PUBLIC_OID, "PUBLIC", 0, null));
        result.add(new TableRow(queryColumns, NS_PG_CATALOG_OID, "PG_CATALOG", 0, null));
        return result;
    }
}
