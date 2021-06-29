package com.gigaspaces.sql.aggregatornode.netty.query.pgcatalog;

import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;
import com.j_spaces.core.IJSpace;

public class PgIndexTable extends PgCatalogTable {
    public PgIndexTable(IJSpace space) {
        super(space, "pg_index");
    }

    @Override
    protected void init(PgCatalogSchema parent) {
        columns.add(new PgTableColumn("oid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("indexrelid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("indrelid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("indnatts", TypeUtils.resolveType("int2"), this));
        columns.add(new PgTableColumn("indnkeyatts", TypeUtils.resolveType("int2"), this));
        columns.add(new PgTableColumn("indisunique", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("indisprimary", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("indisexclusion", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("indimmediate", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("indisclustered", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("indisvalid", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("indcheckxmin", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("indisready", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("indislive", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("indisreplident", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("indkey", TypeUtils.resolveType("int2vector"), this));
        columns.add(new PgTableColumn("indcollation", TypeUtils.resolveType("oidvector"), this));
        columns.add(new PgTableColumn("indclass", TypeUtils.resolveType("oidvector"), this));
        columns.add(new PgTableColumn("indoption", TypeUtils.resolveType("int2vector"), this));
        columns.add(new PgTableColumn("indexprs", TypeUtils.resolveType("pg_node_tree"), this));
        columns.add(new PgTableColumn("indpred", TypeUtils.resolveType("pg_node_tree"), this));

        super.init(parent);
    }
}
