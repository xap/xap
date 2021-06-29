package com.gigaspaces.sql.aggregatornode.netty.query.pgcatalog;

import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;
import com.j_spaces.core.IJSpace;

public class PgDatabaseTable extends PgCatalogTable {
    public PgDatabaseTable(IJSpace space) {
        super(space, "pg_database");
    }

    @Override
    protected void init(PgCatalogSchema parent) {
        columns.add(new PgTableColumn("oid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("datname", TypeUtils.resolveType("name"), this));
        columns.add(new PgTableColumn("datdba", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("encoding", TypeUtils.resolveType("int4"), this));
        columns.add(new PgTableColumn("datcollate", TypeUtils.resolveType("name"), this));
        columns.add(new PgTableColumn("datctype", TypeUtils.resolveType("name"), this));
        columns.add(new PgTableColumn("datistemplate", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("datallowconn", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("datconnlimit", TypeUtils.resolveType("int4"), this));
        columns.add(new PgTableColumn("datlastsysoid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("datfrozenxid", TypeUtils.resolveType("xid"), this));
        columns.add(new PgTableColumn("datminmxid", TypeUtils.resolveType("xid"), this));
        columns.add(new PgTableColumn("dattablespace", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("datacl", TypeUtils.resolveType("aclitem_array"), this));

        super.init(parent);
    }
}
