package com.gigaspaces.sql.aggregatornode.netty.query.pgcatalog;

import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;
import com.j_spaces.core.IJSpace;

public class PgAmTable extends PgCatalogTable {
    public PgAmTable(IJSpace space) {
        super(space, "pg_am");
    }

    @Override
    protected void init(PgCatalogSchema parent) {
        columns.add(new PgTableColumn("oid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("amname", TypeUtils.resolveType("name"), this));
        columns.add(new PgTableColumn("amhandler", TypeUtils.resolveType("regproc"), this));
        columns.add(new PgTableColumn("amtype", TypeUtils.resolveType("char"), this));

        super.init(parent);
    }
}
