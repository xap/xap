package com.gigaspaces.sql.aggregatornode.netty.query.pgcatalog;

import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;
import com.j_spaces.core.IJSpace;

public class PgTriggerTable extends PgCatalogTable {
    public PgTriggerTable(IJSpace space) {
        super(space, "pg_trigger");
    }

    @Override
    protected void init(PgCatalogSchema parent) {
        columns.add(new PgTableColumn("oid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("tgrelid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("tgparentid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("tgname", TypeUtils.resolveType("name"), this));
        columns.add(new PgTableColumn("tgfoid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("tgtype", TypeUtils.resolveType("int2"), this));
        columns.add(new PgTableColumn("tgenabled", TypeUtils.resolveType("char"), this));
        columns.add(new PgTableColumn("tgisinternal", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("tgconstrrelid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("tgconstrindid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("tgconstraint", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("tgdeferrable", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("tginitdeferred", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("tgnargs", TypeUtils.resolveType("int2"), this));
        columns.add(new PgTableColumn("tgattr", TypeUtils.resolveType("int2vector"), this));
        columns.add(new PgTableColumn("tgargs", TypeUtils.resolveType("bytea"), this));
        columns.add(new PgTableColumn("tgqual", TypeUtils.resolveType("pg_node_tree"), this));
        columns.add(new PgTableColumn("tgoldtable", TypeUtils.resolveType("name"), this));
        columns.add(new PgTableColumn("tgnewtable", TypeUtils.resolveType("name"), this));

        super.init(parent);
    }
}
