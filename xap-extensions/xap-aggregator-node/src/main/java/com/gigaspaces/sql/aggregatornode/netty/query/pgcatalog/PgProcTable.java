package com.gigaspaces.sql.aggregatornode.netty.query.pgcatalog;

import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;
import com.j_spaces.core.IJSpace;

public class PgProcTable extends PgCatalogTable {
    public PgProcTable(IJSpace space) {
        super(space, "pg_proc");
    }

    @Override
    protected void init(PgCatalogSchema parent) {
        columns.add(new PgTableColumn("oid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("proname", TypeUtils.resolveType("name"), this));
        columns.add(new PgTableColumn("pronamespace", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("proowner", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("prolang", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("procost", TypeUtils.resolveType("float4"), this));
        columns.add(new PgTableColumn("prorows", TypeUtils.resolveType("float4"), this));
        columns.add(new PgTableColumn("provariadic", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("prosupport", TypeUtils.resolveType("regproc"), this));
        columns.add(new PgTableColumn("prokind", TypeUtils.resolveType("char"), this));
        columns.add(new PgTableColumn("prosecdef", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("proleakproof", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("proisstrict", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("proretset", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("provolatile", TypeUtils.resolveType("char"), this));
        columns.add(new PgTableColumn("proparallel", TypeUtils.resolveType("char"), this));
        columns.add(new PgTableColumn("pronargs", TypeUtils.resolveType("int2"), this));
        columns.add(new PgTableColumn("pronargdefaults", TypeUtils.resolveType("int2"), this));
        columns.add(new PgTableColumn("prorettype", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("proargtypes", TypeUtils.resolveType("oidvector"), this));
        columns.add(new PgTableColumn("proallargtypes", TypeUtils.resolveType("oid_array"), this));
        columns.add(new PgTableColumn("proargmodes", TypeUtils.resolveType("char_array"), this));
        columns.add(new PgTableColumn("proargnames", TypeUtils.resolveType("text_array"), this));
        columns.add(new PgTableColumn("proargdefaults", TypeUtils.resolveType("pg_node_tree"), this));
        columns.add(new PgTableColumn("protrftypes", TypeUtils.resolveType("oid_array"), this));
        columns.add(new PgTableColumn("prosrc", TypeUtils.resolveType("text"), this));
        columns.add(new PgTableColumn("probin", TypeUtils.resolveType("text"), this));
        columns.add(new PgTableColumn("proconfig", TypeUtils.resolveType("text_array"), this));
        columns.add(new PgTableColumn("proacl", TypeUtils.resolveType("aclitem_array"), this));

        super.init(parent);
    }
}
