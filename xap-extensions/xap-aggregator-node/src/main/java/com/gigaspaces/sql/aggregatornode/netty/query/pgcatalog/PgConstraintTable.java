package com.gigaspaces.sql.aggregatornode.netty.query.pgcatalog;

import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;
import com.j_spaces.core.IJSpace;

public class PgConstraintTable extends PgCatalogTable {
    public PgConstraintTable(IJSpace space) {
        super(space, "pg_constraint");
    }

    @Override
    protected void init(PgCatalogSchema parent) {
        columns.add(new PgTableColumn("oid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("conname", TypeUtils.resolveType("name"), this));
        columns.add(new PgTableColumn("connamespace", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("contype", TypeUtils.resolveType("char"), this));
        columns.add(new PgTableColumn("condeferrable", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("condeferred", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("convalidated", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("conrelid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("contypid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("conindid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("conparentid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("confrelid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("confupdtype", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("confdeltype", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("confmatchtype", TypeUtils.resolveType("char"), this));
        columns.add(new PgTableColumn("conislocal", TypeUtils.resolveType("char"), this));
        columns.add(new PgTableColumn("coninhcount", TypeUtils.resolveType("char"), this));
        columns.add(new PgTableColumn("connoinherit", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("conkey", TypeUtils.resolveType("int2_array"), this));
        columns.add(new PgTableColumn("confkey", TypeUtils.resolveType("int2_array"), this));
        columns.add(new PgTableColumn("conpfeqop", TypeUtils.resolveType("oid_array"), this));
        columns.add(new PgTableColumn("conppeqop", TypeUtils.resolveType("oid_array"), this));
        columns.add(new PgTableColumn("conffeqop", TypeUtils.resolveType("oid_array"), this));
        columns.add(new PgTableColumn("conexclop", TypeUtils.resolveType("oid_array"), this));
        columns.add(new PgTableColumn("conbin", TypeUtils.resolveType("pg_node_tree"), this));

        super.init(parent);
    }
}
