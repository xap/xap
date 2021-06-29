package com.gigaspaces.sql.aggregatornode.netty.query.pgcatalog;

import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.sql.aggregatornode.netty.utils.Constants;
import com.gigaspaces.sql.aggregatornode.netty.utils.PgType;
import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;
import com.j_spaces.core.IJSpace;

import java.sql.SQLException;

public class PgTypeTable extends PgCatalogTable {
    public PgTypeTable(IJSpace space) {
        super(space, "pg_type");
    }

    @Override
    protected void init(PgCatalogSchema parent) {
        columns.add(new PgTableColumn("oid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("typname", TypeUtils.resolveType("name"), this));
        columns.add(new PgTableColumn("typnamespace", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("typowner", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("typlen", TypeUtils.resolveType("int2"), this));
        columns.add(new PgTableColumn("typbyval", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("typtype", TypeUtils.resolveType("char"), this));
        columns.add(new PgTableColumn("typisdefined", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("typdelim", TypeUtils.resolveType("char"), this));
        columns.add(new PgTableColumn("typrelid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("typelem", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("typinput", TypeUtils.resolveType("regproc"), this));
        columns.add(new PgTableColumn("typoutput", TypeUtils.resolveType("regproc"), this));
        columns.add(new PgTableColumn("typreceive", TypeUtils.resolveType("regproc"), this));
        columns.add(new PgTableColumn("typsend", TypeUtils.resolveType("regproc"), this));
        columns.add(new PgTableColumn("typanalyze", TypeUtils.resolveType("regproc"), this));
        columns.add(new PgTableColumn("typalign", TypeUtils.resolveType("char"), this));
        columns.add(new PgTableColumn("typstorage", TypeUtils.resolveType("char"), this));
        columns.add(new PgTableColumn("typnotnull", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("typbasetype", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("typtypmod", TypeUtils.resolveType("int4"), this));
        columns.add(new PgTableColumn("typndims", TypeUtils.resolveType("int4"), this));
        columns.add(new PgTableColumn("typdefaultbin", TypeUtils.resolveType("pg_node_tree"), this));
        columns.add(new PgTableColumn("typdefault", TypeUtils.resolveType("text"), this));

        super.init(parent);
    }

    @Override
    protected QueryResult executeReadInternal(QueryExecutionConfig config) throws SQLException {
        QueryResult result = super.executeReadInternal(config);

        QueryColumn[] queryColumns = result.getQueryColumns().toArray(new QueryColumn[0]);

        for (PgType type : TypeUtils.types()) {
            TableRow row = new TableRow(queryColumns,
                    type.getId(), //            oid
                    type.getName(), //            typname
                    PgNamespaceTable.NS_PG_CATALOG_OID, //            typnamespace
                    0,//            typowner
                    (short) type.getLength(),//            typlen
                    null,//            typbyval
                    'b',//            typtype
                    true, //            typisdefined
                    Constants.DELIMITER, //            typdelim
                    0, //            typrelid
                    type.getElementType() , //            typelem
                    0, //            typinput
                    0, //            typoutput
                    0, //            typreceive
                    0, //            typsend
                    0, //            typanalyze
                    'c', //            typalign
                    'p', //            typstorage
                    false, //            typnotnull
                    0, //            typbasetype
                    -1, //            typtypmod
                    (type.getElementType() != 0 ? 1 : 0), //            typndims
                    null, //            typdefaultbin
                    null//            typdefault
            );

            result.add(row);
        }
        return result;
    }
}
