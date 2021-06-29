package com.gigaspaces.sql.aggregatornode.netty.query.pgcatalog;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import com.j_spaces.jdbc.SQLUtil;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.List;

public class PgClassTable extends PgCatalogTable {
    private final IdGen idGen;

    public PgClassTable(IJSpace space, IdGen idGen) {
        super(space, "pg_class");
        this.idGen = idGen;
    }

    @Override
    protected void init(PgCatalogSchema parent) {
        columns.add(new PgTableColumn("oid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("relname", TypeUtils.resolveType("name"), this));
        columns.add(new PgTableColumn("relnamespace", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("reltype", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("relowner", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("relam", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("relfilenode", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("reltablespace", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("relpages", TypeUtils.resolveType("int4"), this));
        columns.add(new PgTableColumn("reltuples", TypeUtils.resolveType("float4"), this));
        columns.add(new PgTableColumn("reltoastrelid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("relhasindex", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("relisshared", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("relkind", TypeUtils.resolveType("char"), this));
        columns.add(new PgTableColumn("relnatts", TypeUtils.resolveType("int2"), this));
        columns.add(new PgTableColumn("relchecks", TypeUtils.resolveType("int2"), this));
        columns.add(new PgTableColumn("reltriggers", TypeUtils.resolveType("int2"), this));
        columns.add(new PgTableColumn("relhasrules", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("relhastriggers", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("relhassubclass", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("relacl", TypeUtils.resolveType("aclitem_array"), this));
        columns.add(new PgTableColumn("reloptions", TypeUtils.resolveType("text_array"), this));

        super.init(parent);
    }

    @Override
    protected QueryResult executeReadInternal(QueryExecutionConfig config) throws SQLException {
        QueryResult result = super.executeReadInternal(config);
        QueryColumn[] queryColumns = result.getQueryColumns().toArray(new QueryColumn[0]);

        for (String name : getSpaceTables(space)) {
            String fqn = "public." + name;
            int oid = idGen.oid(fqn);
            ITypeDesc typeDesc = SQLUtil.checkTableExistence(name, space);
            result.add(new TableRow(queryColumns,
                oid,//    oid
                name,//    relname
                PgNamespaceTable.NS_PUBLIC_OID,//    relnamespace
                0,//    reltype
                0,//    relowner
                0,//    relam
                0,//    relfilenode
                0,//    reltablespace
                0,//    relpages
                100.0f,//    reltuples
                0,//    reltoastrelid
                false,//    relhasindex
                false,//    relisshared
                'r',//    relkind
                (short) typeDesc.getProperties().length, //    relnatts
                (short) 0,//    relchecks
                (short) 0,//    reltriggers
                false, //    relhasrules
                false, //    relhastriggers
                false, //    relhassubclass
                null, // relacl
                null//    reloptions
            ));
        }



        return result;
    }

    private List<String> getSpaceTables(IJSpace space) throws SQLException {
        try {
            return ((IRemoteJSpaceAdmin) space.getAdmin()).getRuntimeInfo().m_ClassNames;
        } catch (RemoteException e) {
            throw new SQLException("Failed to get runtime info from space", e);
        }
    }
}
