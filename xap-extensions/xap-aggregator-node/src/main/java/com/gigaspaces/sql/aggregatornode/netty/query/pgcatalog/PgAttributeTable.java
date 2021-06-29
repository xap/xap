package com.gigaspaces.sql.aggregatornode.netty.query.pgcatalog;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.sql.aggregatornode.netty.utils.PgType;
import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import com.j_spaces.jdbc.SQLUtil;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

public class PgAttributeTable extends PgCatalogTable {
    private final IdGen idGen;
    private PgCatalogSchema parent;

    public PgAttributeTable(IJSpace space, IdGen idGen) {
        super(space, "pg_attribute");
        this.idGen = idGen;
    }

    @Override
    protected void init(PgCatalogSchema parent) {
        this.parent = parent;

        columns.add(new PgTableColumn("attrelid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("attname", TypeUtils.resolveType("name"), this));
        columns.add(new PgTableColumn("atttypid", TypeUtils.resolveType("oid"), this));
        columns.add(new PgTableColumn("attstattarget", TypeUtils.resolveType("int4"), this));
        columns.add(new PgTableColumn("attlen", TypeUtils.resolveType("int2"), this));
        columns.add(new PgTableColumn("attnum", TypeUtils.resolveType("int2"), this));
        columns.add(new PgTableColumn("attndims", TypeUtils.resolveType("int4"), this));
        columns.add(new PgTableColumn("attcacheoff", TypeUtils.resolveType("int4"), this));
        columns.add(new PgTableColumn("atttypmod", TypeUtils.resolveType("int4"), this));
        columns.add(new PgTableColumn("attbyval", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("attstorage", TypeUtils.resolveType("char"), this));
        columns.add(new PgTableColumn("attalign", TypeUtils.resolveType("char"), this));
        columns.add(new PgTableColumn("attnotnull", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("atthasdef", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("attisdropped", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("attislocal", TypeUtils.resolveType("bool"), this));
        columns.add(new PgTableColumn("attinhcount", TypeUtils.resolveType("int4"), this));

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
            short idx = 0;
            for (PropertyInfo property : typeDesc.getProperties()) {
                SqlTypeName sqlTypeName = mapToSqlType(property.getType());
                PgType pgType = TypeUtils.fromInternal(sqlTypeName);

                result.add(new TableRow(queryColumns, 
                    oid, // attrelid
                    property.getName(), // attname
                    pgType.getId(), // atttypid
                    0, // attstattarget
                    (short)pgType.getLength(), // attlen
                    ++idx,// attnum
                    (pgType.getElementType() != 0 ? 1 : 0), // attndims
                    -1, // attcacheoff
                    -1, // atttypmod
                    null, // attbyval
                    'p', // attstorage
                    'c', // attalign
                    false, // attnotnull
                    false, // atthasdef
                    false, // attisdropped
                    false, // attislocal
                    0// attinhcount
                ));
            }
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

    private static SqlTypeName mapToSqlType(Class<?> clazz) {
        if (clazz == Integer.class) {
            return SqlTypeName.INTEGER;
        } else if (clazz == Long.class) {
            return SqlTypeName.BIGINT;
        } else if (clazz == String.class) {
            return SqlTypeName.VARCHAR;
        } else if (clazz == java.util.Date.class) {
            return SqlTypeName.DATE;
        } else if (clazz == java.sql.Time.class) {
            return SqlTypeName.TIME;
        } else if (clazz == java.sql.Timestamp.class) {
            return SqlTypeName.TIMESTAMP;
        }


        throw new UnsupportedOperationException("Unsupported type: " + clazz);
    }
}
