package com.gigaspaces.sql.aggregatornode.netty.query.pgcatalog;

import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;

public class PgTableColumn extends QueryColumn {
    private final RelProtoDataType sqlType;

    public PgTableColumn(String name, RelProtoDataType sqlType, TableContainer tableContainer) {
        this(name, null, sqlType, true, tableContainer);
    }

    public PgTableColumn(String name, String alias, RelProtoDataType sqlType, boolean isVisible, TableContainer tableContainer) {
        super(name, alias, isVisible, tableContainer);
        this.sqlType = sqlType;
    }

    public RelDataType sqlType(RelDataTypeFactory typeFactory) {
        return sqlType.apply(typeFactory);
    }
}
