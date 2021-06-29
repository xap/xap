package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.calcite.schema.GSSchemaTable;
import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.result.*;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.EqualValueRange;
import com.j_spaces.jdbc.builder.range.Range;
import com.j_spaces.jdbc.builder.range.SegmentRange;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class SchemaTableContainer extends TempTableContainer {
    private final GSSchemaTable table;
    private final IJSpace space;

    public SchemaTableContainer(GSSchemaTable table, String alias, IJSpace space) {
        super(alias);
        this.table = table;
        this.tableColumns.addAll(Arrays.stream(table.getSchemas()).map(x -> new ConcreteColumn(x.getPropertyName(), x.getJavaType(), null, true, this, -1)).collect(Collectors.toList()));
        this.space = space;

        allColumnNamesSorted.addAll(tableColumns.stream().map(IQueryColumn::getAlias).collect(Collectors.toList()));
    }

    @Override
    public QueryResult executeRead(QueryExecutionConfig config) throws SQLException {
        if (tableResult != null) return tableResult;
        tableResult = table.execute(this, space, tableColumns);
        if (queryTemplatePacket != null) {
            tableResult.filter(x -> queryTemplatePacket.eval(x));
        }
        return tableResult = new TempQueryResult(this);
    }

    @Override
    public IQueryColumn addQueryColumn(String columnName, String columnAlias, boolean isVisible, int columnOrdinal) {
        IQueryColumn queryColumn = tableColumns.stream()
                .filter(qc -> qc.getName().equalsIgnoreCase(columnName))
                .findFirst()
                .orElseThrow(() -> new ColumnNotFoundException("Could not find column with name [" + columnName + "]"));
        if (isVisible) visibleColumns.add(queryColumn);
        else invisibleColumns.add(queryColumn);
        return queryColumn;
    }


    @Override
    public String getTableNameOrAlias() {
        return alias == null ? table.getName() : alias;
    }

    @Override
    public Object getColumnValue(String columnName, Object value) throws SQLException {
        return value;
    }

}
