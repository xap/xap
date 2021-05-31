package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.calcite.schema.GSSchemaTable;
import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.result.TempTableQTP;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.EqualValueRange;
import com.j_spaces.jdbc.builder.range.Range;
import com.j_spaces.jdbc.builder.range.SegmentRange;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SchemaTableContainer extends TableContainer {
    private final GSSchemaTable table;
    private TableContainer joinedTable;
    private final List<QueryColumn> visibleColumns = new ArrayList<>();
    private final List<QueryColumn> tableColumns;
    private TempTableQTP queryTemplatePacket;

    public SchemaTableContainer(GSSchemaTable table) {
        this.table = table;
        this.tableColumns = Arrays.stream(table.getSchemas()).map(x -> new QueryColumn(x.getPropertyName(), null, true, this)).collect(Collectors.toList());
    }

    @Override
    public QueryResult executeRead(QueryExecutionConfig config) {
        List<QueryColumn> queryColumns = getVisibleColumns();
        QueryResult queryResult = new QueryResult(queryColumns);
        QueryColumn[] arr = queryColumns.toArray(new QueryColumn[0]);
        queryResult.add(new TableRow(arr, new Object[]{"aa1", "bb1", "cc1"}));
        queryResult.add(new TableRow(arr, new Object[]{"aa2", "bb2", "cc2"}));

        if (queryTemplatePacket != null)
            queryResult.filter(x -> queryTemplatePacket.eval(x));
        return queryResult;
    }

    @Override
    public QueryColumn addQueryColumn(String columnName, String alias, boolean visible) {
        QueryColumn queryColumn = tableColumns.stream().filter(qc -> qc.getName().equalsIgnoreCase(columnName)).findFirst().orElseThrow(() -> new ColumnNotFoundException("Could not find column with name [" + columnName + "]"));
        if (visible) visibleColumns.add(queryColumn);
        return queryColumn;
    }

    @Override
    public List<QueryColumn> getVisibleColumns() {
        return visibleColumns;
    }

    @Override
    public List<String> getAllColumnNames() {
        return tableColumns.stream().map(QueryColumn::getName).collect(Collectors.toList());
    }

    @Override
    public String getTableNameOrAlias() {
        return table.getName();
    }

    @Override
    public void setLimit(Integer value) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public QueryTemplatePacket createQueryTemplatePacketWithRange(Range range) {
        addQueryColumn(range.getPath(), null, false);
        if (range instanceof EqualValueRange) {
            return new TempTableQTP((EqualValueRange) range);
        } else if (range instanceof SegmentRange) {
            return new TempTableQTP((SegmentRange) range);
        } else {
            throw new UnsupportedOperationException("Range: " + range);
        }
    }

    @Override
    public void setQueryTemplatePacket(QueryTemplatePacket queryTemplatePacket) {
        this.queryTemplatePacket = ((TempTableQTP) queryTemplatePacket);
    }

    @Override
    public Object getColumnValue(String columnName, Object value) throws SQLException {
        return value;
    }

    @Override
    public TableContainer getJoinedTable() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void setJoinedTable(TableContainer joinedTable) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public QueryResult getQueryResult() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void setJoined(boolean joined) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public boolean isJoined() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public boolean hasColumn(String columnName) {
        return getAllColumnNames().contains(columnName);
    }

    @Override
    public JoinInfo getJoinInfo() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void setJoinInfo(JoinInfo joinInfo) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public boolean checkJoinCondition() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

}
