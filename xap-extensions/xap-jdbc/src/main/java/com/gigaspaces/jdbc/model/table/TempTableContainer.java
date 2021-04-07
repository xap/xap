package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;

import java.util.List;
import java.util.stream.Collectors;

public class TempTableContainer extends TableContainer {
    private final QueryResult tableResult;
    private final String alias;
    private TableContainer joinedTable;

    public TempTableContainer(QueryResult tableResult, String alias) {
        this.tableResult = tableResult;
        this.alias = alias;
    }

    @Override
    public QueryResult executeRead(QueryExecutionConfig config) {
        return tableResult;
    }

    @Override
    public QueryColumn addQueryColumn(String columnName, String alias, boolean visible) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public List<QueryColumn> getVisibleColumns() {
        return tableResult.getQueryColumns();
    }

    @Override
    public List<String> getAllColumnNames() {
        return tableResult.getQueryColumns().stream().map(QueryColumn::getName).collect(Collectors.toList());
    }

    @Override
    public String getTableNameOrAlias() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public TableContainer getJoinedTable() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void setJoinedTable(TableContainer joinedTable) {
        this.joinedTable = joinedTable;
    }

    @Override
    public QueryResult getQueryResult() {
        return tableResult;
    }

    @Override
    public void setLimit(Integer value) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public QueryTemplatePacket createQueryTemplatePacketWithRange(Range range) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void setQueryTemplatePackage(QueryTemplatePacket queryTemplatePacket) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void setJoined(boolean joined) {

    }

    @Override
    public boolean isJoined() {
        return false;
    }

    @Override
    public Object getColumnValue(String columnName, Object value) {
        throw new UnsupportedOperationException("Not supported yet!");
    }
}
