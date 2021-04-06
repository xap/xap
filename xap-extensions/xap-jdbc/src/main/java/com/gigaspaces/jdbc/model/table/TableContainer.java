package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;

import java.sql.SQLException;
import java.util.List;

public abstract class TableContainer {

    public abstract QueryResult executeRead(QueryExecutionConfig config) throws SQLException;

    public abstract QueryColumn addQueryColumn(String columnName, String alias, boolean visible);

    public abstract List<QueryColumn> getVisibleColumns();

    public abstract List<String> getAllColumnNames();

    public abstract String getTableNameOrAlias();

    public abstract void addRange(Range range);

    public abstract void setLimit(Integer value);

    public abstract QueryTemplatePacket createQueryTemplatePacketWithRange(Range range);

    public abstract void setQueryTemplatePackage(QueryTemplatePacket queryTemplatePacket);

    public abstract TableContainer getJoinedTable();

    public abstract void setJoinedTable(TableContainer joinedTable);

    public abstract QueryResult getQueryResult();

    public abstract void setJoined(boolean joined);

    public abstract boolean isJoined();
}
