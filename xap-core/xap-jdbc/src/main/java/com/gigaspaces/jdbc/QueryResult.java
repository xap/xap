package com.gigaspaces.jdbc;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.j_spaces.jdbc.query.IQueryResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class QueryResult extends ArrayList<TableEntry> {
    private final List<QueryColumn> queryColumns;
    public QueryResult(IQueryResultSet<IEntryPacket> res, List<QueryColumn> queryColumns) {
        this.queryColumns = queryColumns;
        this.addAll(res.stream().map(x -> new TableEntry(x, queryColumns)).collect(Collectors.toList()));
    }

    public List<QueryColumn> getQueryColumns() {
        return queryColumns;
    }
}
