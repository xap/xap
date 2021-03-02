package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.j_spaces.jdbc.query.IQueryResultSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class QueryResult {
    private final List<QueryColumn> queryColumns;
    private final List<TableRow> rows;

    public QueryResult(IQueryResultSet<IEntryPacket> res, List<QueryColumn> queryColumns) {
        this.queryColumns = queryColumns;
        this.rows = res.stream().map(x -> new TableRow(x, queryColumns)).collect(Collectors.toList());
    }

    public QueryResult(List<QueryColumn> queryColumns) {
        this.queryColumns = queryColumns;
        this.rows = new ArrayList<>();
    }

    public List<QueryColumn> getQueryColumns() {
        return queryColumns;
    }

    public int size() {
        return rows.size();
    }

    public Iterator<TableRow> iterator() {
        return rows.iterator();
    }

    public void add(TableRow tableRow) {
        this.rows.add(tableRow);
    }
}
