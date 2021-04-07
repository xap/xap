/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.jdbc.query.IQueryResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class QueryResult {
    private final List<QueryColumn> queryColumns;
    private final List<TableRow> rows;
    private final TableContainer tableContainer;
    private Cursor<TableRow> cursor;

    public QueryResult(IQueryResultSet<IEntryPacket> res, List<QueryColumn> queryColumns, TableContainer tableContainer) {
        this.queryColumns = filterNonVisibleColumns(queryColumns);
        this.tableContainer = tableContainer;
        this.rows = res.stream().map(x -> new TableRow(x, queryColumns)).collect(Collectors.toList());
        this.cursor = new RowScanCursor(rows);
    }

    public QueryResult(List<QueryColumn> queryColumns) {
        this.tableContainer = null; // TODO should be handled in subquery
        this.queryColumns = filterNonVisibleColumns(queryColumns);
        this.rows = new ArrayList<>();
    }

    private List<QueryColumn> filterNonVisibleColumns(List<QueryColumn> queryColumns){
        return queryColumns.stream().filter(QueryColumn::isVisible).collect(Collectors.toList());
    }

    public List<QueryColumn> getQueryColumns() {
        return queryColumns;
    }

    public int size() {
        return rows.size();
    }

    public void add(TableRow tableRow) {
        this.rows.add(tableRow);
    }

    public boolean next() {
        if(tableContainer == null || tableContainer.getJoinedTable() == null)
            return getCursor().next();
        QueryResult joinedResult = tableContainer.getJoinedTable().getQueryResult();
        if(joinedResult == null){
            return getCursor().next();
        }
        while (hasNext()){
            if(joinedResult.next()){
                return true;
            }
            if(getCursor().next()){
                joinedResult.reset();
            }else{
                return false;
            }
        }
        return false;
    }

    private boolean hasNext() {
        if(getCursor().isBeforeFirst())
            return getCursor().next();
        return true;
    }

    public TableRow getCurrent() {
        return getCursor().getCurrent();
    }

    public void reset() {
        getCursor().reset();
        if(tableContainer == null || tableContainer.getJoinedTable() == null) {
            return;
        }
        QueryResult joinedResult = tableContainer.getJoinedTable().getQueryResult();
        if(joinedResult != null) {
            joinedResult.reset();
        }
    }

    public Cursor<TableRow> getCursor() {
        if(cursor == null)
            cursor = new RowScanCursor(rows);
        return cursor;
    }
}
