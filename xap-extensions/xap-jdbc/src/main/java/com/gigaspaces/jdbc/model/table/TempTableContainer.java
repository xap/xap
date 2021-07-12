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
package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.internal.utils.ObjectConverter;
import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.explainplan.SubqueryExplainPlan;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.result.*;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.*;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class TempTableContainer extends TableContainer {
    protected final String alias;
    protected QueryResult tableResult;
    protected TempTableQTP queryTemplatePacket;
    protected final List<IQueryColumn> visibleColumns = new ArrayList<>();
    protected final List<IQueryColumn> tableColumns = new ArrayList<>();
    protected final Set<IQueryColumn> invisibleColumns = new HashSet<>();
    protected final List<String> allColumnNamesSorted = new ArrayList<>();
    private TableContainer joinedTable;

    public TempTableContainer(String alias) {
        this.alias = alias;
    }

    public TempTableContainer init(QueryResult tableResult) {
        this.tableResult = tableResult;
        if (tableResult instanceof ExplainPlanQueryResult) {
            tableColumns.addAll(((ExplainPlanQueryResult) tableResult).getVisibleColumns());
        } else {
            tableColumns.addAll(tableResult.getSelectedColumns());
        }

        allColumnNamesSorted.addAll(tableColumns.stream().map(IQueryColumn::getAlias).collect(Collectors.toList()));

        return this;
    }

    @Override
    public QueryResult executeRead(QueryExecutionConfig config) throws SQLException {
        if (config.isExplainPlan()) {
            ExplainPlanQueryResult explainResult = ((ExplainPlanQueryResult) tableResult);
            SubqueryExplainPlan subquery = new SubqueryExplainPlan(getSelectedColumns(),
                    (alias == null ? config.getTempTableNameGenerator().generate() : alias),
                    explainResult.getExplainPlanInfo(), getExprTree(), Collections.unmodifiableList(getOrderColumns()),
                    Collections.unmodifiableList(getGroupByColumns()), isDistinct());
            return new ExplainPlanQueryResult(getSelectedColumns(), subquery, this);
        }
        if (queryTemplatePacket != null) {
            tableResult.filter(x -> queryTemplatePacket.eval(x));
        }

        validate();

        TempQueryResult queryResult = new TempQueryResult(this);
        if(!getGroupByColumns().isEmpty()){
            queryResult.groupBy(); //group the results at the client
            if( hasAggregationFunctions() ) {
                Map<TableRowGroupByKey, List<TableRow>> groupByRowsResult = queryResult.getGroupByRowsResult();
                List<TableRow> totalAggregationsResultRowsList = new ArrayList<>();
                for (List<TableRow> rowsList : groupByRowsResult.values()) {
                    TableRow aggregatedRow = queryResult.aggregate(rowsList);
                    totalAggregationsResultRowsList.add( aggregatedRow );
                }
                queryResult.setRows( totalAggregationsResultRowsList );
            }
        }
        if(!getOrderColumns().isEmpty()) {
            queryResult.sort(); //sort the results at the client
        }
        if(isDistinct()){
            queryResult.distinct();
        }

        return queryResult;
    }

    @Override
    public IQueryColumn addQueryColumn(String columnName, String columnAlias, boolean isVisible, int columnOrdinal) {
        String columnNameOrAlias = columnAlias == null ? columnName : columnAlias;
        IQueryColumn queryColumn = tableColumns.stream() //TODO: @sagiv change to set?
                .filter(qc -> qc.getAlias().equalsIgnoreCase(columnNameOrAlias))
                .findFirst()
                .orElseThrow(() -> new ColumnNotFoundException("Could not find column with name [" + columnNameOrAlias + "]"))
                .create(columnName, columnAlias, isVisible, columnOrdinal);
        if (isVisible) {
            this.visibleColumns.add(queryColumn);
        } else {
            this.invisibleColumns.add(queryColumn);
        }
        return queryColumn;
    }

    @Override
    public List<IQueryColumn> getVisibleColumns() {
        return visibleColumns;
    }

    @Override
    public Set<IQueryColumn> getInvisibleColumns() {
        return this.invisibleColumns;
    }

    @Override
    public List<String> getAllColumnNames() {
        return allColumnNamesSorted;
    }

    @Override
    public String getTableNameOrAlias() {
        return alias;
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
        addQueryColumn(range.getPath(), null, false, -1);
        if (range instanceof EqualValueRange) {
            return new TempTableQTP((EqualValueRange) range);
        } else if (range instanceof SegmentRange || range instanceof NotEqualValueRange || range instanceof RegexRange) {
            return new TempTableQTP(range);
        } else {
            throw new UnsupportedOperationException("Range: " + range);
        }
    }

    @Override
    public void setQueryTemplatePacket(QueryTemplatePacket queryTemplatePacket) {
        this.queryTemplatePacket = ((TempTableQTP) queryTemplatePacket);
    }

    @Override
    public void setJoined(boolean joined) {

    }

    @Override
    public boolean isJoined() {
        return false;
    }

    @Override
    public boolean hasColumn(String columnName) {
        return getAllColumnNames().contains(columnName);
    }

    @Override
    public Object getColumnValue(String columnName, Object value) throws SQLException {
        IQueryColumn column = tableColumns.stream()
                .filter(queryColumn -> queryColumn.getName().equalsIgnoreCase(columnName))
                .findFirst()
                .orElseThrow(() -> new ColumnNotFoundException("Could not find column with name [" + columnName + "]"));

        return ObjectConverter.convert(value, column.getReturnType());
    }

    @Override
    public JoinInfo getJoinInfo() {
        return null;
    }

    @Override
    public void setJoinInfo(JoinInfo joinInfo) {
    }

    @Override
    public boolean checkJoinCondition() {
        return false;
    }
}
