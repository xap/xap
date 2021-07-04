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

import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;
import net.sf.jsqlparser.expression.Expression;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class TableContainer {

    private final List<OrderColumn> orderColumns = new ArrayList<>();
    private final List<AggregationColumn> aggregationColumns = new ArrayList<>();
    private final List<ConcreteColumn> groupByColumns = new ArrayList<>();
    private boolean distinct;
    private Expression exprTree;

    public abstract QueryResult executeRead(QueryExecutionConfig config) throws SQLException;

    public abstract IQueryColumn addQueryColumn(String columnName, String columnAlias, boolean isVisible, int columnOrdinal);

    public abstract List<IQueryColumn> getVisibleColumns();

    public abstract Set<IQueryColumn> getInvisibleColumns();

    public List<IQueryColumn> getAllQueryColumns() {
        return Stream.concat(getVisibleColumns().stream(), getInvisibleColumns().stream()).collect(Collectors.toList());
    }

    public List<IQueryColumn> getSelectedColumns() {
        return Stream.concat(getVisibleColumns().stream(), getAggregationColumns().stream())
                .sorted().collect(Collectors.toList());
    }

    public abstract List<String> getAllColumnNames();

    public abstract String getTableNameOrAlias();

    public abstract void setLimit(Integer value);

    public abstract QueryTemplatePacket createQueryTemplatePacketWithRange(Range range);

    public abstract void setQueryTemplatePacket(QueryTemplatePacket queryTemplatePacket);

    public abstract Object getColumnValue(String columnName, Object value) throws SQLException;

    public abstract TableContainer getJoinedTable();

    public abstract void setJoinedTable(TableContainer joinedTable);

    public abstract QueryResult getQueryResult();

    public abstract void setJoined(boolean joined);

    public abstract boolean isJoined();

    public abstract boolean hasColumn(String columnName);

    public abstract JoinInfo getJoinInfo();

    public abstract void setJoinInfo(JoinInfo joinInfo);

    public abstract boolean checkJoinCondition();

    public void setExpTree(Expression value) {
        this.exprTree = value;
    }

    public Expression getExprTree() {
        return exprTree;
    }

    public void addOrderColumns(OrderColumn orderColumn) {
        this.orderColumns.add(orderColumn);
    }

    public void addGroupByColumns(ConcreteColumn groupByColumn) {
        this.groupByColumns.add(groupByColumn);
        if( !groupByColumn.isVisible() ) {
            this.getInvisibleColumns().add(groupByColumn);
        }
    }

    public List<ConcreteColumn> getGroupByColumns() {
        return groupByColumns;
    }

    public List<OrderColumn> getOrderColumns() {
        return orderColumns;
    }

    public void addAggregationColumn(AggregationColumn aggregationColumn) {
        this.aggregationColumns.add(aggregationColumn);
    }

    public List<AggregationColumn> getAggregationColumns() {
        return aggregationColumns;
    }

    public boolean hasGroupByColumns() {
        return !this.groupByColumns.isEmpty();
    }

    public boolean hasAggregationFunctions() {
        return !this.aggregationColumns.isEmpty();
    }

    public boolean hasOrderColumns() {
        return !this.orderColumns.isEmpty();
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    protected void validate() {

        validateGroupBy();

        //TODO: block operation not supported -- see AggregationsUtil.convertAggregationResult
        if (hasAggregationFunctions() && hasOrderColumns()) {
            throw new IllegalArgumentException("Column [" + getOrderColumns().get(0).getAlias() + "] must appear in the " +
                    "GROUP BY clause or be used in an aggregate function");
        }
    }



    private void validateGroupBy() {

        if( hasAggregationFunctions() ){
            List<IQueryColumn> visibleColumns = getVisibleColumns();
            List<ConcreteColumn> groupByColumns = getGroupByColumns();

            if( visibleColumns.isEmpty() ){
                return;
            }

            List<String> groupByColumnNames = new ArrayList<>();
            for( IQueryColumn groupByColumn : groupByColumns ){
                groupByColumnNames.add( groupByColumn.getName() );
            }

            List<String> missingVisibleColumnNames = new ArrayList<>();
            for( IQueryColumn visibleColumn : visibleColumns ){
                String visibleColumnName = visibleColumn.getName();
                if( !groupByColumnNames.contains( visibleColumnName ) ){
                    missingVisibleColumnNames.add( visibleColumnName );
                }
            }

            if( !missingVisibleColumnNames.isEmpty() ){
                throw new IllegalArgumentException( ( missingVisibleColumnNames.size() == 1 ? "Column" : "Columns" ) + " " +
                        Arrays.toString( missingVisibleColumnNames.toArray( new String[0] ) ) + " must appear in the " +
                        "GROUP BY clause or be used in an aggregate function");
            }
        }
    }

}
