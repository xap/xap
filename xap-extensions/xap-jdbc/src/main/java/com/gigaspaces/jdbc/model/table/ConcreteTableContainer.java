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

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.explainplan.ExplainPlanV3;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ProjectionTemplate;
import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.exceptions.TypeNotFoundException;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.result.ExplainPlanResult;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.query.aggregators.*;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.jdbc.SQLUtil;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;
import com.j_spaces.jdbc.query.IQueryResultSet;
import com.j_spaces.jdbc.query.QueryTableData;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class ConcreteTableContainer extends TableContainer {
    private final IJSpace space;
    private QueryTemplatePacket queryTemplatePacket;
    private final ITypeDesc typeDesc;
    private final List<String> allColumnNamesSorted;
    private final List<QueryColumn> visibleColumns = new ArrayList<>();
    private final Set<QueryColumn> invisibleColumns = new HashSet<>();
    private final String name;
    private final String alias;
    private Integer limit = Integer.MAX_VALUE;
    private QueryResult queryResult;
    private TableContainer joinedTable;
    private boolean joined = false;
    private JoinInfo joinInfo;

    public ConcreteTableContainer(String name, String alias, IJSpace space) {
        this.space = space;
        this.name = name;
        this.alias = alias;

        try {
            typeDesc = SQLUtil.checkTableExistence(name, space);
        } catch (SQLException e) {
            throw new TypeNotFoundException("Unknown table [" + name + "]", e);
        }

        allColumnNamesSorted = Arrays.asList(typeDesc.getPropertiesNames());
    }

    @Override
    public QueryResult executeRead(QueryExecutionConfig config) throws SQLException {
        if (queryResult != null)
            return queryResult;
        //TODO: @sagiv at old JDBC, projectionC contains Aggregation column too. do the same here?
        String[] projectionC = createProjectionTable();

        try {
            ProjectionTemplate _projectionTemplate = ProjectionTemplate.create(projectionC, typeDesc);

            if (queryTemplatePacket == null) {
                queryTemplatePacket = createEmptyQueryTemplatePacket();
            }
            queryTemplatePacket.setProjectionTemplate(_projectionTemplate);

            int modifiers = ReadModifiers.REPEATABLE_READ;
            ExplainPlanV3 explainPlanImpl = null;
            if (config.isExplainPlan()) {
                // Using LinkedHashMap to keep insertion order from the ArrayList
                final Map<String, String> visibleColumnsAndAliasMap = visibleColumns.stream().filter(QueryColumn::isVisible).collect(Collectors.toMap
                        (QueryColumn::getName, queryColumn -> queryColumn.getAlias() == null ? "" : queryColumn.getAlias()
                                , (oldValue, newValue) -> newValue, LinkedHashMap::new));

                explainPlanImpl = new ExplainPlanV3(name, alias, visibleColumnsAndAliasMap);
                queryTemplatePacket.setExplainPlan(explainPlanImpl);
                modifiers = Modifiers.add(modifiers, Modifiers.EXPLAIN_PLAN);
                modifiers = Modifiers.add(modifiers, Modifiers.DRY_RUN);
            }

            validateAggregationFunction();

            // When we use join, we aggregate the results on the client side instead of on the server.
            if(!config.isJoinUsed()) {
                setAggregations();
            }
            queryTemplatePacket.prepareForSpace(typeDesc);

            IQueryResultSet<IEntryPacket> res = queryTemplatePacket.readMultiple(space.getDirectProxy(), null, limit, modifiers);
            if (explainPlanImpl != null) {
                queryResult = new ExplainPlanResult(visibleColumns, explainPlanImpl.getExplainPlanInfo(), this);
            } else {
                queryResult = new QueryResult(res, this);
            }
            return queryResult;
        } catch (Exception e) {
            throw new SQLException("Failed to get results from space", e);
        }
    }

    private String[] createProjectionTable() {
        Set<QueryColumn> tmp = new HashSet<>(invisibleColumns);
        tmp.addAll(visibleColumns);
        return tmp.stream().map(QueryColumn::getName).toArray(String[]::new);
    }

    private void setAggregations() {
        setOrderByAggregation();
        setAggregationFunctions();
        setGroupByAggregation();
    }

    private void setGroupByAggregation() {
        //groupBy in server
        List<QueryColumn> groupByColumns = getGroupByColumns();
        if(!groupByColumns.isEmpty()){
            int groupByColumnsCount = groupByColumns.size();
            String[] groupByColumnsArray = new String[ groupByColumnsCount ];
            for ( int i=0; i < groupByColumnsCount; i++) {
                groupByColumnsArray[ i ] = groupByColumns.get( i ).getName();
            }

            DistinctAggregator distinctAggregator = new DistinctAggregator().distinct(limit, groupByColumnsArray);
            if( queryTemplatePacket.getAggregationSet() == null ) {
                AggregationSet aggregationSet = new AggregationSet().distinct( distinctAggregator );
                queryTemplatePacket.setAggregationSet(aggregationSet);
            }
            else{
                queryTemplatePacket.getAggregationSet().add(distinctAggregator);
            }
        }
    }

    private void setOrderByAggregation() {
        if(hasOrderColumns()){
            OrderByAggregator orderByAggregator = new OrderByAggregator();
            for (OrderColumn column : getOrderColumns()) {
                orderByAggregator.orderBy(column.getName(), column.isAsc() ? OrderBy.ASC : OrderBy.DESC, column.isNullsLast());
            }

            if( queryTemplatePacket.getAggregationSet() == null ) {
                AggregationSet aggregationSet = new AggregationSet().orderBy( orderByAggregator );
                queryTemplatePacket.setAggregationSet(aggregationSet);
            }
            else{
                queryTemplatePacket.getAggregationSet().add(orderByAggregator);
            }
        }
    }

    private void setAggregationFunctions() {
        if(!hasAggregationFunctions()) {
            return;
        }

        AggregationSet aggregationSet;
        if( queryTemplatePacket.getAggregationSet() == null ) {
            aggregationSet = new AggregationSet();
            queryTemplatePacket.setAggregationSet(aggregationSet);
        }
        else{
            aggregationSet = queryTemplatePacket.getAggregationSet();
        }

        for (AggregationColumn aggregationColumn : getAggregationFunctionColumns()) {
            switch (aggregationColumn.getType()) {
                case COUNT:
                    if (aggregationColumn.isAllColumns()) {
                        aggregationSet.count();
                    } else {
                        aggregationSet.count(aggregationColumn.getColumnName());
                    }
                    break;
                case MAX:
                    aggregationSet.maxValue(aggregationColumn.getColumnName());
                    break;
                case MIN:
                    aggregationSet.minValue(aggregationColumn.getColumnName());
                    break;
                case AVG:
                    aggregationSet.average(aggregationColumn.getColumnName());
                    break;
                case SUM:
                    aggregationSet.sum(aggregationColumn.getColumnName());
                    break;
            }
        }
    }

    @Override
    public QueryColumn addQueryColumn(String columnName, String alias, boolean visible, int columnIndex) {
        if (!columnName.equalsIgnoreCase(QueryColumn.UUID_COLUMN) && typeDesc.getFixedPropertyPositionIgnoreCase(columnName) == -1) {
            throw new ColumnNotFoundException("Could not find column with name [" + columnName + "]");
        }

        try {
            QueryColumn qc = new QueryColumn(columnName, SQLUtil.getPropertyType(typeDesc, columnName), alias,
                    visible, this, columnIndex);
            if (visible) {
                this.visibleColumns.add(qc);
            } else {
                this.invisibleColumns.add(qc);
            }
            return qc;
        } catch (SQLException e) {
            throw new ColumnNotFoundException("Could not find column with name [" + columnName + "]", e);
        }
    }

    public List<QueryColumn> getVisibleColumns() {
        return visibleColumns;
    }

    @Override
    public Set<QueryColumn> getInvisibleColumns() {
        return this.invisibleColumns;
    }

    @Override
    public List<String> getAllColumnNames() {
        return allColumnNamesSorted;
    }

    @Override
    public String getTableNameOrAlias() {
        return alias == null ? name : alias;
    }

    @Override
    public TableContainer getJoinedTable() {
        return joinedTable;
    }

    @Override
    public void setJoinedTable(TableContainer joinedTable) {
        this.joinedTable = joinedTable;
    }

    public QueryResult getQueryResult() {
        return queryResult;
    }

    @Override
    public void setLimit(Integer value) {
        if (this.limit != Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Already set!");
        }
        this.limit = value;
    }


    private QueryTemplatePacket createEmptyQueryTemplatePacket() {
        QueryTableData queryTableData = new QueryTableData(this.name, null, 0);
        queryTableData.setTypeDesc(typeDesc);
        return new QueryTemplatePacket(queryTableData, QueryResultTypeInternal.NOT_SET);
    }

    @Override
    public QueryTemplatePacket createQueryTemplatePacketWithRange(Range range) {
        QueryTableData queryTableData = new QueryTableData(this.name, null, 0);
        queryTableData.setTypeDesc(typeDesc);
        return new QueryTemplatePacket(queryTableData, QueryResultTypeInternal.NOT_SET, range.getPath(), range);
    }

    @Override
    public void setQueryTemplatePacket(QueryTemplatePacket queryTemplatePacket) {
        this.queryTemplatePacket = queryTemplatePacket;
    }

    @Override
    public boolean isJoined() {
        return joined;
    }

    @Override
    public boolean hasColumn(String columnName) {
        return allColumnNamesSorted.contains(columnName);
    }

    @Override
    public void setJoined(boolean joined) {
        this.joined = joined;
    }

    @Override
    public Object getColumnValue(String columnName, Object value) throws SQLException {
        return SQLUtil.cast(typeDesc, columnName, value, false);

    }

    @Override
    public JoinInfo getJoinInfo() {
        return joinInfo;
    }

    @Override
    public void setJoinInfo(JoinInfo joinInfo) {
        this.joinInfo = joinInfo;
    }

    @Override
    public boolean checkJoinCondition() {
        if (joinInfo == null)
            return true;
        return joinInfo.checkJoinCondition();
    }
}
