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
import com.gigaspaces.jdbc.model.result.ConcreteQueryResult;
import com.gigaspaces.jdbc.model.result.ExplainPlanQueryResult;
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
import java.util.stream.Stream;

public class ConcreteTableContainer extends TableContainer {
    private final IJSpace space;
    private QueryTemplatePacket queryTemplatePacket;
    private final ITypeDesc typeDesc;
    private final List<String> allColumnNamesSorted;
    private final List<IQueryColumn> visibleColumns = new ArrayList<>();
    private final Set<IQueryColumn> invisibleColumns = new HashSet<>();
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
                final Map<String, String> visibleColumnsAndAliasMap = getSelectedColumns().stream().collect(Collectors.toMap
                        (IQueryColumn::getName, queryColumn ->
                                        queryColumn.getAlias().equals(queryColumn.getName()) ? "" : queryColumn.getAlias()
                                , (oldValue, newValue) -> newValue, LinkedHashMap::new));

                explainPlanImpl = new ExplainPlanV3(name, alias, visibleColumnsAndAliasMap, isDistinct());
                queryTemplatePacket.setExplainPlan(explainPlanImpl);
                modifiers = Modifiers.add(modifiers, Modifiers.EXPLAIN_PLAN);
                modifiers = Modifiers.add(modifiers, Modifiers.DRY_RUN);
            }

            validate();

            setAggregations(config.isJoinUsed());

            queryTemplatePacket.prepareForSpace(typeDesc);

            IQueryResultSet<IEntryPacket> res = queryTemplatePacket.readMultiple(space.getDirectProxy(), null, limit, modifiers);
            if (explainPlanImpl != null) {
                queryResult = new ExplainPlanQueryResult(visibleColumns, explainPlanImpl.getExplainPlanInfo(), this);
            } else {
                queryResult = new ConcreteQueryResult(res, this);
                if( hasGroupByColumns() && hasOrderColumns() ){
                    queryResult.sort();
                }
            }
            return queryResult;
        } catch (Exception e) {
            throw new SQLException("Failed to get results from space", e);
        }
    }

    private String[] createProjectionTable() {
        return Stream.concat(visibleColumns.stream(), invisibleColumns.stream()).map(IQueryColumn::getName).distinct().toArray(String[]::new);
    }

    private void setAggregations(boolean isJoinUsed) {
        // When we use join, we aggregate the results on the client side instead of on the server.
        if(!isJoinUsed) {
            if( !hasGroupByColumns() ){
                setOrderByAggregation();
            }
            setAggregationFunctions();
            setGroupByAggregation();
        }
        setDistinctAggregation();
    }

    private void setDistinctAggregation() {
        //distinct in server
        if (isDistinct()) {
            if (!visibleColumns.isEmpty()) {
                String[] distinctColumnsArray = new String[visibleColumns.size()];
                for (int i = 0; i < visibleColumns.size(); i++) {
                    distinctColumnsArray[i] = visibleColumns.get(i).getName();
                }
                DistinctAggregator distinctAggregator = new DistinctAggregator().distinct(false, limit, distinctColumnsArray);
                if (queryTemplatePacket.getAggregationSet() == null) {
                    AggregationSet aggregationSet = new AggregationSet().distinct(distinctAggregator);
                    queryTemplatePacket.setAggregationSet(aggregationSet);
                } else {
                    queryTemplatePacket.getAggregationSet().add(distinctAggregator);
                }
            }
            if (hasAggregationFunctions()){
                String[] distinctColumnsArray = new String[getAggregationColumns().size()];
                for (int i = 0; i < getAggregationColumns().size(); i++) {
                    distinctColumnsArray[i] = getAggregationColumns().get(i).getColumnName();
                }
                DistinctAggregator distinctAggregator = new DistinctAggregator().distinct(false, limit, distinctColumnsArray);
                if (queryTemplatePacket.getAggregationSet() == null) {
                    AggregationSet aggregationSet = new AggregationSet().distinct(distinctAggregator);
                    queryTemplatePacket.setAggregationSet(aggregationSet);
                } else {
                    queryTemplatePacket.getAggregationSet().add(distinctAggregator);
                }
            }
        }

    }

    private void setGroupByAggregation() {
        //groupBy in server
        List<ConcreteColumn> groupByColumns = getGroupByColumns();
        if(!groupByColumns.isEmpty()){
            int groupByColumnsCount = groupByColumns.size();
            String[] groupByColumnsArray = new String[ groupByColumnsCount ];
            for ( int i=0; i < groupByColumnsCount; i++) {
                groupByColumnsArray[ i ] = groupByColumns.get( i ).getName();
            }

            if( hasAggregationFunctions() ){

                GroupByAggregator groupByAggregator = new GroupByAggregator().groupBy(groupByColumnsArray);
                List<SpaceEntriesAggregator> aggregators = AggregationInternalUtils.getAggregators(queryTemplatePacket.getAggregationSet());
                if (!aggregators.isEmpty()) {
                    groupByAggregator = groupByAggregator.select(aggregators.toArray(new SpaceEntriesAggregator[]{}));
                }

                queryTemplatePacket.setAggregationSet( new AggregationSet().groupBy(groupByAggregator) );
            }
            else {
                //int limit = hasOrderColumns() ? Integer.MAX_VALUE : entriesLimit;
                DistinctAggregator distinctAggregator = new DistinctAggregator().distinct(true, limit, groupByColumnsArray);
                if (queryTemplatePacket.getAggregationSet() == null) {
                    AggregationSet aggregationSet = new AggregationSet().distinct(distinctAggregator);
                    queryTemplatePacket.setAggregationSet(aggregationSet);
                } else {
                    queryTemplatePacket.getAggregationSet().add(distinctAggregator);
                }
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

        for (AggregationColumn aggregationColumn : getAggregationColumns()) {
            String columnName = aggregationColumn.getColumnName();
            switch (aggregationColumn.getType()) {
                case COUNT:
                    if (aggregationColumn.isAllColumns()) {
                        aggregationSet.count();
                    } else {
                        aggregationSet.count(columnName);
                    }
                    break;
                case MAX:
                    aggregationSet.maxValue(columnName);
                    break;
                case MIN:
                    aggregationSet.minValue(columnName);
                    break;
                case AVG:
                    aggregationSet.average(columnName);
                    break;
                case SUM:
                    aggregationSet.sum(columnName);
                    break;
            }
        }

        for( IQueryColumn visibleColumn : getVisibleColumns() ){
            aggregationSet = aggregationSet.add(new SingleValueAggregator().setPath(visibleColumn.getName()));
        }
    }

    @Override
    public IQueryColumn addQueryColumn(String columnName, String columnAlias, boolean isVisible, int columnOrdinal) {
        if (!columnName.equalsIgnoreCase(IQueryColumn.UUID_COLUMN) && typeDesc.getFixedPropertyPositionIgnoreCase(columnName) == -1) {
            throw new ColumnNotFoundException("Could not find column with name [" + columnName + "]");
        }

        try {
            ConcreteColumn concreteColumn = new ConcreteColumn(columnName, SQLUtil.getPropertyType(typeDesc, columnName), columnAlias,
                    isVisible, this, columnOrdinal);
            if (isVisible) {
                this.visibleColumns.add(concreteColumn);
            } else {
                this.invisibleColumns.add(concreteColumn);
            }
            return concreteColumn;
        } catch (SQLException e) {
            throw new ColumnNotFoundException("Could not find column with name [" + columnName + "]", e);
        }
    }

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

    public ITypeDesc getTypeDesc() {
        return typeDesc;
    }
}
