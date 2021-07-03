package com.gigaspaces.jdbc.calcite.experimental;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.explainplan.ExplainPlanV3;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ProjectionTemplate;
import com.gigaspaces.jdbc.calcite.experimental.model.*;
import com.gigaspaces.jdbc.calcite.experimental.model.join.JoinInfo;
import com.gigaspaces.jdbc.calcite.experimental.result.ConcreteQueryResult;
import com.gigaspaces.jdbc.calcite.experimental.result.ExplainPlanQueryResult;
import com.gigaspaces.jdbc.calcite.experimental.result.QueryResult;
import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;

import com.gigaspaces.jdbc.model.table.ConcreteColumn;
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

public class SingleResultSupplier implements ResultSupplier{
    private final ITypeDesc typeDesc;
    private final IJSpace space;
    private QueryTemplatePacket queryTemplatePacket;
    private QueryResult queryResult;
    private Integer limit = Integer.MAX_VALUE;;
    private boolean distinct;
    private final Map<String, IQueryColumn> physicalColumns = new HashMap<>();
    private final List<IQueryColumn> projectionColumns = new ArrayList<>();
    private final List<OrderColumn> orderColumns = new ArrayList<>();
    private final List<AggregationColumn> aggregationColumns = new ArrayList<>();
    private final List<FunctionColumn> functionColumns = new ArrayList<>();
    private final Object[] preparedValues;
    private final List<PhysicalColumn> groupByColumns = new ArrayList<>();


    public SingleResultSupplier(ITypeDesc typeDesc, IJSpace space, Object[] preparedValues) {
        this.typeDesc = typeDesc;
        this.space = space;
        this.preparedValues = preparedValues;
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
                final Map<String, String> visibleColumnsAndAliasMap = getProjectedColumns().stream().collect(Collectors.toMap
                        (IQueryColumn::getName, queryColumn ->
                                        queryColumn.getAlias().equals(queryColumn.getName()) ? "" : queryColumn.getAlias()
                                , (oldValue, newValue) -> newValue, LinkedHashMap::new));

                explainPlanImpl = new ExplainPlanV3(typeDesc.getTypeSimpleName(), typeDesc.getTypeSimpleName(), visibleColumnsAndAliasMap, isDistinct());
                queryTemplatePacket.setExplainPlan(explainPlanImpl);
                modifiers = Modifiers.add(modifiers, Modifiers.EXPLAIN_PLAN);
                modifiers = Modifiers.add(modifiers, Modifiers.DRY_RUN);
            }

//            validate();

            setAggregations(config.isJoinUsed());

            queryTemplatePacket.prepareForSpace(typeDesc);

            IQueryResultSet<IEntryPacket> res = queryTemplatePacket.readMultiple(space.getDirectProxy(), null, limit, modifiers);
            if (explainPlanImpl != null) {
                queryResult = new ExplainPlanQueryResult(getProjectedColumns(), explainPlanImpl.getExplainPlanInfo(), this);
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

    private boolean hasOrderColumns() {
        return !this.orderColumns.isEmpty();
    }

    private boolean hasGroupByColumns() {
        return !this.groupByColumns.isEmpty();
    }

    private QueryTemplatePacket createEmptyQueryTemplatePacket() {
        QueryTableData queryTableData = new QueryTableData(this.typeDesc.getTypeName(), null, 0);
        queryTableData.setTypeDesc(typeDesc);
        return new QueryTemplatePacket(queryTableData, QueryResultTypeInternal.NOT_SET);
    }

    private String[] createProjectionTable() {
        if(projectionColumns.isEmpty()){
            return typeDesc.getPropertiesNames();
        }
        return physicalColumns.values().stream().map(IQueryColumn::getName).toArray(String[]::new);
    }

    @Override
    public Object[] getPreparedValues() {
        return preparedValues;
    }

    @Override
    public QueryTemplatePacket createQueryTemplatePacketWithRange(Range range) {
        QueryTableData queryTableData = new QueryTableData(this.typeDesc.getTypeName(), null, 0);
        queryTableData.setTypeDesc(typeDesc);
        return new QueryTemplatePacket(queryTableData, QueryResultTypeInternal.NOT_SET, range.getPath(), range);
    }

    @Override
    public JoinInfo getJoinInfo() {
        return null;
    }

    @Override
    public Object getColumnValue(String column, Object value) throws SQLException{
        return SQLUtil.cast(typeDesc, column, value, false);
    }

    @Override
    public QueryResult getQueryResult() {
        return queryResult;
    }

    @Override
    public List<OrderColumn> getOrderColumns() {
        return Collections.unmodifiableList(orderColumns);
    }

    @Override
    public boolean hasAggregationFunctions() {
        return false;
    }

    @Override
    public List<PhysicalColumn> getGroupByColumns() {
        return Collections.unmodifiableList(groupByColumns);
    }

    @Override
    public String getTableNameOrAlias() {
        return typeDesc.getTypeName();
    }

    @Override
    public List<IQueryColumn> getProjectedColumns() {
        if(projectionColumns.isEmpty()){
            return Arrays.stream(typeDesc.getPropertiesNames()).map(p -> physicalColumns.containsKey(p) ? physicalColumns.get(p) : new PhysicalColumn(p, null, this)).collect(Collectors.toList());
        }
        return projectionColumns;
    }

    @Override
    public List<IQueryColumn> getAllQueryColumns() {
        if(projectionColumns.isEmpty()){
            return Arrays.stream(typeDesc.getPropertiesNames()).map(p -> new PhysicalColumn(p, null, this)).collect(Collectors.toList());
        }
        return projectionColumns;
    }

    @Override
    public void setLimit(Integer value) {
        this.limit = value;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    @Override
    public IQueryColumn getColumnByName(String column) throws ColumnNotFoundException {
        throw new ColumnNotFoundException("");
    }

    @Override
    public boolean hasColumn(String column) {
        return false;
    }

    @Override
    public void addProjection(IQueryColumn projection) {
        projectionColumns.add(projection);
        if(projection.isPhysical()){
            physicalColumns.putIfAbsent(projection.getName(), projection);
        }
    }

    @Override
    public IQueryColumn getOrCreatePhysicalColumn(String physicalColumn) throws ColumnNotFoundException{
        if (!physicalColumn.equalsIgnoreCase(com.gigaspaces.jdbc.model.table.IQueryColumn.UUID_COLUMN) && typeDesc.getFixedPropertyPositionIgnoreCase(physicalColumn) == -1) {
            throw new ColumnNotFoundException("Could not find column with name [" + physicalColumn + "]");
        }
        if(!physicalColumns.containsKey(physicalColumn)){
            physicalColumns.put(physicalColumn, new PhysicalColumn(physicalColumn, null, this));
        }
        return physicalColumns.get(physicalColumn);
    }

    @Override
    public void addAggregationColumn(AggregationColumn aggregationColumn) {
        aggregationColumns.add(aggregationColumn);
    }

    @Override
    public void addOrderColumn(OrderColumn orderColumn) {
        orderColumns.add(orderColumn);
    }

    @Override
    public void addFunctionColumn(FunctionColumn functionColumn) {
        functionColumns.add(functionColumn);
    }

    @Override
    public Class<?> getReturnType(String columnName) throws SQLException {
        return SQLUtil.getPropertyType(typeDesc, columnName);
    }

    @Override
    public ResultSupplier getJoinedSupplier() {
        return null;
    }

    @Override
    public void setQueryTemplatePacket(QueryTemplatePacket queryTemplatePacket) {
        this.queryTemplatePacket = queryTemplatePacket;
    }

    @Override
    public List<AggregationColumn> getAggregationColumns() {
        return aggregationColumns;
    }

    @Override
    public void addGroupByColumn(PhysicalColumn physicalColumn) {
        this.groupByColumns.add(physicalColumn);
    }

    @Override
    public boolean clearProjections() {
        if(projectionColumns.isEmpty()){
            return false;
        }
        projectionColumns.clear();
        return true;
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

        for(IQueryColumn projectedColumn : getProjectedColumns() ){
            aggregationSet = aggregationSet.add(new SingleValueAggregator().setPath(projectedColumn.getName()));
        }
    }

    private void setGroupByAggregation() {
        //groupBy in server
        List<PhysicalColumn> groupByColumns = getGroupByColumns();
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

    private void setDistinctAggregation() {
        //distinct in server
        if (isDistinct()) {
            if (!projectionColumns.isEmpty()) {
                String[] distinctColumnsArray = new String[projectionColumns.size()];
                for (int i = 0; i < projectionColumns.size(); i++) {
                    distinctColumnsArray[i] = projectionColumns.get(i).getName();
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

    /*private final List<OrderColumn> orderColumns = new ArrayList<>();
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

    public abstract SingleResultSupplier getJoinedTable();

    public abstract void setJoinedTable(SingleResultSupplier joinedTable);

    public abstract QueryResult getQueryResult();

    public abstract void setJoined(boolean joined);

    public abstract boolean isJoined();

    public abstract boolean hasColumn(String columnName);

    public abstract JoinInfo getJoinInfo();

    public abstract void setJoinInfo(JoinInfo joinInfo);

    public abstract boolean checkJoinCondition();

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
                    " h  clause or be used in an aggregate function");
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
    }*/

}
