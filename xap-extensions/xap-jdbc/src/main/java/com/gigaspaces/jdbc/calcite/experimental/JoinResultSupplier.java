package com.gigaspaces.jdbc.calcite.experimental;

import com.gigaspaces.jdbc.calcite.experimental.model.*;
import com.gigaspaces.jdbc.calcite.experimental.model.join.JoinInfo;
import com.gigaspaces.jdbc.calcite.experimental.result.JoinQueryResult;
import com.gigaspaces.jdbc.calcite.experimental.result.QueryResult;
import com.gigaspaces.jdbc.calcite.experimental.result.TableRow;
import com.gigaspaces.jdbc.calcite.experimental.result.TableRowFactory;
import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;

import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

public class JoinResultSupplier implements ResultSupplier {
    private final ResultSupplier left;
    private final ResultSupplier right;
    private final IJSpace space;
    private final QueryExecutionConfig config;
    private final Object[] preparedValues;
    private boolean isAllColumnsSelected = false;
    private final LinkedList<Integer> fieldCountList = new LinkedList<>();
    private final Map<String, IQueryColumn> physicalColumns = new HashMap<>();
    private final List<AggregationColumn> aggregationColumns = new ArrayList<>();
    private List<IQueryColumn> projectionColumns = new ArrayList<>();
    private List<IQueryColumn> allColumns = new ArrayList<>();
    private final List<OrderColumn> orderColumns = new ArrayList<>();
    private final List<FunctionColumn> functionColumns = new ArrayList<>();
    private final List<PhysicalColumn> groupByColumns = new ArrayList<>();
    private final JoinInfo joinInfo;
    private QueryResult queryResult;
    private final boolean root;


    public JoinResultSupplier(ResultSupplier left, ResultSupplier right, IJSpace space, QueryExecutionConfig config, Object[] preparedValues, JoinInfo joinInfo, boolean root) {
        this.left = left;
        this.right = right;
        this.space = space;
        this.config = config;
        this.preparedValues = preparedValues;
        this.joinInfo = joinInfo;
        this.root = root;
    }

    public JoinResultSupplier(ResultSupplier left, ResultSupplier right, IJSpace space, Object[] preparedValues, JoinInfo joinInfo, boolean root) {
        this(left, right, space, new QueryExecutionConfig(), preparedValues, joinInfo, root);
    }

    public QueryResult executeRead(QueryExecutionConfig config) throws SQLException {
        left.executeRead(config);
        right.executeRead(config);
        queryResult = new JoinQueryResult(getAllQueryColumns());
        while (next()) {
            // TODO - create a complete temporary table from join result where each column can get its own current value
            // TODO = notice that checking of join condition will not work with the temp view
            if(checkJoinCondition()) {
                queryResult.addRow(TableRowFactory.createTableRowFromSpecificColumns(getAllQueryColumns(), orderColumns, groupByColumns));
            }
        }
        return queryResult;
    }

    public Object[] getPreparedValues() {
        return preparedValues;
    }

    public boolean isAllColumnsSelected() {
        return isAllColumnsSelected;
    }

    public void setAllColumnsSelected(boolean isAllColumnsSelected) {
        this.isAllColumnsSelected = isAllColumnsSelected;
    }

    /*
    public QueryResult execute() {
        final List<OrderColumn> orderColumns = new ArrayList<>();
        final List<ConcreteColumn> groupByColumns = new ArrayList<>();
        boolean isDistinct = false;
        for (TableContainer table : tables) {
            try {
                table.executeRead(config);
                orderColumns.addAll(table.getOrderColumns());
                groupByColumns.addAll(table.getGroupByColumns());
                isDistinct |= table.isDistinct();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new IllegalArgumentException(e);
            }
        }

        JoinTablesIterator joinTablesIterator = new JoinTablesIterator(tables);
        if(config.isExplainPlan()) {
            return explain(joinTablesIterator, orderColumns, groupByColumns, isDistinct);
        }
        QueryResult res = new JoinQueryResult(this.selectedQueryColumns);
        while (joinTablesIterator.hasNext()) {
            if(tables.stream().allMatch(TableContainer::checkJoinCondition))
                res.addRow(TableRowFactory.createTableRowFromSpecificColumns(this.allQueryColumns, orderColumns,
                        groupByColumns));
        }

        if( groupByColumns.isEmpty()) {
            if( !aggregationColumns.isEmpty() ) {
                List<TableRow> aggregateRows = new ArrayList<>();
                aggregateRows.add(TableRowUtils.aggregate(res.getRows(), selectedQueryColumns, this.aggregationColumns, visibleColumns));
                res.setRows(aggregateRows);
            }
        }
        else {
            res.groupBy(); //group by the results at the client
            if( !aggregationColumns.isEmpty() ) {
                Map<TableRowGroupByKey, List<TableRow>> groupByRowsResult = res.getGroupByRowsResult();
                List<TableRow> totalAggregationsResultRowsList = new ArrayList<>();
                for (List<TableRow> rowsList : groupByRowsResult.values()) {
                    TableRow aggregatedRow = TableRowUtils.aggregate( rowsList, selectedQueryColumns, aggregationColumns, visibleColumns );
                    totalAggregationsResultRowsList.add( aggregatedRow );
                }
                res.setRows( totalAggregationsResultRowsList );
            }
        }
        if(!orderColumns.isEmpty()) {
            res.sort(); //sort the results at the client
        }
        if (isDistinct){
            res.distinct();
        }

        return res;
    }

    private QueryResult explain(JoinTablesIterator joinTablesIterator, List<OrderColumn> orderColumns,
                                List<ConcreteColumn> groupByColumns, boolean isDistinct) {
        Stack<TableContainer> stack = new Stack<>();
        TableContainer current = joinTablesIterator.getStartingPoint();
        stack.push(current);
        while (current.getJoinedTable() != null){
            current = current.getJoinedTable();
            stack.push(current);
        }
        TableContainer first = stack.pop();
        TableContainer second = stack.pop();
        JoinExplainPlan joinExplainPlan = new JoinExplainPlan(first.getJoinInfo(), ((ExplainPlanQueryResult) first.getQueryResult()).getExplainPlanInfo(), ((ExplainPlanQueryResult) second.getQueryResult()).getExplainPlanInfo());
        TableContainer last = second;
        while (!stack.empty()) {
            TableContainer curr = stack.pop();
            joinExplainPlan = new JoinExplainPlan(last.getJoinInfo(), joinExplainPlan, ((ExplainPlanQueryResult) curr.getQueryResult()).getExplainPlanInfo());
            last = curr;
        }
        joinExplainPlan.setSelectColumns(visibleColumns.stream().map(IQueryColumn::toString).collect(Collectors.toList()));
        joinExplainPlan.setOrderColumns(orderColumns);
        joinExplainPlan.setGroupByColumns(groupByColumns);
        joinExplainPlan.setDistinct(isDistinct);
        return new ExplainPlanQueryResult(visibleColumns, joinExplainPlan, null);
    }
     */

    @Override
    public List<AggregationColumn> getAggregationColumns() {
        return aggregationColumns;
    }

    public IJSpace getSpace() {
        return space;
    }

    public QueryExecutionConfig getConfig() {
        return config;
    }

    public void addColumn(IQueryColumn column) {
//        if (column.isVisible()) {
//            visibleColumns.add(column);
//        } else {
//            invisibleColumns.add(column);
//        }
    }

    @Override
    public void addAggregationColumn(AggregationColumn aggregationColumn) {

    }

    @Override
    public void addOrderColumn(OrderColumn orderColumn) {

    }

    public void addFieldCount(int size) {
        int columnCount = fieldCountList.isEmpty() ?  size: fieldCountList.getLast() + size;
        fieldCountList.add(columnCount);
    }

    @Override
    public QueryTemplatePacket createQueryTemplatePacketWithRange(Range range) {
        return null;
    }

    @Override
    public JoinInfo getJoinInfo() {
        return null;
    }

    @Override
    public Object getColumnValue(String column, Object value) throws SQLException{
        return null;
    }

    @Override
    public QueryResult getQueryResult() {
        return this.queryResult;
    }

    @Override
    public List<OrderColumn> getOrderColumns() {
        return null;
    }

    @Override
    public boolean hasAggregationFunctions() {
        return false;
    }

    @Override
    public List<PhysicalColumn> getGroupByColumns() {
        return null;
    }

    @Override
    public String getTableNameOrAlias() {
        return null;
    }

    @Override
    public List<IQueryColumn> getProjectedColumns() {
        if(projectionColumns.isEmpty()){
            projectionColumns = Stream.concat(left.getProjectedColumns().stream(), right.getProjectedColumns().stream()).collect(Collectors.toList());
        }
        return projectionColumns;
    }

    @Override
    public List<IQueryColumn> getAllQueryColumns() {
        if(allColumns.isEmpty()){
            allColumns = Stream.concat(left.getAllQueryColumns().stream(), right.getAllQueryColumns().stream()).collect(Collectors.toList());
        }
        return allColumns;
    }

    @Override
    public void setLimit(Integer value) {

    }

    @Override
    public boolean isDistinct() {
        return false;
    }

    @Override
    public void setDistinct(boolean distinct) {

    }

    @Override
    public IQueryColumn getColumnByName(String column) throws ColumnNotFoundException {
        if(left.hasColumn(column))
            return left.getColumnByName(column);
        if(right.hasColumn(column))
            return right.getColumnByName(column);
        throw new ColumnNotFoundException(format("Column %s was not found", column));
    }

    @Override
    public boolean hasColumn(String column) {
        return left.hasColumn(column) || right.hasColumn(column);
    }

    @Override
    public void addProjection(IQueryColumn projection) {

    }

    @Override
    public IQueryColumn getOrCreatePhysicalColumn(String physicalColumn) {
        IQueryColumn result;
        try{
            result =  left.getOrCreatePhysicalColumn(physicalColumn);
        }catch (ColumnNotFoundException e){
            result = right.getOrCreatePhysicalColumn(physicalColumn);
        }
        return result;
    }

    @Override
    public void addFunctionColumn(FunctionColumn functionColumn) {

    }

    @Override
    public Class<?> getReturnType(String columnName) throws SQLException {
        return null;
    }

    @Override
    public void setQueryTemplatePacket(QueryTemplatePacket queryTemplatePacket) {

    }

    @Override
    public void addGroupByColumn(PhysicalColumn physicalColumn) {

    }

    @Override
    public boolean clearProjections() {
        return false;
    }

    @Override
    public boolean checkJoinCondition() {
        Object rightValue = right.getQueryResult().getCurrent().getPropertyValue(joinInfo.getRightColumn());
        Object leftValue = left.getQueryResult().getCurrent().getPropertyValue(joinInfo.getLeftColumn());
        if(joinInfo.checkJoinCondition(rightValue, leftValue)){
//            System.out.println(format("right table %s right value %s left table %s left value %s", right, rightValue, left, leftValue));
            return true;
        }
        return false;
    }

    public boolean next(){
        while (hasNext()){
            if(left.getQueryResult().next()){
                return true;
            }
            if(right.getQueryResult().next()){
                left.getQueryResult().reset();
            } else{
                return false;
            }
        }
        return false;
    }

    private boolean hasNext() {
        if (right.getQueryResult().getCursor().isBeforeFirst())
            return right.getQueryResult().getCursor().next();
        return true;
    }

    public ResultSupplier getLeft() {
        return left;
    }

    public ResultSupplier getRight() {
        return right;
    }
}
