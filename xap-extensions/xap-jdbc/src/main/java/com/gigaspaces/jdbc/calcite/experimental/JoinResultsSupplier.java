package com.gigaspaces.jdbc.calcite.experimental;

import com.gigaspaces.jdbc.calcite.experimental.model.AggregationColumn;
import com.gigaspaces.jdbc.calcite.experimental.model.ConcreteColumn;
import com.gigaspaces.jdbc.calcite.experimental.model.IQueryColumn;
import com.gigaspaces.jdbc.calcite.experimental.model.OrderColumn;
import com.gigaspaces.jdbc.calcite.experimental.model.join.JoinInfo;
import com.gigaspaces.jdbc.calcite.experimental.result.QueryResult;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;


import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;

import java.sql.SQLException;
import java.util.*;

public class JoinResultsSupplier implements ResultSupplier{
    private final ResultSupplier left;
    private final ResultSupplier right;
    private final Set<IQueryColumn> invisibleColumns = new HashSet<>();
    private final List<IQueryColumn> visibleColumns = new ArrayList<>();
    private final List<AggregationColumn> aggregationColumns = new ArrayList<>();
    private final IJSpace space;
    private final QueryExecutionConfig config;
    private final Object[] preparedValues;
    private boolean isAllColumnsSelected = false;
    private final LinkedList<Integer> fieldCountList = new LinkedList<>();


    public JoinResultsSupplier(ResultSupplier left, ResultSupplier right, IJSpace space, QueryExecutionConfig config, Object[] preparedValues) {
        this.left = left;
        this.right = right;
        this.space = space;
        this.config = config;
        this.preparedValues = preparedValues;
    }

    public JoinResultsSupplier(ResultSupplier left, ResultSupplier right, IJSpace space, Object[] preparedValues) {
        this(left, right, space, new QueryExecutionConfig(), preparedValues);
    }

    public QueryResult executeRead(QueryExecutionConfig config) throws SQLException {
        System.out.println("Executing Join");
        return merge(left.executeRead(config), right.executeRead(config));
    }

    private QueryResult merge(QueryResult left, QueryResult right) {
        return null;
    }

    public Set<IQueryColumn> getInvisibleColumns() {
        return invisibleColumns;
    }

    public List<IQueryColumn> getVisibleColumns() {
        return visibleColumns;
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

    public void addAggregationColumn(AggregationColumn aggregationColumn) {
        this.aggregationColumns.add(aggregationColumn);
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
        return null;
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
    public List<ConcreteColumn> getGroupByColumns() {
        return null;
    }

    @Override
    public String getTableNameOrAlias() {
        return null;
    }

    @Override
    public List<IQueryColumn> getSelectedColumns() {
        return null;
    }

    @Override
    public List<IQueryColumn> getAllQueryColumns() {
        return null;
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
}
