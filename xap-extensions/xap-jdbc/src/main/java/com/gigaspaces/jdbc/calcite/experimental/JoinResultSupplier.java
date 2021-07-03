package com.gigaspaces.jdbc.calcite.experimental;

import com.gigaspaces.jdbc.calcite.experimental.model.*;
import com.gigaspaces.jdbc.calcite.experimental.model.join.JoinInfo;
import com.gigaspaces.jdbc.calcite.experimental.result.QueryResult;
import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;


import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;

import java.sql.SQLException;
import java.util.*;

import static java.lang.String.format;

public class JoinResultSupplier implements ResultSupplier{
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


    public JoinResultSupplier(ResultSupplier left, ResultSupplier right, IJSpace space, QueryExecutionConfig config, Object[] preparedValues) {
        this.left = left;
        this.right = right;
        this.space = space;
        this.config = config;
        this.preparedValues = preparedValues;
    }

    public JoinResultSupplier(ResultSupplier left, ResultSupplier right, IJSpace space, Object[] preparedValues) {
        this(left, right, space, new QueryExecutionConfig(), preparedValues);
    }

    public QueryResult executeRead(QueryExecutionConfig config) throws SQLException {
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
    public List<PhysicalColumn> getGroupByColumns() {
        return null;
    }

    @Override
    public String getTableNameOrAlias() {
        return null;
    }

    @Override
    public List<IQueryColumn> getProjectedColumns() {
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
        return null;
    }

    @Override
    public void addFunctionColumn(FunctionColumn functionColumn) {

    }

    @Override
    public Class<?> getReturnType(String columnName) throws SQLException {
        return null;
    }

    @Override
    public ResultSupplier getJoinedSupplier() {
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
}
