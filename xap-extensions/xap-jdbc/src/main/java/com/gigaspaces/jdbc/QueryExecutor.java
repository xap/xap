package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.table.AggregationColumn;
import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.core.IJSpace;

import java.sql.SQLException;
import java.util.*;

public class QueryExecutor {
    private final List<TableContainer> tables = new ArrayList<>();
    private final Set<IQueryColumn> invisibleColumns = new HashSet<>();
    private final List<IQueryColumn> visibleColumns = new ArrayList<>();
    private final List<AggregationColumn> aggregationColumns = new ArrayList<>();
    private final IJSpace space;
    private final QueryExecutionConfig config;
    private final Object[] preparedValues;
    private boolean isAllColumnsSelected = false;
    private final LinkedList<Integer> fieldCountList = new LinkedList<>();


    public QueryExecutor(IJSpace space, QueryExecutionConfig config, Object[] preparedValues) {
        this.space = space;
        this.config = config;
        this.preparedValues = preparedValues;
    }

    public QueryExecutor(IJSpace space, Object[] preparedValues) {
        this(space, new QueryExecutionConfig(), preparedValues);
    }

    public QueryResult execute() throws SQLException {
        if (tables.size() == 1) { //Simple Query
            return tables.get(0).executeRead(config);
        }
        JoinQueryExecutor joinE = new JoinQueryExecutor(this);
        return joinE.execute();
    }

    public List<TableContainer> getTables() {
        return tables;
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
        if (column.isVisible()) {
            visibleColumns.add(column);
        } else {
            invisibleColumns.add(column);
        }
    }

    public void addAggregationColumn(AggregationColumn aggregationColumn) {
        this.aggregationColumns.add(aggregationColumn);
    }

    public void addTable(TableContainer tableContainer){
        tables.add(tableContainer);
        addFieldCount(tableContainer.getVisibleColumns().size());
    }

    public TableContainer getTableByColumnIndex(int columnIndex){
        for (int i = 0; i < fieldCountList.size(); i++) {
            if(columnIndex < fieldCountList.get(i)){
                return getTables().get(i);
            }
        }
        throw new UnsupportedOperationException("");
    }

    public IQueryColumn getColumnByColumnIndex(int globalColumnIndex){
        for (int i = 0; i < fieldCountList.size(); i++) {
            if(globalColumnIndex < fieldCountList.get(i)){
                int columnIndex = i == 0 ? globalColumnIndex : globalColumnIndex - fieldCountList.get(i - 1);
                return getTables().get(i).getVisibleColumns().get(columnIndex);
            }
        }
        throw new UnsupportedOperationException("");
    }


    public void addFieldCount(int size) {
        int columnCount = fieldCountList.isEmpty() ?  size: fieldCountList.getLast() + size;
        fieldCountList.add(columnCount);
    }
}
