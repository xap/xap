package com.gigaspaces.jdbc.model.table;

import java.util.List;

public class AggregationFunction extends QueryColumn {

    private final AggregationFunctionType type;
    private final String functionName;
    private final String functionAlias;
    private final boolean allColumns;
    private List<TableContainer> tableContainers;

    public AggregationFunction(AggregationFunctionType type, String functionName, String alias, String columnName,
                               String columnAlias, TableContainer tableContainer, boolean visible, boolean allColumns) {
        super(columnName, columnAlias, visible, tableContainer);
        this.type = type;
        this.functionName = functionName;
        this.functionAlias = alias;
        this.allColumns = allColumns;
    }

    public AggregationFunctionType getType() {
        return this.type;
    }

    public String getFunctionName() {
        return this.functionName;
    }

    public String getFunctionAlias() {
        return this.functionAlias;
    } //TODO: remove?

    public String getAlias() { return this.functionAlias;
    }

    public TableContainer getTableContainer() {
        return super.getTableContainer();
    }

    public String getColumnAlias() {
        return super.getAlias();
    }

    public String getColumnName() {
//        return super.getName(); //TODO: what better?
        return super.getNameOrAlias();
    }

    public boolean isVisible() {
        return super.isVisible();
    }

    public boolean isAllColumns() {
        return allColumns;
    }

    public QueryColumn getQueryColumn() {
        return this;
    }

    public String getName() {
        return String.format("%s(%s)", getFunctionName(), getColumnName());
    }

    @Override
    public String getNameOrAlias() {
        return getFunctionAlias() == null ? String.format("%s(%s)", getFunctionName(), getColumnName()) :
                getFunctionAlias();
    }

    public List<TableContainer> getTableContainers() {
        return tableContainers;
    }

    public void setTableContainers(List<TableContainer> tableContainers) {
        this.tableContainers = tableContainers;
    }

    @Override
    public String toString() {
        if(this.tableContainer != null) {
            return String.format("%s(%s)", getFunctionName(), super.toString());
        }
        return String.format("%s(%s)", getFunctionName(), getColumnName());
    }

    public enum AggregationFunctionType {
        COUNT, MAX, MIN, AVG, SUM;
    }
}
