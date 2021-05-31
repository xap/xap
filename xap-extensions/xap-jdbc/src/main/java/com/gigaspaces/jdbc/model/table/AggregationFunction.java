package com.gigaspaces.jdbc.model.table;

public class AggregationFunction extends QueryColumn {

    private final AggregationFunctionType type;
    private final String functionName;
    private final String alias;
    private final boolean allColumns;

    public AggregationFunction(AggregationFunctionType type, String functionName, String alias, String columnName,
                               String columnAlias, TableContainer tableContainer, boolean visible, boolean allColumns) {
        super(columnName, columnAlias, visible, tableContainer);
        this.type = type;
        this.functionName = functionName;
        this.alias = alias;
        this.allColumns = allColumns;
    }

    public AggregationFunctionType getType() {
        return this.type;
    }

    public String getFunctionName() {
        return this.functionName;
    }

    public String getAlias() {
        return this.alias;
    }

    public TableContainer getTableContainer() {
        return super.getTableContainer();
    }

    public String getColumnAlias() {
        return super.getAlias();
    }

    public String getColumnName() {
        return super.getName();
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
    public String toString() {
        return String.format("%s(%s)", getFunctionName(), getColumnName());
    }

    public enum AggregationFunctionType {
        COUNT, MAX, MIN, AVG, SUM;

    }

}
