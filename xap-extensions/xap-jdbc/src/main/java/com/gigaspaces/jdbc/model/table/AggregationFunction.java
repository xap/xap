package com.gigaspaces.jdbc.model.table;

import java.util.Locale;

public class AggregationFunction extends QueryColumn {

    private final AggregationFunctionType type;
    private final String functionName;
    private final String functionAlias;
    private final boolean allColumns;

    public AggregationFunction(AggregationFunctionType type, String functionName, String alias, String columnName,
                               String columnAlias, TableContainer tableContainer, boolean visible,
                               boolean allColumns, int columnIndex) {
        super(columnName, columnAlias, visible, tableContainer, columnIndex);
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

    public String getName() {
        return String.format("%s(%s)", getFunctionName(), getColumnName());
    }

    public String getNameWithLowerCase() {
        return String.format("%s(%s)", getFunctionName().toLowerCase(Locale.ROOT), getColumnName());
    }

    @Override
    public String getNameOrAlias() {
        return getFunctionAlias() == null ? getName() : getFunctionAlias();
    }

    @Override
    public String toString() {
        if(getTableContainer() != null) {
            return String.format("%s(%s)", getFunctionName(), getTableContainer().getTableNameOrAlias() + "." + getColumnName());
        }
        return String.format("%s(%s)", getFunctionName(), getColumnName());
    }

    public enum AggregationFunctionType {
        COUNT, MAX, MIN, AVG, SUM;
    }
}
