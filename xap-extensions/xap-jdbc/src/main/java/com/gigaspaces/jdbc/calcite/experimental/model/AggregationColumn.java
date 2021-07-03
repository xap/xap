package com.gigaspaces.jdbc.calcite.experimental.model;


import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.calcite.experimental.ResultSupplier;
import com.gigaspaces.jdbc.model.table.TableContainer;

import java.util.Locale;
import java.util.Objects;

public class AggregationColumn implements IQueryColumn {

    private final AggregationFunctionType type;
    private final String functionAlias;
    private final boolean allColumns;
    private final IQueryColumn queryColumn;

    public AggregationColumn(AggregationFunctionType functionType, String functionAlias, IQueryColumn queryColumn, boolean allColumns) {
        this.queryColumn = queryColumn;
        this.type = functionType;
        this.functionAlias = Objects.requireNonNull(functionAlias);
        this.allColumns = allColumns;
    }

    public AggregationFunctionType getType() {
        return this.type;
    }

    private String getFunctionName() {
        return this.type.name().toLowerCase(Locale.ROOT);
    }

    public String getAlias() {
        return this.functionAlias;
    }

    public ResultSupplier getResultSupplier() {
        return this.queryColumn != null ? this.queryColumn.getResultSupplier() : null;
    }

    @Override
    public Object getCurrentValue() {
        if (getResultSupplier().getQueryResult().getCurrent() == null) {
            return null;
        }
        return getResultSupplier().getQueryResult().getCurrent().getPropertyValue(this);
    }

    @Override
    public Class<?> getReturnType() {
        return this.queryColumn != null ? this.queryColumn.getReturnType() : null;
    }

//    @Override
//    public IQueryColumn create(String columnName, String columnAlias, int columnOrdinal) {
//        return new AggregationColumn(getType(), columnAlias == null ? columnName : columnAlias, getQueryColumn(), isAllColumns(), columnOrdinal);
//    }

    public String getColumnName() {
        if (this.queryColumn == null) {
            return isAllColumns() ? "*" : null;
        }
        return this.queryColumn.getAlias();  //return either name or alias.
    }

    @Override
    public boolean isUUID() {
        return false;
    }

    public boolean isAllColumns() {
        return this.allColumns;
    }

    public String getName() {
        return String.format("%s(%s)", getFunctionName(), getColumnName());
    }

    public IQueryColumn getQueryColumn() {
        return this.queryColumn;
    }

    @Override
    public String toString() {
        if (getResultSupplier() != null) {
            return String.format("%s(%s)", getFunctionName(), getResultSupplier().getTableNameOrAlias() + "." + getColumnName());
        }
        return String.format("%s(%s)", getFunctionName(), getColumnName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AggregationColumn)) return false;
        AggregationColumn that = (AggregationColumn) o;
        return  isAllColumns() == that.isAllColumns()
                && getType() == that.getType()
                && Objects.equals(getAlias(), that.getAlias())
                && Objects.equals(getQueryColumn(), that.getQueryColumn());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), getAlias(), isAllColumns(), getQueryColumn());
    }

    @Override
    public Object getValue(IEntryPacket entryPacket) {
        return queryColumn.getValue(entryPacket);
    }
}
