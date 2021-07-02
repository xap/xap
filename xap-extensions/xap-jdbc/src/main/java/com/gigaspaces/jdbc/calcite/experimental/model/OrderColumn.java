package com.gigaspaces.jdbc.calcite.experimental.model;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.calcite.experimental.ResultSupplier;

public class OrderColumn implements IQueryColumn {

    private final boolean isAsc;
    private final boolean isNullsLast;
    private final IQueryColumn queryColumn;

    public OrderColumn(IQueryColumn queryColumn, boolean isAsc, boolean isNullsLast) {
        this.queryColumn = queryColumn;
        this.isAsc = isAsc;
        this.isNullsLast = isNullsLast;
    }

    public boolean isAsc() {
        return isAsc;
    }

    public boolean isNullsLast() {
        return isNullsLast;
    }

    @Override
    public int getColumnOrdinal() {
        return this.queryColumn.getColumnOrdinal();
    }

    @Override
    public String getName() {
        return this.queryColumn.getName();
    }

    @Override
    public String getAlias() {
        return this.queryColumn.getAlias();
    }

    @Override
    public boolean isUUID() {
        return this.queryColumn.isUUID();
    }

    @Override
    public ResultSupplier getResultSupplier() {
        return this.queryColumn.getResultSupplier();
    }

    @Override
    public Object getCurrentValue() {
        if(getResultSupplier().getQueryResult().getCurrent() == null) {
            return null;
        }
        return getResultSupplier().getQueryResult().getCurrent().getPropertyValue(this); // visit getPropertyValue(OrderColumn)
    }

    @Override
    public Class<?> getReturnType() {
        return this.queryColumn.getReturnType();
    }

    @Override
    public IQueryColumn create(String columnName, String columnAlias, int columnOrdinal) {
        return new OrderColumn(new ConcreteColumn(columnName, getReturnType(), columnAlias,
                getResultSupplier(), columnOrdinal), isAsc(), isNullsLast());
    }

    public IQueryColumn getQueryColumn() {
        return queryColumn;
    }

    @Override
    public String toString() {
        return getAlias() + " " + (isAsc ? "ASC" : "DESC") + " " + (isNullsLast ? "NULLS LAST" : "NULLS FIRST");
    }

    @Override
    public int compareTo(IQueryColumn other) {
        return this.queryColumn.compareTo(other);
    }

    @Override
    public Object getValue(IEntryPacket entryPacket) {
        return queryColumn.getValue(entryPacket);
    }
}
