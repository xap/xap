package com.gigaspaces.jdbc.calcite.experimental.model;

import com.gigaspaces.jdbc.calcite.experimental.ResultSupplier;

import java.util.Objects;

public class ConcreteColumn implements IQueryColumn {
    protected final ResultSupplier resultSupplier;
    private final String columnName;
    private final String columnAlias;
    private final boolean isUUID;
    private final Class<?> returnType;
    private final int columnOrdinal;

    public ConcreteColumn(String columnName, Class<?> returnType, String columnAlias, ResultSupplier resultSupplier, int columnOrdinal) {
        this.columnName = columnName;
        this.columnAlias = columnAlias == null ? columnName : columnAlias;
        this.isUUID = columnName.equalsIgnoreCase(UUID_COLUMN);
        this.resultSupplier = resultSupplier;
        this.returnType = returnType;
        this.columnOrdinal = columnOrdinal;
    }

    @Override
    public int getColumnOrdinal() {
        return columnOrdinal;
    }

    @Override
    public String getName() {
        return columnName;
    }

    @Override
    public String getAlias() {
        return columnAlias;
    }

    @Override
    public boolean isUUID() {
        return isUUID;
    }

    @Override
    public ResultSupplier getResultSupplier() {
        return resultSupplier;
    }

    @Override
    public Object getCurrentValue() {
        if (resultSupplier.getQueryResult().getCurrent() == null)
            return null;
        return resultSupplier.getQueryResult().getCurrent().getPropertyValue(this);
    }

    @Override
    public Class<?> getReturnType() {
        return returnType;
    }

    @Override
    public IQueryColumn create(String columnName, String columnAlias, int columnOrdinal) {
        return new ConcreteColumn(columnName, getReturnType(), columnAlias, getResultSupplier(), columnOrdinal);
    }

    @Override
    public String toString() {
        return resultSupplier.getTableNameOrAlias() + "." + getAlias();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConcreteColumn)) return false;
        IQueryColumn that = (IQueryColumn) o;
        return isUUID() == that.isUUID()
                && Objects.equals(getResultSupplier(), that.getResultSupplier())
                && Objects.equals(getName(), that.getName())
                && Objects.equals(getAlias(), that.getAlias());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getResultSupplier(), getName(), getAlias(), isUUID());
    }

    @Override
    public int compareTo(IQueryColumn other) {
        return Integer.compare(this.getColumnOrdinal(), other.getColumnOrdinal());
    }
}
