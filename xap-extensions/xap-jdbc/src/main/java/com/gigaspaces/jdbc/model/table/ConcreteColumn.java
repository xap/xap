package com.gigaspaces.jdbc.model.table;

import java.util.Objects;

public class ConcreteColumn implements IQueryColumn {
    protected final TableContainer tableContainer;
    private final String columnName;
    private final String columnAlias;
    private final boolean isVisible;
    private final boolean isUUID;
    private final Class<?> returnType;
    private final int columnOrdinal;

    public ConcreteColumn(String name, Class<?> returnType, String alias, boolean isVisible, TableContainer tableContainer, int columnOrdinal) {
        this.columnName = name;
        this.columnAlias = alias;
        this.isVisible = isVisible;
        this.isUUID = name.equalsIgnoreCase(UUID_COLUMN);
        this.tableContainer = tableContainer;
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
    public boolean isVisible() {
        return isVisible;
    }

    @Override
    public boolean isUUID() {
        return isUUID;
    }

    @Override
    public TableContainer getTableContainer() {
        return tableContainer;
    }

    @Override
    public Object getCurrentValue() {
        if (tableContainer.getQueryResult().getCurrent() == null)
            return null;
        return tableContainer.getQueryResult().getCurrent().getPropertyValue(this);
    }

    @Override
    public Class<?> getReturnType() {
        return returnType;
    }

    @Override
    public String toString() {
        return tableContainer.getTableNameOrAlias() + "." + getNameOrAlias();
    }

    @Override
    public String getNameOrAlias() {
        return columnAlias != null ? columnAlias : columnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConcreteColumn)) return false;
        IQueryColumn that = (IQueryColumn) o;
        return isVisible() == that.isVisible()
                && isUUID() == that.isUUID()
                && Objects.equals(getTableContainer(), that.getTableContainer())
                && Objects.equals(getName(), that.getName())
                && Objects.equals(getAlias(), that.getAlias());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTableContainer(), getName(), getAlias(), isVisible(), isUUID());
    }

    @Override
    public int compareTo(IQueryColumn other) {
        return Integer.compare(this.getColumnOrdinal(), other.getColumnOrdinal());
    }
}
