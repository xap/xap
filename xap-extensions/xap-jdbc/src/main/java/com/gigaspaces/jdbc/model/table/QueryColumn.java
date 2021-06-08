package com.gigaspaces.jdbc.model.table;

import java.util.Objects;

public class QueryColumn implements Comparable<QueryColumn>{
    public static final String UUID_COLUMN = "UID";
    protected final TableContainer tableContainer;
    private final String columnName;
    private final String columnAlias;
    private final boolean isVisible;
    private final boolean isUUID;
    private final int columnIndex;

    public QueryColumn(String name, String alias, boolean isVisible, TableContainer tableContainer, int columnIndex) {
        this.columnName = name;
        this.columnAlias = alias;
        this.isVisible = isVisible;
        this.isUUID = name.equalsIgnoreCase(UUID_COLUMN);
        this.tableContainer = tableContainer;
        this.columnIndex = columnIndex;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public String getName() {
        return columnName;
    }

    public String getAlias() {
        return columnAlias;
    }

    public boolean isVisible() {
        return isVisible;
    }

    public boolean isUUID() {
        return isUUID;
    }

    public TableContainer getTableContainer() {
        return tableContainer;
    }

    public Object getCurrentValue() {
        if (tableContainer.getQueryResult().getCurrent() == null)
            return null;
        return tableContainer.getQueryResult().getCurrent().getPropertyValue(this);
    }

    @Override
    public String toString() {
        return tableContainer.getTableNameOrAlias() + "." + getNameOrAlias();
    }

    public String getNameOrAlias() {
        return columnAlias != null ? columnAlias : columnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryColumn)) return false;
        QueryColumn that = (QueryColumn) o;
        return isVisible() == that.isVisible() && isUUID() == that.isUUID() && Objects.equals(getTableContainer(), that.getTableContainer()) && Objects.equals(getName(), that.getName()) && Objects.equals(getAlias(), that.getAlias());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTableContainer(), getName(), getAlias(), isVisible(), isUUID());
    }

    @Override
    public int compareTo(QueryColumn other) {
        return Integer.compare(this.getColumnIndex(), other.getColumnIndex());
    }
}
